using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.ZooKeeper.Zk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper.ResourceManagement
{
    public class ResourceManager
    {
        // services
        private readonly IRebalancerLogger logger;
        private readonly IZooKeeperService zooKeeperService;

        // immutable state
        private readonly SemaphoreSlim actionsSemaphore = new SemaphoreSlim(1, 1);
        private readonly object resourcesLockObj = new object();
        private readonly OnChangeActions onChangeActions;
        private readonly RebalancingMode rebalancingMode;

        // mutable statwe
        private List<string> resources;
        private AssignmentStatus assignmentStatus;

        public ResourceManager(IZooKeeperService zooKeeperService,
            IRebalancerLogger logger,
            OnChangeActions onChangeActions,
            RebalancingMode rebalancingMode)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            resources = new List<string>();
            assignmentStatus = AssignmentStatus.NoAssignmentYet;
            this.onChangeActions = onChangeActions;
            this.rebalancingMode = rebalancingMode;
        }

        public AssignmentStatus GetAssignmentStatus()
        {
            lock (resourcesLockObj)
            {
                return assignmentStatus;
            }
        }

        public GetResourcesResponse GetResources()
        {
            lock (resourcesLockObj)
            {
                if (assignmentStatus == AssignmentStatus.ResourcesAssigned)
                {
                    return new GetResourcesResponse()
                    {
                        Resources = new List<string>(resources),
                        AssignmentStatus = assignmentStatus
                    };
                }
                else
                {
                    return new GetResourcesResponse()
                    {
                        Resources = new List<string>(),
                        AssignmentStatus = assignmentStatus
                    };
                }
            }
        }

        private void SetResources(AssignmentStatus newAssignmentStatus, List<string> newResources)
        {
            lock (resourcesLockObj)
            {
                assignmentStatus = newAssignmentStatus;
                resources = new List<string>(newResources);
            }
        }

        public bool IsInStartedState()
        {
            lock (resourcesLockObj)
            {
                return assignmentStatus == AssignmentStatus.ResourcesAssigned ||
                       assignmentStatus == AssignmentStatus.NoResourcesAssigned;
            }
        }

        public async Task InvokeOnStopActionsAsync(string clientId, string role)
        {
            await actionsSemaphore.WaitAsync();

            try
            {
                List<string> resourcesToRemove = new List<string>(GetResources().Resources);
                if (resourcesToRemove.Any())
                {
                    logger.Info(clientId, $"{role} - Invoking on stop actions. Unassigned resources {string.Join(",", resourcesToRemove)}");

                    try
                    {
                        foreach (Action onStopAction in onChangeActions.OnStopActions)
                        {
                            onStopAction.Invoke();
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(clientId, "{role} - End user on stop actions threw an exception. Terminating. ", e);
                        throw new TerminateClientException("End user on stop actions threw an exception.", e);
                    }

                    if (rebalancingMode == RebalancingMode.ResourceBarrier)
                    {
                        try
                        {
                            logger.Info(clientId,
                                $"{role} - Removing barriers on resources {string.Join(",", resourcesToRemove)}");
                            int counter = 1;
                            foreach (string resource in resourcesToRemove)
                            {
                                await zooKeeperService.RemoveResourceBarrierAsync(resource);

                                if (counter % 10 == 0)
                                {
                                    logger.Info(clientId,
                                        $"{role} - Removed barriers on {counter} resources of {resourcesToRemove.Count}");
                                }

                                counter++;
                            }

                            logger.Info(clientId, $"{role} - Removed barriers on {resourcesToRemove.Count} resources of {resourcesToRemove.Count}");
                        }
                        catch (ZkOperationCancelledException)
                        {
                            // do nothing, cancellation is in progress, these are ephemeral nodes anyway
                        }
                        catch (ZkSessionExpiredException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            throw new InconsistentStateException("An error occurred while removing resource barriers", e);
                        }
                    }

                    SetResources(AssignmentStatus.NoAssignmentYet, new List<string>());
                    logger.Info(clientId, $"{role} - On stop complete");
                }
                else
                {
                    SetResources(AssignmentStatus.NoAssignmentYet, new List<string>());
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }

        public async Task InvokeOnStartActionsAsync(string clientId,
            string role,
            List<string> newResources,
            CancellationToken rebalancingToken,
            CancellationToken clientToken)
        {
            await actionsSemaphore.WaitAsync();

            if (IsInStartedState())
            {
                throw new InconsistentStateException("An attempt to invoke on start actions occurred while already in the started state");
            }

            try
            {
                if (newResources.Any())
                {
                    if (rebalancingMode == RebalancingMode.ResourceBarrier)
                    {
                        try
                        {
                            logger.Info(clientId,
                                $"{role} - Putting barriers on resources {string.Join(",", newResources)}");
                            int counter = 1;
                            foreach (string resource in newResources)
                            {
                                await zooKeeperService.TryPutResourceBarrierAsync(resource, rebalancingToken,
                                    logger);

                                if (counter % 10 == 0)
                                {
                                    logger.Info(clientId,
                                        $"{role} - Put barriers on {counter} resources of {newResources.Count}");
                                }

                                counter++;
                            }
                            logger.Info(clientId, $"{role} - Put barriers on {newResources.Count} resources of {newResources.Count}");
                        }
                        catch (ZkOperationCancelledException)
                        {
                            if (clientToken.IsCancellationRequested)
                            {
                                throw;
                            }
                            else
                            {
                                logger.Info(clientId,
                                    $"{role} - Rebalancing cancelled, removing barriers on resources {string.Join(",", newResources)}");
                                try
                                {
                                    int counter = 1;
                                    foreach (string resource in newResources)
                                    {
                                        await zooKeeperService.RemoveResourceBarrierAsync(resource);

                                        if (counter % 10 == 0)
                                        {
                                            logger.Info(clientId,
                                                $"{role} - Removing barriers on {counter} resources of {newResources.Count}");
                                        }

                                        counter++;
                                    }
                                    logger.Info(clientId, $"{role} - Removed barriers on {newResources.Count} resources of {newResources.Count}");
                                }
                                catch (ZkSessionExpiredException)
                                {
                                    throw;
                                }
                                catch (ZkOperationCancelledException)
                                {
                                    // do nothing, client cancellation in progress
                                }
                                catch (Exception e)
                                {
                                    throw new InconsistentStateException(
                                        "An error occurred while removing resource barriers due to rebalancing cancellation", e);
                                }

                                return;
                            }
                        }
                        catch (ZkSessionExpiredException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            throw new InconsistentStateException("An error occurred while putting resource barriers", e);
                        }
                    }

                    SetResources(AssignmentStatus.ResourcesAssigned, newResources);

                    try
                    {
                        logger.Info(clientId, $"{role} - Invoking on start with resources {string.Join(",", resources)}");
                        foreach (Action<IList<string>> onStartAction in onChangeActions.OnStartActions)
                        {
                            onStartAction.Invoke(resources);
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(clientId, $"{role} - End user on start actions threw an exception. Terminating. ", e);
                        throw new TerminateClientException("End user on start actions threw an exception.", e);
                    }

                    logger.Info(clientId, $"{role} - On start complete");
                }
                else
                {
                    SetResources(AssignmentStatus.NoResourcesAssigned, newResources);
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }


        public async Task InvokeOnAbortActionsAsync(string clientId, string message, Exception ex = null)
        {
            await actionsSemaphore.WaitAsync();
            try
            {
                logger.Info(clientId, "Invoking on abort actions.");

                try
                {
                    foreach (Action<string, Exception> onAbortAction in onChangeActions.OnAbortActions)
                    {
                        onAbortAction.Invoke(message, ex);
                    }
                }
                catch (Exception e)
                {
                    logger.Error(clientId, "End user on error actions threw an exception. Terminating. ", e);
                    throw new TerminateClientException("End user on error actions threw an exception.", e);
                }
            }
            finally
            {
                actionsSemaphore.Release();
            }
        }

    }
}