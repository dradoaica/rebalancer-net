using org.apache.zookeeper;
using Rebalancer.Core.Logging;
using Rebalancer.ZooKeeper.ResourceManagement;
using Rebalancer.ZooKeeper.Zk;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper.GlobalBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {
        // services
        private readonly IZooKeeperService zooKeeperService;
        private readonly IRebalancerLogger logger;
        private readonly ResourceManager store;

        // immutable state
        private CancellationToken coordinatorToken;
        private readonly TimeSpan minimumRebalancingInterval;
        private readonly string clientId;
        private readonly TimeSpan sessionTimeout;
        private readonly TimeSpan onStartDelay;

        // mutable state
        private StatusZnode status;
        private Task rebalancingTask;
        private CancellationTokenSource rebalancingCts;
        private int resourcesVersion;
        private readonly BlockingCollection<CoordinatorEvent> events;
        private bool ignoreWatches;
        private RebalancingResult? lastRebalancingResult;
        private readonly Stopwatch disconnectedTimer;

        public Coordinator(IZooKeeperService zooKeeperService,
            IRebalancerLogger logger,
            ResourceManager store,
            string clientId,
            TimeSpan minimumRebalancingInterval,
            TimeSpan sessionTimeout,
            TimeSpan onStartDelay,
            CancellationToken coordinatorToken)
        {
            this.zooKeeperService = zooKeeperService;
            this.logger = logger;
            this.store = store;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            this.clientId = clientId;
            this.sessionTimeout = sessionTimeout;
            this.onStartDelay = onStartDelay;
            this.coordinatorToken = coordinatorToken;
            rebalancingCts = new CancellationTokenSource();
            events = new BlockingCollection<CoordinatorEvent>();
            disconnectedTimer = new Stopwatch();
        }

        public override async Task process(WatchedEvent @event)
        {
            if (coordinatorToken.IsCancellationRequested || ignoreWatches)
            {
                return;
            }

            if (@event.getPath() != null)
            {
                logger.Info(clientId, $"Coordinator - KEEPER EVENT {@event.getState()} - {@event.get_Type()} - {@event.getPath()}");
            }
            else
            {
                logger.Info(clientId, $"Coordinator - KEEPER EVENT {@event.getState()} - {@event.get_Type()}");
            }

            switch (@event.getState())
            {
                case Event.KeeperState.Expired:
                    events.Add(CoordinatorEvent.SessionExpired);
                    break;
                case Event.KeeperState.Disconnected:
                    if (!disconnectedTimer.IsRunning)
                    {
                        disconnectedTimer.Start();
                    }

                    break;
                case Event.KeeperState.ConnectedReadOnly:
                case Event.KeeperState.SyncConnected:
                    if (disconnectedTimer.IsRunning)
                    {
                        disconnectedTimer.Reset();
                    }

                    if (@event.getPath() != null)
                    {
                        if (@event.get_Type() == Event.EventType.NodeDataChanged)
                        {
                            if (@event.getPath().EndsWith("epoch"))
                            {
                                events.Add(CoordinatorEvent.NoLongerCoordinator);
                            }
                        }
                        else if (@event.get_Type() == Event.EventType.NodeChildrenChanged)
                        {
                            if (@event.getPath().EndsWith("resources"))
                            {
                                events.Add(CoordinatorEvent.RebalancingTriggered);
                            }
                            else if (@event.getPath().EndsWith("clients"))
                            {
                                events.Add(CoordinatorEvent.RebalancingTriggered);
                            }
                        }
                    }

                    break;
                default:
                    logger.Error(clientId,
                        $"Coordinator - Currently this library does not support ZooKeeper state {@event.getState()}");
                    events.Add(CoordinatorEvent.PotentialInconsistentState);
                    break;
            }

            await Task.Yield();
        }

        public async Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch)
        {
            try
            {
                ignoreWatches = false;
                await zooKeeperService.IncrementAndWatchEpochAsync(currentEpoch, this);
                await zooKeeperService.WatchNodesAsync(this);

                ResourcesZnode getResourcesRes = await zooKeeperService.GetResourcesAsync(this, null);
                resourcesVersion = getResourcesRes.Version;

                status = await zooKeeperService.GetStatusAsync();
            }
            catch (ZkStaleVersionException e)
            {
                logger.Error(clientId, "Could not become coordinator as a stale version number was used", e);
                return BecomeCoordinatorResult.StaleEpoch;
            }
            catch (ZkInvalidOperationException e)
            {
                logger.Error(clientId, "Could not become coordinator as an invalid ZooKeeper operation occurred", e);
                return BecomeCoordinatorResult.Error;
            }

            events.Add(CoordinatorEvent.RebalancingTriggered);
            return BecomeCoordinatorResult.Ok;
        }

        public async Task<CoordinatorExitReason> StartEventLoopAsync()
        {
            Stopwatch rebalanceTimer = new Stopwatch();

            while (!coordinatorToken.IsCancellationRequested)
            {
                if (disconnectedTimer.IsRunning && disconnectedTimer.Elapsed > sessionTimeout)
                {
                    zooKeeperService.SessionExpired();
                    await CleanUpAsync();
                    return CoordinatorExitReason.SessionExpired;
                }

                if (events.TryTake(out CoordinatorEvent coordinatorEvent))
                {
                    switch (coordinatorEvent)
                    {
                        case CoordinatorEvent.SessionExpired:
                            zooKeeperService.SessionExpired();
                            await CleanUpAsync();
                            return CoordinatorExitReason.SessionExpired;

                        case CoordinatorEvent.NoLongerCoordinator:
                            await CleanUpAsync();
                            return CoordinatorExitReason.NoLongerCoordinator;

                        case CoordinatorEvent.PotentialInconsistentState:
                            await CleanUpAsync();
                            return CoordinatorExitReason.PotentialInconsistentState;

                        case CoordinatorEvent.FatalError:
                            await CleanUpAsync();
                            return CoordinatorExitReason.FatalError;

                        case CoordinatorEvent.RebalancingTriggered:
                            if (events.Any())
                            {
                                // skip this event. All other events take precedence over rebalancing
                                // there may be multiple rebalancing events, so if the events collection
                                // consists only of rebalancing events then we'll just process the last one
                            }
                            else if (!rebalanceTimer.IsRunning || rebalanceTimer.Elapsed > minimumRebalancingInterval)
                            {
                                await CancelRebalancingIfInProgressAsync();
                                rebalanceTimer.Reset();
                                rebalanceTimer.Start();
                                logger.Info(clientId, "Coordinator - Rebalancing triggered");
                                rebalancingTask = Task.Run(async () => await TriggerRebalancing(rebalancingCts.Token));
                            }
                            else
                            {
                                // if enough time has not passed since the last rebalancing just readd it
                                events.Add(CoordinatorEvent.RebalancingTriggered);
                            }
                            break;
                        default:
                            await CleanUpAsync();
                            return CoordinatorExitReason.PotentialInconsistentState;
                    }
                }

                await WaitFor(TimeSpan.FromSeconds(1));
            }

            if (coordinatorToken.IsCancellationRequested)
            {
                await CancelRebalancingIfInProgressAsync();
                await zooKeeperService.CloseSessionAsync();
                return CoordinatorExitReason.Cancelled;
            }

            return CoordinatorExitReason.PotentialInconsistentState; // if this happens then we have a correctness bug
        }

        private async Task CleanUpAsync()
        {
            try
            {
                ignoreWatches = true;
                await CancelRebalancingIfInProgressAsync();
            }
            finally
            {
                await store.InvokeOnStopActionsAsync(clientId, "Coordinator");
            }
        }

        private async Task CancelRebalancingIfInProgressAsync()
        {
            if (rebalancingTask != null && !rebalancingTask.IsCompleted)
            {
                logger.Info(clientId, "Coordinator - Cancelling the rebalancing that is in progress");
                rebalancingCts.Cancel();
                try
                {
                    await rebalancingTask; // might need to put a time limit on this
                }
                catch (Exception ex)
                {
                    logger.Error(clientId, "Coordinator - Errored on cancelling rebalancing", ex);
                    events.Add(CoordinatorEvent.PotentialInconsistentState);
                }
                rebalancingCts = new CancellationTokenSource(); // reset cts
            }
        }

        private async Task WaitFor(TimeSpan waitPeriod)
        {
            try
            {
                await Task.Delay(waitPeriod, coordinatorToken);
            }
            catch (TaskCanceledException)
            { }
        }

        private async Task WaitFor(TimeSpan waitPeriod, CancellationToken rebalancingToken)
        {
            try
            {
                await Task.Delay(waitPeriod, rebalancingToken);
            }
            catch (TaskCanceledException)
            { }
        }

        private async Task TriggerRebalancing(CancellationToken rebalancingToken)
        {
            try
            {
                await zooKeeperService.WatchResourcesChildrenAsync(this);
                await zooKeeperService.WatchNodesAsync(this);

                RebalancingResult result = await RebalanceAsync(rebalancingToken);
                switch (result)
                {
                    case RebalancingResult.Complete:
                        logger.Info(clientId, "Coordinator - Rebalancing complete");
                        break;
                    case RebalancingResult.Cancelled:
                        logger.Info(clientId, "Coordinator - Rebalancing cancelled");
                        break;
                    case RebalancingResult.NotRequired:
                        logger.Info(clientId, "Coordinator - Rebalancing not required");
                        break;
                }

                lastRebalancingResult = result;
            }
            catch (ZkSessionExpiredException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId, "Coordinator - The current session has expired", e);
                events.Add(CoordinatorEvent.SessionExpired);
            }
            catch (ZkStaleVersionException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId,
                    "Coordinator - A stale znode version was used, aborting rebalancing.", e);
                events.Add(CoordinatorEvent.NoLongerCoordinator);
            }
            catch (ZkInvalidOperationException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId,
                    "Coordinator - An invalid ZooKeeper operation occurred, aborting rebalancing.",
                    e);
                events.Add(CoordinatorEvent.PotentialInconsistentState);
            }
            catch (InconsistentStateException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId,
                    "Coordinator - An error occurred potentially leaving the client in an inconsistent state, aborting rebalancing.",
                    e);
                events.Add(CoordinatorEvent.PotentialInconsistentState);
            }
            catch (TerminateClientException e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId,
                    "Coordinator - A fatal error has occurred, aborting rebalancing.",
                    e);
                events.Add(CoordinatorEvent.FatalError);
            }
            catch (ZkOperationCancelledException)
            {
                lastRebalancingResult = RebalancingResult.Cancelled;
                logger.Warn(clientId, "Coordinator - Rebalancing cancelled");
            }
            catch (Exception e)
            {
                lastRebalancingResult = RebalancingResult.Failed;
                logger.Error(clientId,
                    "Coordinator - An unexpected error has occurred, aborting rebalancing.", e);
                events.Add(CoordinatorEvent.PotentialInconsistentState);
            }
        }

        private async Task<RebalancingResult> RebalanceAsync(CancellationToken rebalancingToken)
        {
            // the clients and resources identified in the stop phase are the only
            // ones taken into account during the rebalancing

            StopPhaseResult stopPhaseResult = await StopActivityPhaseAsync(rebalancingToken);
            if (stopPhaseResult.PhaseResult != RebalancingResult.Complete)
            {
                return stopPhaseResult.PhaseResult;
            }

            RebalancingResult assignPhaseResult = await AssignResourcesPhaseAsync(rebalancingToken,
                stopPhaseResult.ResourcesZnode,
                stopPhaseResult.ClientsZnode);
            if (assignPhaseResult != RebalancingResult.Complete)
            {
                return assignPhaseResult;
            }

            RebalancingResult verifyPhaseResult = await VerifyStartedPhaseAsync(rebalancingToken,
                stopPhaseResult.FollowerIds);
            if (verifyPhaseResult != RebalancingResult.Complete)
            {
                return assignPhaseResult;
            }

            return RebalancingResult.Complete;
        }

        private string GetClientId(string clientPath)
        {
            return clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal) + 1);
        }

        private async Task<StopPhaseResult> StopActivityPhaseAsync(CancellationToken rebalancingToken)
        {
            logger.Info(clientId, "Coordinator - Get active clients and resources");
            ClientsZnode clients = await zooKeeperService.GetActiveClientsAsync();
            List<string> followerIds = clients.ClientPaths.Select(GetClientId).Where(x => x != clientId).ToList();
            ResourcesZnode resources = await zooKeeperService.GetResourcesAsync(null, null);
            logger.Info(clientId, $"Coordinator - {followerIds.Count} followers in scope and {resources.Resources.Count} resources in scope");
            logger.Info(clientId, $"Coordinator - Assign resources ({string.Join(",", resources.Resources)}) to clients ({string.Join(",", clients.ClientPaths.Select(GetClientId))})");

            if (resources.Version != resourcesVersion)
            {
                throw new ZkStaleVersionException("Resources znode version does not match expected value, indicates another client has been made coordinator and is executing a rebalancing.");
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return new StopPhaseResult(RebalancingResult.Cancelled);
            }

            // if no resources were changed and there are more clients than resources then check
            // to see if rebalancing is necessary. If existing assignments are still valid then
            // a new client or the loss of a client with no assignments need not trigger a rebalancing
            if (!IsRebalancingRequired(clients, resources))
            {
                logger.Info(clientId, "Coordinator - No rebalancing required. No resource change. No change to existing assigned clients. More clients than resources.");
                return new StopPhaseResult(RebalancingResult.NotRequired);
            }

            logger.Info(clientId, "Coordinator - Command followers to stop");
            status.RebalancingStatus = RebalancingStatus.StopActivity;
            status.Version = await zooKeeperService.SetStatus(status);

            if (rebalancingToken.IsCancellationRequested)
            {
                return new StopPhaseResult(RebalancingResult.Cancelled);
            }

            await store.InvokeOnStopActionsAsync(clientId, "Coordinator");

            // wait for confirmation that all followers have stopped or for time limit
            while (!rebalancingToken.IsCancellationRequested)
            {
                List<string> stopped = await zooKeeperService.GetStoppedAsync();

                if (AreClientsStopped(followerIds, stopped))
                {
                    logger.Info(clientId, $"Coordinator - All {stopped.Count} in scope followers have stopped");
                    break;
                }
                else
                {
                    // check that a client hasn't died mid-rebalancing, if so, trigger a new rebalancing and abort this one.
                    // else wait and check again
                    ClientsZnode latestClients = await zooKeeperService.GetActiveClientsAsync();
                    List<string> missingClients = GetMissing(followerIds, latestClients.ClientPaths);
                    if (missingClients.Any())
                    {
                        logger.Info(clientId, $"Coordinator - {missingClients.Count} followers have disappeared. Missing: {string.Join(",", missingClients)}. Triggering new rebalancing.");
                        events.Add(CoordinatorEvent.RebalancingTriggered);
                        return new StopPhaseResult(RebalancingResult.Cancelled);
                    }
                    else
                    {
                        List<string> pendingClientIds = GetMissing(followerIds, stopped);
                        logger.Info(clientId, $"Coordinator - waiting for followers to stop: {string.Join(",", pendingClientIds)}");
                        await WaitFor(TimeSpan.FromSeconds(2)); // try again in 2s
                    }
                }
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return new StopPhaseResult(RebalancingResult.Cancelled);
            }

            StopPhaseResult phaseResult = new StopPhaseResult(RebalancingResult.Complete)
            {
                ResourcesZnode = resources,
                ClientsZnode = clients,
                FollowerIds = followerIds
            };

            return phaseResult;
        }

        private async Task<RebalancingResult> AssignResourcesPhaseAsync(CancellationToken rebalancingToken,
            ResourcesZnode resources,
            ClientsZnode clients)
        {
            logger.Info(clientId, "Coordinator - Assign resources to clients");
            Queue<string> resourcesToAssign = new Queue<string>(resources.Resources);
            List<ResourceAssignment> resourceAssignments = new List<ResourceAssignment>();
            int clientIndex = 0;
            while (resourcesToAssign.Any())
            {
                resourceAssignments.Add(new ResourceAssignment()
                {
                    ClientId = GetClientId(clients.ClientPaths[clientIndex]),
                    Resource = resourcesToAssign.Dequeue()
                });

                clientIndex++;
                if (clientIndex >= clients.ClientPaths.Count)
                {
                    clientIndex = 0;
                }
            }

            // write assignments back to resources znode
            resources.ResourceAssignments.Assignments = resourceAssignments;
            resourcesVersion = await zooKeeperService.SetResourcesAsync(resources);

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            status.RebalancingStatus = RebalancingStatus.ResourcesGranted;
            status.Version = await zooKeeperService.SetStatus(status);

            if (onStartDelay.Ticks > 0)
            {
                logger.Info(clientId, $"Coordinator - Delaying on start for {(int)onStartDelay.TotalMilliseconds}ms");
                await WaitFor(onStartDelay, rebalancingToken);
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            List<string> leaderAssignments = resourceAssignments.Where(x => x.ClientId == clientId).Select(x => x.Resource).ToList();
            await store.InvokeOnStartActionsAsync(clientId, "Coordinator", leaderAssignments, rebalancingToken, coordinatorToken);

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            return RebalancingResult.Complete;
        }

        private async Task<RebalancingResult> VerifyStartedPhaseAsync(CancellationToken rebalancingToken,
            IList<string> followerIds)
        {
            logger.Info(clientId, "Coordinator - Verify all followers have started");
            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            while (!rebalancingToken.IsCancellationRequested)
            {
                List<string> stopped = await zooKeeperService.GetStoppedAsync();
                List<string> stoppedClientsInScope = GetPresentInBoth(followerIds, stopped);
                if (!stoppedClientsInScope.Any())
                {
                    break;
                }
                else
                {
                    logger.Info(clientId, $"Coordinator - Waiting for {stoppedClientsInScope.Count} remaining in scope followers to start");
                    await WaitFor(TimeSpan.FromSeconds(2)); // try again in 2s
                }
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            logger.Info(clientId, "Coordinator - All followers confirm started");
            //status.RebalancingStatus = RebalancingStatus.StartConfirmed;
            //this.status.Version = await this.zooKeeperService.SetStatus(status);

            return RebalancingResult.Complete;
        }

        private bool IsRebalancingRequired(ClientsZnode clients, ResourcesZnode resources)
        {
            // if this is the first rebalancing as coordinator or the last one was not successful then rebalancing is required
            if (store.GetAssignmentStatus() == AssignmentStatus.NoAssignmentYet
                || !lastRebalancingResult.HasValue
                || (lastRebalancingResult.Value != RebalancingResult.Complete && lastRebalancingResult.Value != RebalancingResult.NotRequired))
            {
                return true;
            }

            // any change to resources requires a rebalancing
            if (resources.HasResourceChange())
            {
                return true;
            }

            // given a client was either added or removed

            // if there are less clients than resources then we require a rebalancing
            if (clients.ClientPaths.Count < resources.Resources.Count)
            {
                return true;
            }

            // given we have an equal or greater number clients than resources

            // if an existing client is currently assigned more than one resource we require a rebalancing
            if (resources.ResourceAssignments.Assignments.GroupBy(x => x.ClientId).Any(x => x.Count() > 1))
            {
                return true;
            }

            // given all existing assignments are one client to one resource

            // if any client for the existing assignments is no longer around then we require a rebalancing
            List<string> clientIds = clients.ClientPaths.Select(GetClientId).ToList();
            foreach (ResourceAssignment assignment in resources.ResourceAssignments.Assignments)
            {
                if (!clientIds.Contains(assignment.ClientId, StringComparer.Ordinal))
                {
                    return true;
                }
            }

            // otherwise no rebalancing is required
            return false;
        }

        private bool AreClientsStopped(List<string> followerIds, List<string> stoppedPaths)
        {
            List<string> stoppedClientIds = stoppedPaths.Select(x => GetClientId(x)).ToList();

            // we only care that the clients that fall under the current rebalancing are included in the list of stopped nodes
            // it is possible that since rebalancing started, a new client came online and saw the status change to stop activity
            // and added its own stopped node. These we ignore.
            return followerIds.Intersect(stoppedClientIds).Count() == followerIds.Count;
        }

        private List<string> GetMissing(List<string> followerIds, List<string> clientPaths2)
        {
            List<string> clientIds2 = clientPaths2.Select(x => GetClientId(x)).OrderBy(x => x).ToList();

            return followerIds.Except(clientIds2).ToList();
        }

        private List<string> GetPresentInBoth(IList<string> followerIds, List<string> clientPaths2)
        {
            List<string> clientIds2 = clientPaths2.Select(x => GetClientId(x)).OrderBy(x => x).ToList();

            return followerIds.Intersect(clientIds2).ToList();
        }




    }
}