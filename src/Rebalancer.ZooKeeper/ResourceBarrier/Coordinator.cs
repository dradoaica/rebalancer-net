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

namespace Rebalancer.ZooKeeper.ResourceBarrier
{
    public class Coordinator : Watcher, ICoordinator
    {
        // services
        private readonly IZooKeeperService zooKeeperService;
        private readonly IRebalancerLogger logger;
        private readonly ResourceManager store;

        // immutable state
        private readonly CancellationToken coordinatorToken;
        private readonly string clientId;
        private readonly TimeSpan minimumRebalancingInterval;
        private readonly TimeSpan sessionTimeout;
        private readonly TimeSpan onStartDelay;

        // mutable state
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
            this.clientId = clientId;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            this.sessionTimeout = sessionTimeout;
            this.onStartDelay = onStartDelay;
            this.coordinatorToken = coordinatorToken;

            rebalancingCts = new CancellationTokenSource();
            events = new BlockingCollection<CoordinatorEvent>();
            disconnectedTimer = new Stopwatch();
        }

        // very important that this method does not throw any exceptions as it is called from the zookeeper library
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
                }

                lastRebalancingResult = result;
            }
            catch (ZkSessionExpiredException e)
            {
                logger.Error(clientId, "Coordinator - The current session has expired", e);
                events.Add(CoordinatorEvent.SessionExpired);
                lastRebalancingResult = RebalancingResult.Failed;
            }
            catch (ZkStaleVersionException e)
            {
                logger.Error(clientId,
                    "Coordinator - A stale znode version was used, aborting rebalancing.", e);
                events.Add(CoordinatorEvent.NoLongerCoordinator);
                lastRebalancingResult = RebalancingResult.Failed;
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
                logger.Warn(clientId, "Coordinator - Rebalancing cancelled");
                lastRebalancingResult = RebalancingResult.Cancelled;
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
            Stopwatch sw = new Stopwatch();
            sw.Start();

            logger.Info(clientId, "Coordinator - Get clients and resources list");
            ClientsZnode clients = await zooKeeperService.GetActiveClientsAsync();
            ResourcesZnode resources = await zooKeeperService.GetResourcesAsync(null, null);

            if (resources.Version != resourcesVersion)
            {
                throw new ZkStaleVersionException("Resources znode version does not match expected value, indicates another client has been made coordinator and is executing a rebalancing.");
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            // if no resources were changed and there are more clients than resources then check
            // to see if rebalancing is necessary. If existing assignments are still valid then
            // a new client or the loss of a client with no assignments need not trigger a rebalancing
            if (!IsRebalancingRequired(clients, resources))
            {
                logger.Info(clientId, "Coordinator - No rebalancing required. No resource change. No change to existing clients. More clients than resources.");
                return RebalancingResult.Complete;
            }

            logger.Info(clientId, $"Coordinator - Assign resources ({string.Join(",", resources.Resources)}) to clients ({string.Join(",", clients.ClientPaths.Select(GetClientId))})");
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

            await store.InvokeOnStopActionsAsync(clientId, "Coordinator");
            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            if (onStartDelay.Ticks > 0)
            {
                logger.Info(clientId, $"Coordinator - Delaying on start for {(int)onStartDelay.TotalMilliseconds}ms");
                await WaitFor(onStartDelay, rebalancingToken);
            }

            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            List<string> leaderAssignments = resourceAssignments
                    .Where(x => x.ClientId == clientId)
                    .Select(x => x.Resource)
                    .ToList();
            await store.InvokeOnStartActionsAsync(clientId, "Coordinator", leaderAssignments, rebalancingToken, coordinatorToken);
            if (rebalancingToken.IsCancellationRequested)
            {
                return RebalancingResult.Cancelled;
            }

            return RebalancingResult.Complete;
        }

        private bool IsRebalancingRequired(ClientsZnode clients, ResourcesZnode resources)
        {
            // if this is the first rebalancing as coordinator or the last one was not successful then rebalancing is required
            if (store.GetAssignmentStatus() == AssignmentStatus.NoAssignmentYet || !lastRebalancingResult.HasValue || lastRebalancingResult.Value != RebalancingResult.Complete)
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

        private string GetClientId(string clientPath)
        {
            return clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal) + 1);
        }
    }
}