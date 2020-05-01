using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.ZooKeeper.ResourceManagement;
using Rebalancer.ZooKeeper.Zk;
using System;
using System.Threading;
using System.Threading.Tasks;
using GB = Rebalancer.ZooKeeper.GlobalBarrier;
using RB = Rebalancer.ZooKeeper.ResourceBarrier;

namespace Rebalancer.ZooKeeper
{
    public class ZooKeeperProvider : IRebalancerProvider
    {
        // services
        private readonly IRebalancerLogger logger;
        private readonly IZooKeeperService zooKeeperService;
        private ResourceManager resourceManager;

        // non-mutable state
        private readonly RebalancingMode rebalancingMode;
        private readonly string zooKeeperRootPath;
        private readonly TimeSpan minimumRebalancingInterval;
        private string resourceGroup;
        private TimeSpan sessionTimeout;
        private TimeSpan connectTimeout;
        private TimeSpan onStartDelay;
        private readonly object startLockObj = new object();
        private readonly Random rand;

        // mutable state
        private string clientId;
        private string clientPath;
        private int clientNumber;
        private ClientInternalState state;
        private string watchSiblingNodePath;
        private int epoch;
        private bool started;
        private bool aborted;
        private Task mainTask;

        public ZooKeeperProvider(string zookeeperHosts,
            string zooKeeperRootPath,
            TimeSpan sessionTimeout,
            TimeSpan connectTimeout,
            TimeSpan minimumRebalancingInterval,
            RebalancingMode rebalancingMode,
            IRebalancerLogger logger,
            IZooKeeperService zooKeeperService = null)
        {
            this.zooKeeperRootPath = zooKeeperRootPath;
            this.rebalancingMode = rebalancingMode;
            this.logger = logger;
            this.sessionTimeout = sessionTimeout;
            this.connectTimeout = connectTimeout;
            this.minimumRebalancingInterval = minimumRebalancingInterval;
            rand = new Random(Guid.NewGuid().GetHashCode());

            if (zooKeeperService == null)
            {
                this.zooKeeperService = new ZooKeeperService(zookeeperHosts);
            }
            else
            {
                this.zooKeeperService = zooKeeperService;
            }
        }

        public async Task StartAsync(string resourceGroup,
            OnChangeActions onChangeActions,
            CancellationToken token,
            ClientOptions clientOptions)
        {
            // just in case someone tries to start the client twice (with some concurrency)
            lock (startLockObj)
            {
                if (started)
                {
                    throw new RebalancerException("Client already started");
                }

                started = true;
            }

            resourceManager = new ResourceManager(zooKeeperService, logger, onChangeActions, rebalancingMode);
            SetStateToNoSession();
            this.resourceGroup = resourceGroup;
            onStartDelay = clientOptions.OnAssignmentDelay;

            mainTask = Task.Run(async () => await RunStateMachine(token, clientOptions));
            await Task.Yield();
        }

        private async Task RunStateMachine(CancellationToken token, ClientOptions clientOptions)
        {
            while (state != ClientInternalState.Terminated)
            {
                if (token.IsCancellationRequested)
                {
                    await TerminateAsync("cancellation", false);
                }

                try
                {
                    switch (state)
                    {
                        case ClientInternalState.NoSession:
                            NewSessionResult established = await EstablishSessionAsync(token);
                            switch (established)
                            {
                                case NewSessionResult.Established:
                                    state = ClientInternalState.NoClientNode;
                                    break;
                                case NewSessionResult.TimeOut:
                                    state = ClientInternalState.NoSession;
                                    await WaitRandomTime(TimeSpan.FromSeconds(5));
                                    break;
                                default:
                                    state = ClientInternalState.Error;
                                    break;
                            }

                            break;
                        case ClientInternalState.NoClientNode:
                            bool created = await CreateClientNodeAsync();
                            if (created)
                            {
                                state = ClientInternalState.NoRole;
                            }
                            else
                            {
                                state = ClientInternalState.Error;
                            }

                            break;
                        case ClientInternalState.NoRole:
                            bool epochAttained = await CacheEpochLocallyAsync();
                            if (!epochAttained)
                            {
                                await EvaluateTerminationAsync(token, clientOptions, "Couldn't read the current epoch.");
                            }

                            (ElectionResult electionResult, string lowerSiblingPath) = await DetermineLeadershipAsync();
                            switch (electionResult)
                            {
                                case ElectionResult.IsLeader:
                                    state = ClientInternalState.IsLeader;
                                    watchSiblingNodePath = string.Empty;
                                    break;
                                case ElectionResult.IsFollower:
                                    state = ClientInternalState.IsFollower;
                                    watchSiblingNodePath = lowerSiblingPath;
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions, "The client has entered an unknown state");
                                    break;
                            }

                            break;
                        case ClientInternalState.IsLeader:
                            CoordinatorExitReason coordinatorExitReason = await BecomeCoordinatorAsync(token);
                            switch (coordinatorExitReason)
                            {
                                case CoordinatorExitReason.NoLongerCoordinator:
                                    SetStateToNoSession(); // need a new client node
                                    break;
                                case CoordinatorExitReason.Cancelled:
                                    await TerminateAsync("cancellation", false);
                                    break;
                                case CoordinatorExitReason.SessionExpired:
                                    SetStateToNoSession();
                                    break;
                                case CoordinatorExitReason.PotentialInconsistentState:
                                    await EvaluateTerminationAsync(token, clientOptions, "The client has entered a potentially inconsistent state");
                                    break;
                                case CoordinatorExitReason.FatalError:
                                    await TerminateAsync("fatal error", true);
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions, "The client has entered an unknown state");
                                    break;
                            }

                            break;
                        case ClientInternalState.IsFollower:
                            FollowerExitReason followerExitReason = await BecomeFollowerAsync(token);
                            switch (followerExitReason)
                            {
                                case FollowerExitReason.PossibleRoleChange:
                                    state = ClientInternalState.NoRole;
                                    break;
                                case FollowerExitReason.Cancelled:
                                    await TerminateAsync("cancellation", false);
                                    break;
                                case FollowerExitReason.SessionExpired:
                                    SetStateToNoSession();
                                    break;
                                case FollowerExitReason.FatalError:
                                    await TerminateAsync("fatal error", true);
                                    break;
                                case FollowerExitReason.PotentialInconsistentState:
                                    await EvaluateTerminationAsync(token, clientOptions, "The client has entered an potential inconsistent state");
                                    break;
                                default:
                                    await EvaluateTerminationAsync(token, clientOptions, "The client has entered an unknown state");
                                    break;
                            }

                            break;
                        case ClientInternalState.Error:
                            await EvaluateTerminationAsync(token, clientOptions, "The client has entered an error state");
                            break;
                        default:
                            await EvaluateTerminationAsync(token, clientOptions, "The client has entered an unknown state");
                            break;
                    }
                }
                catch (ZkSessionExpiredException)
                {
                    logger.Info(clientId, "ZooKeeper session lost");
                    SetStateToNoSession();
                }
                catch (ZkOperationCancelledException)
                {
                    await TerminateAsync("cancellation", false);
                }
                catch (TerminateClientException e)
                {
                    await TerminateAsync("Fatal error", true, e);
                }
                catch (InconsistentStateException e)
                {
                    await EvaluateTerminationAsync(token, clientOptions, "An error has caused that may have left the client in an inconsistent state.", e);
                }
                catch (Exception e)
                {
                    await EvaluateTerminationAsync(token, clientOptions, "An unexpected error has been caught", e);
                }
            }
        }

        public async Task WaitForCompletionAsync()
        {
            await mainTask;
        }

        public AssignedResources GetAssignedResources()
        {
            GetResourcesResponse assignment = resourceManager.GetResources();
            return new AssignedResources()
            {
                Resources = assignment.Resources,
                ClientState = GetState(assignment.AssignmentStatus)
            };
        }

        public ClientState GetState()
        {
            if (started)
            {
                AssignmentStatus assignmentState = resourceManager.GetAssignmentStatus();
                return GetState(assignmentState);
            }
            else if (aborted)
            {
                return ClientState.Aborted;
            }
            else
            {
                return ClientState.NotStarted;
            }
        }

        private ClientState GetState(AssignmentStatus assignmentState)
        {
            switch (assignmentState)
            {
                case AssignmentStatus.ResourcesAssigned:
                case AssignmentStatus.NoResourcesAssigned: return ClientState.Assigned;
                case AssignmentStatus.NoAssignmentYet: return ClientState.PendingAssignment;
                default: return ClientState.PendingAssignment;
            }
        }

        private void ResetMutableState()
        {
            clientId = string.Empty;
            clientNumber = -1;
            clientPath = string.Empty;
            watchSiblingNodePath = string.Empty;
            epoch = 0;
        }

        private async Task EvaluateTerminationAsync(CancellationToken token,
            ClientOptions clientOptions,
            string message,
            Exception e = null)
        {
            if (token.IsCancellationRequested)
            {
                await TerminateAsync("cancellation", false);
            }
            else if (clientOptions.AutoRecoveryOnError)
            {
                SetStateToNoSession();
                if (e != null)
                {
                    logger.Error(clientId, $"Error: {message} - {e} Auto-recovery enabled. Will restart in {clientOptions.RestartDelay.TotalMilliseconds}ms.");
                }
                else
                {
                    logger.Error(clientId, $"Error: {message} Auto-recovery enabled. Will restart in {clientOptions.RestartDelay.TotalMilliseconds}ms.");
                }

                await Task.Delay(clientOptions.RestartDelay);
            }
            else
            {
                await TerminateAsync($"Error: {message}. Auto-recovery disabled", true, e);
            }
        }

        private void SetStateToNoSession()
        {
            ResetMutableState();
            state = ClientInternalState.NoSession;
        }

        private async Task<NewSessionResult> EstablishSessionAsync(CancellationToken token)
        {
            int randomWait = rand.Next(2000);
            logger.Info(clientId, $"Will try to open a new session in {randomWait}ms");
            await Task.Delay(randomWait);

            // blocks until the session starts or timesout
            bool connected = await zooKeeperService.StartSessionAsync(sessionTimeout, connectTimeout, token);
            if (!connected)
            {
                logger.Error(clientId, "Failed to open a session, connect timeout exceeded");
                return NewSessionResult.TimeOut;
            }

            logger.Info(clientId, "Initializing zookeeper client paths");

            try
            {
                switch (rebalancingMode)
                {
                    case RebalancingMode.GlobalBarrier:
                        await zooKeeperService.InitializeGlobalBarrierAsync(
                            $"{zooKeeperRootPath}/{resourceGroup}/clients",
                            $"{zooKeeperRootPath}/{resourceGroup}/status",
                            $"{zooKeeperRootPath}/{resourceGroup}/stopped",
                            $"{zooKeeperRootPath}/{resourceGroup}/resources",
                            $"{zooKeeperRootPath}/{resourceGroup}/epoch");
                        break;
                    case RebalancingMode.ResourceBarrier:
                        await zooKeeperService.InitializeResourceBarrierAsync(
                            $"{zooKeeperRootPath}/{resourceGroup}/clients",
                            $"{zooKeeperRootPath}/{resourceGroup}/resources",
                            $"{zooKeeperRootPath}/{resourceGroup}/epoch");
                        break;
                }
            }
            catch (ZkInvalidOperationException e)
            {
                string msg =
                    "Could not start a new rebalancer client due to a problem with the prerequisite paths in ZooKeeper.";
                logger.Error(clientId, msg, e);
                return NewSessionResult.Error;
            }
            catch (Exception e)
            {
                string msg =
                    "An unexpected error occurred while intializing the rebalancer ZooKeeper paths";
                logger.Error(clientId, msg, e);
                return NewSessionResult.Error;
            }

            return NewSessionResult.Established;
        }

        private async Task<bool> CreateClientNodeAsync()
        {
            try
            {
                clientPath = await zooKeeperService.CreateClientAsync();
                SetIdFromPath();
                logger.Info(clientId, $"Client znode registered with Id {clientId}");
                return true;
            }
            catch (ZkInvalidOperationException e)
            {
                logger.Error(clientId, "Could not create the client znode.", e);
                return false;
            }
        }

        private void SetIdFromPath()
        {
            clientNumber = int.Parse(clientPath.Substring(clientPath.Length - 10, 10));
            clientId = clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal) + 1);
        }

        private async Task<bool> CacheEpochLocallyAsync()
        {
            try
            {
                epoch = await zooKeeperService.GetEpochAsync();
                return true;
            }
            catch (ZkInvalidOperationException e)
            {
                logger.Error(clientId, "Could not cache the epoch. ", e);
                return false;
            }
        }

        private async Task<(ElectionResult, string)> DetermineLeadershipAsync()
        {
            logger.Info(clientId, $"Looking for a next smaller sibling to watch");
            (bool success, string lowerSiblingPath) = await FindLowerSiblingAsync();
            if (success)
            {
                if (lowerSiblingPath == string.Empty)
                {
                    return (ElectionResult.IsLeader, string.Empty);
                }
                else
                {
                    return (ElectionResult.IsFollower, lowerSiblingPath);
                }
            }
            else
            {
                return (ElectionResult.Error, string.Empty);
            }
        }

        private async Task<(bool, string)> FindLowerSiblingAsync()
        {
            try
            {
                int maxClientNumber = -1;
                string watchChild = string.Empty;
                ClientsZnode clients = await zooKeeperService.GetActiveClientsAsync();
                foreach (string childPath in clients.ClientPaths)
                {
                    int siblingClientNumber = int.Parse(childPath.Substring(childPath.Length - 10, 10));
                    if (siblingClientNumber > maxClientNumber && siblingClientNumber < clientNumber)
                    {
                        watchChild = childPath;
                        maxClientNumber = siblingClientNumber;
                    }
                }

                if (maxClientNumber == -1)
                {
                    return (true, string.Empty);
                }

                return (true, watchChild);
            }
            catch (ZkInvalidOperationException e)
            {
                logger.Error(clientId, "Unable to determine if there are sibling clients with a lower id", e);
                return (false, string.Empty);
            }
        }

        private async Task<CoordinatorExitReason> BecomeCoordinatorAsync(CancellationToken token)
        {
            logger.Info(clientId, $"Becoming coordinator");
            ICoordinator coordinator;
            switch (rebalancingMode)
            {
                case RebalancingMode.GlobalBarrier:
                    coordinator = new GB.Coordinator(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        minimumRebalancingInterval,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
                case RebalancingMode.ResourceBarrier:
                    coordinator = new RB.Coordinator(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        minimumRebalancingInterval,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
                default:
                    coordinator = new RB.Coordinator(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        minimumRebalancingInterval,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
            }

            BecomeCoordinatorResult hasBecome = await coordinator.BecomeCoordinatorAsync(epoch);
            switch (hasBecome)
            {
                case BecomeCoordinatorResult.Ok:
                    logger.Info(clientId, $"Have successfully become the coordinator");

                    // this blocks until coordinator terminates (due to failure, session expiry or detects it is a zombie)
                    CoordinatorExitReason coordinatorExitReason = await coordinator.StartEventLoopAsync();
                    logger.Info(clientId, $"The coordinator has exited for reason {coordinatorExitReason}");
                    return coordinatorExitReason;
                case BecomeCoordinatorResult.StaleEpoch:
                    logger.Info(clientId,
                        "Since being elected, the epoch has been incremented suggesting another leader. Aborting coordinator role to check leadership again");
                    return CoordinatorExitReason.NoLongerCoordinator;
                default:
                    logger.Error(clientId, "Could not become coordinator");
                    return CoordinatorExitReason.PotentialInconsistentState;
            }
        }

        private async Task<FollowerExitReason> BecomeFollowerAsync(CancellationToken token)
        {
            logger.Info(clientId, $"Becoming a follower");

            IFollower follower;
            switch (rebalancingMode)
            {
                case RebalancingMode.GlobalBarrier:
                    follower = new GB.Follower(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        clientNumber,
                        watchSiblingNodePath,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
                case RebalancingMode.ResourceBarrier:
                    follower = new RB.Follower(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        clientNumber,
                        watchSiblingNodePath,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
                default:
                    follower = new RB.Follower(zooKeeperService,
                        logger,
                        resourceManager,
                        clientId,
                        clientNumber,
                        watchSiblingNodePath,
                        TimeSpan.FromMilliseconds((int)sessionTimeout.TotalMilliseconds / 3),
                        onStartDelay,
                        token);
                    break;
            }

            BecomeFollowerResult hasBecome = await follower.BecomeFollowerAsync();
            switch (hasBecome)
            {
                case BecomeFollowerResult.Ok:
                    logger.Info(clientId, $"Have become a follower, starting follower event loop");

                    // blocks until follower either fails, the session expires or the follower detects it might be the new leader
                    FollowerExitReason followerExitReason = await follower.StartEventLoopAsync();
                    logger.Info(clientId, $"The follower has exited for reason {followerExitReason}");
                    return followerExitReason;
                case BecomeFollowerResult.WatchSiblingGone:
                    logger.Info(clientId, $"The follower was unable to watch its sibling as the sibling has gone");
                    return FollowerExitReason.PossibleRoleChange;
                default:
                    logger.Error(clientId, "Could not become a follower");
                    return FollowerExitReason.PotentialInconsistentState;
            }
        }

        private async Task TerminateAsync(string terminationReason, bool aborted, Exception abortException = null)
        {
            try
            {
                state = ClientInternalState.Terminated;
                if (aborted)
                {
                    if (abortException != null)
                    {
                        logger.Error(clientId, $"Client aborting due: {terminationReason}. Exception: {abortException}");
                    }
                    else
                    {
                        logger.Error(clientId, $"Client aborting due: {terminationReason}");
                    }
                }
                else
                {
                    logger.Info(clientId, $"Client terminating due: {terminationReason}");
                }

                await zooKeeperService.CloseSessionAsync();
                await resourceManager.InvokeOnStopActionsAsync(clientId, "No role");

                if (aborted)
                {
                    await resourceManager.InvokeOnAbortActionsAsync(clientId, $"The client has aborted due to: {terminationReason}", abortException);
                }

                logger.Info(clientId, "Client terminated");
            }
            catch (TerminateClientException e)
            {
                logger.Error(clientId, "Client termination failure during invocation of on stop/abort actions", e);
            }
            finally
            {
                if (aborted)
                {
                    this.aborted = true;
                }

                started = false;
            }
        }

        private async Task WaitRandomTime(TimeSpan maxWait)
        {
            await Task.Delay(rand.Next((int)maxWait.TotalMilliseconds));
        }
    }
}