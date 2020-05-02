using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.Redis.Clients;
using Rebalancer.Redis.Leases;
using Rebalancer.Redis.Resources;
using Rebalancer.Redis.Roles;
using Rebalancer.Redis.Store;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.Redis
{
    public class RedisProvider : IRebalancerProvider
    {
        private readonly IRebalancerLogger logger;
        private readonly ILeaseService leaseService;
        private readonly IResourceService resourceService;
        private readonly IClientService clientService;
        private Guid clientId;
        private readonly Coordinator coordinator;
        private readonly Follower follower;
        private string resourceGroup;
        private static readonly object startLockObj = new object();
        private bool started;
        private bool isCoordinator;
        private readonly ResourceGroupStore store;
        private Task mainTask;
        private readonly ConnectionMultiplexer connection;
        private readonly IDatabase cache;

        public RedisProvider(string connectionString,
            IRebalancerLogger logger = null,
            ILeaseService leaseService = null,
            IResourceService resourceService = null,
            IClientService clientService = null)
        {
            connection = ConnectionMultiplexer.Connect(connectionString);
            cache = connection.GetDatabase();
            store = new ResourceGroupStore();

            if (logger == null)
            {
                this.logger = new NullRebalancerLogger();
            }
            else
            {
                this.logger = logger;
            }

            if (logger == null)
            {
                this.logger = new NullRebalancerLogger();
            }
            else
            {
                this.logger = logger;
            }

            if (leaseService == null)
            {
                this.leaseService = new LeaseService(cache);
            }
            else
            {
                this.leaseService = leaseService;
            }

            if (resourceService == null)
            {
                this.resourceService = new ResourceService(cache);
            }
            else
            {
                this.resourceService = resourceService;
            }

            if (clientService == null)
            {
                this.clientService = new ClientService(cache);
            }
            else
            {
                this.clientService = clientService;
            }

            clientId = Guid.NewGuid();
            coordinator = new Coordinator(this.logger, this.resourceService, this.clientService, store);
            follower = new Follower(this.logger, this.clientService, store);
        }

        public async Task StartAsync(string resourceGroup,
                    OnChangeActions onChangeActions,
                    CancellationToken parentToken,
                    ClientOptions clientOptions)
        {
            // just in case someone does some concurrency
            lock (startLockObj)
            {
                if (started)
                {
                    throw new RebalancerException("Context already started");
                }

                started = true;
            }

            this.resourceGroup = resourceGroup;
            await clientService.CreateClientAsync(this.resourceGroup, clientId);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            mainTask = Task.Run(async () =>
            {
                while (!parentToken.IsCancellationRequested)
                {
                    CancellationTokenSource childTaskCts = new CancellationTokenSource();
                    try
                    {
                        BlockingCollection<ClientEvent> clientEvents = new BlockingCollection<ClientEvent>();

                        Task leaderElectionTask = StartLeadershipTask(childTaskCts.Token, clientEvents);
                        Task roleTask = StartRoleTask(clientOptions.OnAssignmentDelay, childTaskCts.Token, onChangeActions, clientEvents);

                        while (!parentToken.IsCancellationRequested
                            && !leaderElectionTask.IsCompleted
                            && !roleTask.IsCompleted
                            && !clientEvents.IsCompleted)
                        {
                            await Task.Delay(100);
                        }

                        // cancel child tasks
                        childTaskCts.Cancel();

                        if (parentToken.IsCancellationRequested)
                        {
                            logger.Info(clientId.ToString(), "Context shutting down due to cancellation");
                        }
                        else
                        {
                            if (leaderElectionTask.IsFaulted)
                            {
                                await NotifyOfErrorAsync(leaderElectionTask, $"Shutdown due to leader election task fault. Automatic restart is set to {clientOptions.AutoRecoveryOnError}", onChangeActions);
                            }
                            else if (roleTask.IsFaulted)
                            {
                                await NotifyOfErrorAsync(roleTask, $"Shutdown due to coordinator/follower task fault. Automatic restart is set to {clientOptions.AutoRecoveryOnError}", onChangeActions);
                            }
                            else
                            {
                                NotifyOfError(onChangeActions, $"Unknown shutdown reason. Automatic restart is set to {clientOptions.AutoRecoveryOnError}", null);
                            }

                            if (clientOptions.AutoRecoveryOnError)
                            {
                                await WaitFor(clientOptions.RestartDelay, parentToken);
                            }
                            else
                            {
                                break;
                            }
                        }

                        if (!leaderElectionTask.IsFaulted)
                        {
                            await leaderElectionTask;
                        }

                        if (!roleTask.IsFaulted)
                        {
                            await roleTask;
                        }

                        clientService.SetClientStatusAsync(clientId, ClientStatus.Terminated);

                        if (isCoordinator)
                        {
                            leaseService.RelinquishLeaseAsync(new RelinquishLeaseRequest()
                            {
                                ClientId = clientId,
                                FencingToken = coordinator.GetCurrentFencingToken(),
                                ResourceGroup = this.resourceGroup
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        NotifyOfError(onChangeActions, $"An unexpected error has caused shutdown. Automatic restart is set to {clientOptions.AutoRecoveryOnError}", ex);

                        if (clientOptions.AutoRecoveryOnError)
                        {
                            await WaitFor(clientOptions.RestartDelay, parentToken);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        public async Task WaitForCompletionAsync()
        {
            try
            {
                await mainTask;
            }
            catch (Exception ex)
            {
                logger.Error(clientId.ToString(), ex);
            }
        }

        private async Task NotifyOfErrorAsync(Task faultedTask, string message, OnChangeActions onChangeActions)
        {
            await InvokeOnErrorAsync(faultedTask, message, onChangeActions);
            InvokeOnStop(onChangeActions);
        }

        private void NotifyOfError(OnChangeActions onChangeActions, string message, Exception exception)
        {
            InvokeOnError(onChangeActions, message, exception);
            InvokeOnStop(onChangeActions);
        }

        private async Task InvokeOnErrorAsync(Task faultedTask, string message, OnChangeActions onChangeActions)
        {
            try
            {
                await faultedTask;
            }
            catch (Exception ex)
            {
                InvokeOnError(onChangeActions, message, ex);
            }
        }

        private void InvokeOnError(OnChangeActions onChangeActions, string message, Exception exception)
        {
            try
            {
                foreach (Action<string, Exception> onAbortActions in onChangeActions.OnAbortActions)
                {
                    onAbortActions.Invoke(message, exception);
                }
            }
            catch (Exception ex)
            {
                logger.Error(clientId.ToString(), ex.ToString());
            }
        }

        private void InvokeOnStop(OnChangeActions onChangeActions)
        {
            try
            {
                foreach (Action onErrorAction in onChangeActions.OnStopActions)
                {
                    onErrorAction.Invoke();
                }
            }
            catch (Exception ex)
            {
                logger.Error(clientId.ToString(), ex.ToString());
            }
        }

        private Task StartLeadershipTask(CancellationToken token,
            BlockingCollection<ClientEvent> clientEvents)
        {
            return Task.Run(async () =>
            {
                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        AcquireLeaseRequest request = new AcquireLeaseRequest()
                        {
                            ClientId = clientId,
                            ResourceGroup = resourceGroup
                        };
                        LeaseResponse response = await TryAcquireLeaseAsync(request, token);
                        if (response.Result == LeaseResult.Granted) // is now the Coordinator
                        {
                            await ExecuteLeaseRenewals(token, clientEvents, response.Lease);
                        }
                        else if (response.Result == LeaseResult.Denied) // is a Follower
                        {
                            PostFollowerEvent(response.Lease.ExpiryPeriod, clientEvents);
                            await WaitFor(GetInterval(response.Lease.HeartbeatPeriod), token);
                        }
                        else if (response.Result == LeaseResult.NoLease)
                        {
                            throw new RebalancerException($"The resource group {resourceGroup} does not exist.");
                        }
                        else if (response.IsErrorResponse())
                        {
                            throw new RebalancerException("An non-recoverable error occurred.", response.Exception);
                        }
                        else
                        {
                            throw new RebalancerException("A non-supported lease result was received"); // should never happen, just in case I screw up in the future
                        }
                    }
                }
                finally
                {
                    clientEvents.CompleteAdding();
                }
            });
        }

        private async Task ExecuteLeaseRenewals(CancellationToken token,
            BlockingCollection<ClientEvent> clientEvents,
            Lease lease)
        {
            CoordinatorToken coordinatorToken = new CoordinatorToken();
            PostLeaderEvent(lease.FencingToken, lease.ExpiryPeriod, coordinatorToken, clientEvents);
            await WaitFor(GetInterval(lease.HeartbeatPeriod), token, coordinatorToken);

            // lease renewal loop
            while (!token.IsCancellationRequested && !coordinatorToken.FencingTokenViolation)
            {
                LeaseResponse response = await TryRenewLeaseAsync(new RenewLeaseRequest() { ClientId = clientId, ResourceGroup = resourceGroup, FencingToken = lease.FencingToken }, token);
                if (response.Result == LeaseResult.Granted)
                {
                    PostLeaderEvent(lease.FencingToken, lease.ExpiryPeriod, coordinatorToken, clientEvents);
                    await WaitFor(GetInterval(lease.HeartbeatPeriod), token, coordinatorToken);
                }
                else if (response.Result == LeaseResult.Denied)
                {
                    PostFollowerEvent(lease.ExpiryPeriod, clientEvents);
                    await WaitFor(GetInterval(lease.HeartbeatPeriod), token);
                    break;
                }
                else if (response.Result == LeaseResult.NoLease)
                {
                    throw new RebalancerException($"The resource group {resourceGroup} does not exist.");
                }
                else if (response.IsErrorResponse())
                {
                    throw new RebalancerException("An non-recoverable error occurred.", response.Exception);
                }
                else
                {
                    throw new RebalancerException("A non-supported lease result was received"); // should never happen, just in case I screw up in the future
                }
            }
        }

        private async Task<LeaseResponse> TryAcquireLeaseAsync(AcquireLeaseRequest request, CancellationToken token)
        {
            int delaySeconds = 2;
            int triesLeft = 3;
            while (triesLeft > 0)
            {
                triesLeft--;
                LeaseResponse response = await leaseService.TryAcquireLeaseAsync(request);
                if (response.Result != LeaseResult.TransientError)
                {
                    return response;
                }
                else if (triesLeft > 0)
                {
                    await WaitFor(TimeSpan.FromSeconds(delaySeconds), token);
                }
                else
                {
                    return response;
                }

                delaySeconds = delaySeconds * 2;
            }

            // this should never happen
            return new LeaseResponse() { Result = LeaseResult.Error };
        }

        private async Task<LeaseResponse> TryRenewLeaseAsync(RenewLeaseRequest request, CancellationToken token)
        {
            int delaySeconds = 2;
            int triesLeft = 3;
            while (triesLeft > 0)
            {
                triesLeft--;
                LeaseResponse response = await leaseService.TryRenewLeaseAsync(request);
                if (response.Result != LeaseResult.TransientError)
                {
                    return response;
                }
                else if (triesLeft > 0)
                {
                    await WaitFor(TimeSpan.FromSeconds(delaySeconds), token);
                }
                else
                {
                    return response;
                }

                delaySeconds = delaySeconds * 2;
            }

            // this should never happen
            return new LeaseResponse() { Result = LeaseResult.Error };
        }

        private TimeSpan GetInterval(TimeSpan leaseExpiry)
        {
            return TimeSpan.FromMilliseconds(leaseExpiry.TotalMilliseconds / 2.5);
        }

        private void PostLeaderEvent(int fencingToken,
            TimeSpan keepAliveExpiryPeriod,
            CoordinatorToken coordinatorToken,
            BlockingCollection<ClientEvent> clientEvents)
        {
            logger.Debug(clientId.ToString(), $"{clientId} is leader");
            isCoordinator = true;
            ClientEvent clientEvent = new ClientEvent()
            {
                ResourceGroup = resourceGroup,
                EventType = EventType.Coordinator,
                FencingToken = fencingToken,
                CoordinatorToken = coordinatorToken,
                KeepAliveExpiryPeriod = keepAliveExpiryPeriod
            };
            clientEvents.Add(clientEvent);
        }

        private void PostFollowerEvent(TimeSpan keepAliveExpiryPeriod,
            BlockingCollection<ClientEvent> clientEvents)
        {
            logger.Debug(clientId.ToString(), $"{clientId} is follower");
            isCoordinator = false;
            ClientEvent clientEvent = new ClientEvent()
            {
                EventType = EventType.Follower,
                ResourceGroup = resourceGroup,
                KeepAliveExpiryPeriod = keepAliveExpiryPeriod
            };
            clientEvents.Add(clientEvent);
        }

        private Task StartRoleTask(TimeSpan onStartDelay,
            CancellationToken token,
            OnChangeActions onChangeActions,
            BlockingCollection<ClientEvent> clientEvents)
        {
            return Task.Run(async () =>
            {
                while (!token.IsCancellationRequested && !clientEvents.IsAddingCompleted)
                {
                    // take the most recent event, if multiple are queued up then we only need the latest
                    ClientEvent clientEvent = null;
                    while (clientEvents.Any())
                    {
                        try
                        {
                            clientEvent = clientEvents.Take(token);
                        }
                        catch (OperationCanceledException) { }
                    }

                    // if there was an event then call the appropriate role beahvaiour
                    if (clientEvent != null)
                    {
                        if (clientEvent.EventType == EventType.Coordinator)
                        {
                            if (onStartDelay.Ticks > 0)
                            {
                                logger.Debug(clientId.ToString(), $"Coordinator - Delaying on start for {(int)onStartDelay.TotalMilliseconds}ms");
                                await WaitFor(onStartDelay, token);
                            }

                            if (token.IsCancellationRequested)
                            {
                                return;
                            }

                            await coordinator.ExecuteCoordinatorRoleAsync(clientId,
                                    clientEvent,
                                    onChangeActions,
                                    token);
                        }
                        else
                        {
                            if (onStartDelay.Ticks > 0)
                            {
                                logger.Debug(clientId.ToString(), $"Follower - Delaying on start for {(int)onStartDelay.TotalMilliseconds}ms");
                                await WaitFor(onStartDelay, token);
                            }

                            if (token.IsCancellationRequested)
                            {
                                return;
                            }

                            await follower.ExecuteFollowerRoleAsync(clientId,
                                    clientEvent,
                                    onChangeActions,
                                    token);
                        }
                    }
                    else
                    {
                        await WaitFor(TimeSpan.FromSeconds(1), token);
                    }
                }
            });
        }

        private async Task WaitFor(TimeSpan delayPeriod, CancellationToken token, CoordinatorToken coordinatorToken)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (!token.IsCancellationRequested && !coordinatorToken.FencingTokenViolation)
            {
                if (sw.Elapsed < delayPeriod)
                {
                    await Task.Delay(100);
                }
                else
                {
                    break;
                }
            }
        }

        private async Task WaitFor(TimeSpan delayPeriod, CancellationToken token)
        {
            try
            {
                await Task.Delay(delayPeriod, token);
            }
            catch (TaskCanceledException)
            {
            }
        }

        public AssignedResources GetAssignedResources()
        {
            while (true)
            {
                GetResourcesResponse response = store.GetResources();
                if (response.AssignmentStatus == AssignmentStatus.ResourcesAssigned || response.AssignmentStatus == AssignmentStatus.NoResourcesAssigned)
                {
                    return new AssignedResources()
                    {
                        Resources = response.Resources,
                        ClientState = GetState(response.AssignmentStatus)
                    };
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
        }

        public AssignedResources GetAssignedResources(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                GetResourcesResponse response = store.GetResources();
                if (response.AssignmentStatus == AssignmentStatus.ResourcesAssigned || response.AssignmentStatus == AssignmentStatus.NoResourcesAssigned)
                {
                    return new AssignedResources()
                    {
                        Resources = response.Resources,
                        ClientState = GetState(response.AssignmentStatus)
                    };
                }
                else
                {
                    Thread.Sleep(100);
                }
            }

            return new AssignedResources()
            {
                Resources = new List<string>(),
                ClientState = ClientState.Stopped
            };
        }

        public ClientState GetState()
        {
            if (started)
            {
                GetResourcesResponse response = store.GetResources();
                return GetState(response.AssignmentStatus);
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
                case AssignmentStatus.AssignmentInProgress:
                default: return ClientState.PendingAssignment;
            }
        }
    }
}
