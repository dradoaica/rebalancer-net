using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.SqlServer.Clients;
using Rebalancer.SqlServer.Store;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Roles
{
    internal class Follower
    {
        private readonly IRebalancerLogger logger;
        private readonly IClientService clientService;
        private readonly ResourceGroupStore store;

        public Follower(IRebalancerLogger logger,
            IClientService clientService,
            ResourceGroupStore store)
        {
            this.logger = logger;
            this.clientService = clientService;
            this.store = store;
        }

        public async Task ExecuteFollowerRoleAsync(Guid followerClientId,
            ClientEvent clientEvent,
            OnChangeActions onChangeActions,
            CancellationToken token)
        {
            Client self = await clientService.KeepAliveAsync(followerClientId);
            logger.Debug(followerClientId.ToString(), $"FOLLOWER : Keep Alive sent. Coordinator: {self.CoordinatorStatus} Client: {self.ClientStatus}");
            if (self.CoordinatorStatus == CoordinatorStatus.StopActivity)
            {
                if (self.ClientStatus == ClientStatus.Active)
                {
                    logger.Info(followerClientId.ToString(), "-------------- Stopping activity ---------------");
                    logger.Debug(followerClientId.ToString(), "FOLLOWER : Invoking on stop actions");
                    foreach (Action stopAction in onChangeActions.OnStopActions)
                    {
                        stopAction.Invoke();
                    }

                    store.SetResources(new SetResourcesRequest() { AssignmentStatus = AssignmentStatus.AssignmentInProgress, Resources = new List<string>() });
                    await clientService.SetClientStatusAsync(followerClientId, ClientStatus.Waiting);
                    logger.Info(followerClientId.ToString(), $"FOLLOWER : State= {self.ClientStatus} -> WAITING");
                }
                else
                {
                    logger.Debug(followerClientId.ToString(), $"FOLLOWER : State= {self.ClientStatus}");
                }
            }
            else if (self.CoordinatorStatus == CoordinatorStatus.ResourcesGranted)
            {
                if (self.ClientStatus == ClientStatus.Waiting)
                {
                    if (self.AssignedResources.Any())
                    {
                        store.SetResources(new SetResourcesRequest() { AssignmentStatus = AssignmentStatus.ResourcesAssigned, Resources = self.AssignedResources });
                    }
                    else
                    {
                        store.SetResources(new SetResourcesRequest() { AssignmentStatus = AssignmentStatus.NoResourcesAssigned, Resources = new List<string>() });
                    }

                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    await clientService.SetClientStatusAsync(followerClientId, ClientStatus.Active);

                    if (self.AssignedResources.Any())
                    {
                        logger.Info(followerClientId.ToString(), $"FOLLOWER : Granted resources={string.Join(",", self.AssignedResources)}");
                    }
                    else
                    {
                        logger.Info(followerClientId.ToString(), "FOLLOWER : No resources available to be assigned.");
                    }

                    foreach (Action<IList<string>> startAction in onChangeActions.OnStartActions)
                    {
                        startAction.Invoke(self.AssignedResources.Any() ? self.AssignedResources : new List<string>());
                    }

                    logger.Info(followerClientId.ToString(), $"FOLLOWER : State={self.ClientStatus} -> ACTIVE");
                    logger.Info(followerClientId.ToString(), "-------------- Activity started ---------------");
                }
                else
                {
                    logger.Debug(followerClientId.ToString(), $"FOLLOWER : State= {self.ClientStatus}");
                }
            }
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
    }
}
