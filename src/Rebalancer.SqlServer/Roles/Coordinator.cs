using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.SqlServer.Clients;
using Rebalancer.SqlServer.Resources;
using Rebalancer.SqlServer.Store;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Roles
{
    internal class Coordinator
    {
        private readonly IRebalancerLogger logger;
        private readonly IResourceService resourceService;
        private readonly IClientService clientService;
        private readonly ResourceGroupStore store;

        private List<string> resources;
        private List<Guid> clients;
        private int currentFencingToken;

        public Coordinator(IRebalancerLogger logger,
            IResourceService resourceService,
            IClientService clientService,
            ResourceGroupStore store)
        {
            this.logger = logger;
            this.resourceService = resourceService;
            this.clientService = clientService;
            this.store = store;
            resources = new List<string>();
            clients = new List<Guid>();
        }

        public int GetFencingToken()
        {
            return currentFencingToken;
        }

        public async Task ExecuteCoordinatorRoleAsync(Guid coordinatorClientId,
            ClientEvent clientEvent,
            OnChangeActions onChangeActions,
            CancellationToken token)
        {
            currentFencingToken = clientEvent.FencingToken;
            Client self = await clientService.KeepAliveAsync(coordinatorClientId);
            List<string> resourcesNow = (await resourceService.GetResourcesAsync(clientEvent.ResourceGroup)).OrderBy(x => x).ToList();
            List<Client> clientsNow = await GetLiveClientsAsync(clientEvent, coordinatorClientId);
            List<Guid> clientIds = clientsNow.Select(x => x.ClientId).ToList();
            clientIds.Add(coordinatorClientId);

            if (clientsNow.Any(x => x.FencingToken > clientEvent.FencingToken))
            {
                clientEvent.CoordinatorToken.FencingTokenViolation = true;
                return;
            }

            if (!resources.OrderBy(x => x).SequenceEqual(resourcesNow.OrderBy(x => x)))
            {
                logger.Debug(coordinatorClientId.ToString(), $"Resource change: Old: {string.Join(",", resources.OrderBy(x => x))} New: {string.Join(",", resourcesNow.OrderBy(x => x))}");
                await TriggerRebalancingAsync(coordinatorClientId, clientEvent, clientsNow, resourcesNow, onChangeActions, token);
            }
            else if (!clients.OrderBy(x => x).SequenceEqual(clientIds.OrderBy(x => x)))
            {
                logger.Debug(coordinatorClientId.ToString(), $"Client change: Old: {string.Join(",", clients.OrderBy(x => x))} New: {string.Join(",", clientIds.OrderBy(x => x))}");
                await TriggerRebalancingAsync(coordinatorClientId, clientEvent, clientsNow, resourcesNow, onChangeActions, token);
            }
            else
            {
                // no change, do nothing
            }
        }

        public int GetCurrentFencingToken()
        {
            return currentFencingToken;
        }

        private async Task<List<Client>> GetLiveClientsAsync(ClientEvent clientEvent, Guid coordinatorClientId)
        {
            List<Client> allClientsNow = (await clientService.GetActiveClientsAsync(clientEvent.ResourceGroup))
                                    .Where(x => x.ClientId != coordinatorClientId)
                                    .ToList();

            List<Client> liveClientsNow = allClientsNow.Where(x => (x.TimeNow - x.LastKeepAlive) < clientEvent.KeepAliveExpiryPeriod).ToList();

            return liveClientsNow;
        }

        private async Task TriggerRebalancingAsync(Guid coordinatorClientId,
            ClientEvent clientEvent,
            List<Client> clients,
            List<string> resources,
            OnChangeActions onChangeActions,
            CancellationToken token)
        {
            logger.Info(coordinatorClientId.ToString(), $"---------- Rebalancing triggered -----------");

            // request stop of all clients
            logger.Info(coordinatorClientId.ToString(), "COORDINATOR: Requested stop");
            if (clients.Any())
            {
                ModifyClientResult result = await clientService.StopActivityAsync(clientEvent.FencingToken, clients);
                if (result == ModifyClientResult.FencingTokenViolation)
                {
                    clientEvent.CoordinatorToken.FencingTokenViolation = true;
                    return;
                }
                else if (result == ModifyClientResult.Error)
                {
                    logger.Error(coordinatorClientId.ToString(), "COORDINATOR: Rebalancing error");
                    return;
                }
            }

            // stop all resource activity in local coordinator client
            foreach (Action onStopAction in onChangeActions.OnStopActions)
            {
                onStopAction.Invoke();
            }

            // wait for all live clients to confirm stopped
            bool allClientsWaiting = false;
            List<Client> clientsNow = null;
            while (!allClientsWaiting && !token.IsCancellationRequested)
            {
                await WaitFor(TimeSpan.FromSeconds(5), token);
                clientsNow = await GetLiveClientsAsync(clientEvent, coordinatorClientId);

                if (!clientsNow.Any())
                {
                    allClientsWaiting = true;
                }
                else
                {
                    allClientsWaiting = clientsNow.All(x => x.ClientStatus == ClientStatus.Waiting);
                }
            }
            logger.Info(coordinatorClientId.ToString(), "COORDINATOR: Stop confirmed");

            // assign resources first to coordinator then to other live clients
            if (token.IsCancellationRequested)
            {
                return;
            }
            else if (allClientsWaiting)
            {
                Queue<string> resourcesToAssign = new Queue<string>(resources);
                List<ClientStartRequest> clientStartRequests = new List<ClientStartRequest>();
                int remainingClients = clientsNow.Count + 1;
                int resourcesPerClient = Math.Max(1, resourcesToAssign.Count / remainingClients);

                ClientStartRequest coordinatorRequest = new ClientStartRequest
                {
                    ClientId = coordinatorClientId
                };
                while (coordinatorRequest.AssignedResources.Count < resourcesPerClient && resourcesToAssign.Any())
                {
                    coordinatorRequest.AssignedResources.Add(resourcesToAssign.Dequeue());
                }

                clientStartRequests.Add(coordinatorRequest);
                remainingClients--;

                foreach (Client client in clientsNow)
                {
                    resourcesPerClient = Math.Max(1, resourcesToAssign.Count / remainingClients);

                    ClientStartRequest request = new ClientStartRequest
                    {
                        ClientId = client.ClientId
                    };

                    while (request.AssignedResources.Count < resourcesPerClient && resourcesToAssign.Any())
                    {
                        request.AssignedResources.Add(resourcesToAssign.Dequeue());
                    }

                    clientStartRequests.Add(request);
                    remainingClients--;
                }

                if (token.IsCancellationRequested)
                {
                    return;
                }

                logger.Info(coordinatorClientId.ToString(), "COORDINATOR: Resources assigned");
                ModifyClientResult startResult = await clientService.StartActivityAsync(clientEvent.FencingToken, clientStartRequests);
                if (startResult == ModifyClientResult.FencingTokenViolation)
                {
                    clientEvent.CoordinatorToken.FencingTokenViolation = true;
                    return;
                }
                else if (startResult == ModifyClientResult.Error)
                {
                    logger.Error(coordinatorClientId.ToString(), "COORDINATOR: Rebalancing error");
                    return;
                }

                store.SetResources(new SetResourcesRequest() { AssignmentStatus = AssignmentStatus.ResourcesAssigned, Resources = coordinatorRequest.AssignedResources });
                foreach (Action<IList<string>> onStartAction in onChangeActions.OnStartActions)
                {
                    onStartAction.Invoke(coordinatorRequest.AssignedResources);
                }

                logger.Debug(coordinatorClientId.ToString(), "COORDINATOR: Local client started");

                List<Guid> clientIds = clientsNow.Select(x => x.ClientId).ToList();
                clientIds.Add(coordinatorClientId);
                this.clients = clientIds;
                this.resources = resources;
                logger.Info(coordinatorClientId.ToString(), $"---------- Activity Started -----------");
            }
            else
            {
                // log it
                logger.Info(coordinatorClientId.ToString(), "!!!");
                return;
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
