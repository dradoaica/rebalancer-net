using Rebalancer.Core;
using Rebalancer.Redis.Utils;
using StackExchange.Redis;
using StackExchange.Redis.DataTypes.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Clients
{
    internal class ClientService : IClientService
    {
        private readonly IDatabase cache;

        public ClientService(IDatabase cache)
        {
            this.cache = cache;
        }

        public Task CreateClientAsync(string resourceGroup, Guid clientId)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            _ = new RedisDictionary<Guid, Client>(cache, cacheKey)
            {
                {
                    clientId,
                    new Client
                    {
                        ClientId = clientId,
                        ResourceGroup = resourceGroup,
                        ClientStatus = ClientStatus.Waiting,
                        CoordinatorStatus = CoordinatorStatus.StopActivity
                    }
                }
            };
            return Task.CompletedTask;
        }

        public Task<List<Client>> GetActiveClientsAsync(string resourceGroup)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            RedisDictionary<Guid, Client> redisDictionary = new RedisDictionary<Guid, Client>(cache, cacheKey);
            List<Client> clients = redisDictionary.Values.Where(x => x.ResourceGroup == resourceGroup && (x.ClientStatus == ClientStatus.Waiting || x.ClientStatus == ClientStatus.Active)).ToList();
            clients.ForEach(x => x.TimeNow = DateTime.UtcNow);
            return Task.FromResult(clients);
        }

        public Task<Client> KeepAliveAsync(Guid clientId)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            RedisDictionary<Guid, Client> redisDictionary = new RedisDictionary<Guid, Client>(cache, cacheKey);
            Client client = redisDictionary[clientId];
            if (client == null)
            {
                throw new RebalancerException($"No client exists with id {clientId}");
            }
            else
            {
                client.LastKeepAlive = DateTime.UtcNow;
                redisDictionary[client.ClientId] = client;
            }

            return Task.FromResult(client);
        }

        public Task SetClientStatusAsync(Guid clientId, ClientStatus clientStatus)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            RedisDictionary<Guid, Client> redisDictionary = new RedisDictionary<Guid, Client>(cache, cacheKey);
            Client client = redisDictionary[clientId];
            if (client != null)
            {
                client.ClientStatus = clientStatus;
                redisDictionary[client.ClientId] = client;
            }

            return Task.CompletedTask;
        }

        public Task<ModifyClientResult> StartActivityAsync(int fencingToken, List<ClientStartRequest> clientStartRequests)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            RedisDictionary<Guid, Client> redisDictionary = new RedisDictionary<Guid, Client>(cache, cacheKey);
            foreach (ClientStartRequest request in clientStartRequests)
            {
                Client client = redisDictionary.Values.FirstOrDefault(x => x.ClientId == request.ClientId && x.FencingToken <= fencingToken);
                if (client == null)
                {
                    Task.FromResult(ModifyClientResult.FencingTokenViolation);
                }
                else
                {
                    client.CoordinatorStatus = CoordinatorStatus.ResourcesGranted;
                    client.FencingToken = fencingToken;
                    client.AssignedResources = request.AssignedResources;
                    redisDictionary[client.ClientId] = client;
                }
            }

            return Task.FromResult(ModifyClientResult.Ok);
        }

        public Task<ModifyClientResult> StopActivityAsync(int fencingToken, List<Client> clients)
        {
            string cacheKey = $"{Constants.SCHEMA}:Clients";
            RedisDictionary<Guid, Client> redisDictionary = new RedisDictionary<Guid, Client>(cache, cacheKey);
            foreach (Client targetedClient in clients)
            {
                Client client = redisDictionary[targetedClient.ClientId];
                if (client == null)
                {
                    Task.FromResult(ModifyClientResult.FencingTokenViolation);
                }
                else
                {
                    client.CoordinatorStatus = CoordinatorStatus.StopActivity;
                    client.FencingToken = fencingToken;
                    client.AssignedResources = new List<string>();
                    redisDictionary[client.ClientId] = client;
                }
            }

            return Task.FromResult(ModifyClientResult.Ok);
        }
    }
}
