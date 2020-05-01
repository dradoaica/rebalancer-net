using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Clients
{
    /// <summary>
    /// TODO: implement using redis
    /// </summary>
    internal class ClientService : IClientService
    {
        private readonly IDatabase cache;

        public ClientService(IDatabase cache)
        {
            this.cache = cache;
        }

        public Task CreateClientAsync(string resourceGroup, Guid clientId)
        {
            throw new NotImplementedException();
        }

        public Task<List<Client>> GetActiveClientsAsync(string resourceGroup)
        {
            throw new NotImplementedException();
        }

        public Task<Client> KeepAliveAsync(Guid clientId)
        {
            throw new NotImplementedException();
        }

        public Task SetClientStatusAsync(Guid clientId, ClientStatus clientStatus)
        {
            throw new NotImplementedException();
        }

        public Task<ModifyClientResult> StartActivityAsync(int fencingToken, List<ClientStartRequest> clientStartRequests)
        {
            throw new NotImplementedException();
        }

        public Task<ModifyClientResult> StopActivityAsync(int fencingToken, List<Client> clients)
        {
            throw new NotImplementedException();
        }
    }
}
