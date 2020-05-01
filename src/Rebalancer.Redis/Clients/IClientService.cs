using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Clients
{
    public interface IClientService
    {
        Task CreateClientAsync(string resourceGroup, Guid clientId);
        Task<List<Client>> GetActiveClientsAsync(string resourceGroup);
        Task<Client> KeepAliveAsync(Guid clientId);
        Task SetClientStatusAsync(Guid clientId, ClientStatus clientStatus);
        Task<ModifyClientResult> StopActivityAsync(int fencingToken, List<Client> clients);
        Task<ModifyClientResult> StartActivityAsync(int fencingToken, List<ClientStartRequest> clientStartRequests);
    }
}
