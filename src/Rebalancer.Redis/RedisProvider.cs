using Rebalancer.Core;
using Rebalancer.Core.Logging;
using Rebalancer.Redis.Clients;
using Rebalancer.Redis.Leases;
using Rebalancer.Redis.Roles;
using Rebalancer.Redis.Store;
using System;
using System.ComponentModel.Design;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.Redis
{
    /// <summary>
    /// TODO: implement using redis
    /// </summary>
    public class RedisProvider : IRebalancerProvider
    {
        private readonly IRebalancerLogger logger;
        private readonly ILeaseService leaseService;
        private readonly IResourceService resourceService;
        private readonly IClientService clientService;
        private Guid clientId;
        private readonly Coordinator coordinator;
        private readonly Follower follower;
        private readonly string resourceGroup;
        private static readonly object startLockObj = new object();
        private readonly bool started;
        private readonly bool isCoordinator;
        private readonly ResourceGroupStore store;
        private readonly Task mainTask;

        public RedisProvider(string connectionString,
            IRebalancerLogger logger = null,
            ILeaseService leaseService = null,
            IResourceService resourceService = null,
            IClientService clientService = null)
        {
            if (logger == null)
            {
                this.logger = new NullRebalancerLogger();
            }
            else
            {
                this.logger = logger;
            }
        }

        public AssignedResources GetAssignedResources()
        {
            throw new NotImplementedException();
        }

        public ClientState GetState()
        {
            throw new NotImplementedException();
        }

        public Task StartAsync(string group, OnChangeActions onChangeActions, CancellationToken token, ClientOptions clientOptions)
        {
            throw new NotImplementedException();
        }

        public Task WaitForCompletionAsync()
        {
            throw new NotImplementedException();
        }
    }
}
