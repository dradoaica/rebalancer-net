using Rebalancer.Core.Logging;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Leases
{
    /// <summary>
    /// TODO: implement using redis
    /// </summary>
    internal class LeaseService : ILeaseService
    {
        private readonly IDatabase cache;
        private readonly IRebalancerLogger logger;

        public LeaseService(IDatabase cache, IRebalancerLogger logger)
        {
            this.cache = cache;
            this.logger = logger;
        }

        public Task RelinquishLeaseAsync(RelinquishLeaseRequest relinquishLeaseRequest)
        {
            throw new NotImplementedException();
        }

        public Task<LeaseResponse> TryAcquireLeaseAsync(AcquireLeaseRequest acquireLeaseRequest)
        {
            throw new NotImplementedException();
        }

        public Task<LeaseResponse> TryRenewLeaseAsync(RenewLeaseRequest renewLeaseRequest)
        {
            throw new NotImplementedException();
        }
    }
}
