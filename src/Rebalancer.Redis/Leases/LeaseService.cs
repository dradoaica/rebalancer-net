using System;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Leases
{
    /// <summary>
    /// TODO: implement using redis
    /// </summary>
    internal class LeaseService : ILeaseService
    {
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
