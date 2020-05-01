using System;

namespace Rebalancer.Redis.Leases
{
    public class AcquireLeaseRequest
    {
        public Guid ClientId { get; set; }
        public string ResourceGroup { get; set; }
    }
}
