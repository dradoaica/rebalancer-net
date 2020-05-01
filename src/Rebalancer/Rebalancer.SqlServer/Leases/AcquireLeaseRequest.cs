using System;

namespace Rebalancer.SqlServer.Leases
{
    public class AcquireLeaseRequest
    {
        public Guid ClientId { get; set; }
        public string ResourceGroup { get; set; }
    }
}
