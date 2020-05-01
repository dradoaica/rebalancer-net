using System.Threading.Tasks;

namespace Rebalancer.Redis.Leases
{
    public interface ILeaseService
    {
        Task<LeaseResponse> TryAcquireLeaseAsync(AcquireLeaseRequest acquireLeaseRequest);
        Task<LeaseResponse> TryRenewLeaseAsync(RenewLeaseRequest renewLeaseRequest);
        Task RelinquishLeaseAsync(RelinquishLeaseRequest relinquishLeaseRequest);
    }
}
