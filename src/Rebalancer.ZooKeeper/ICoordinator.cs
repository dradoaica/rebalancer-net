using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper
{
    public interface ICoordinator
    {
        Task<BecomeCoordinatorResult> BecomeCoordinatorAsync(int currentEpoch);
        Task<CoordinatorExitReason> StartEventLoopAsync();
    }
}