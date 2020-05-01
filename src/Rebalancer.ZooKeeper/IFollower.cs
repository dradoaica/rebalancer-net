using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper
{
    public interface IFollower
    {
        Task<BecomeFollowerResult> BecomeFollowerAsync();
        Task<FollowerExitReason> StartEventLoopAsync();
    }
}