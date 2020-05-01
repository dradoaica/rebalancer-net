namespace Rebalancer.ZooKeeper.ResourceBarrier
{
    public enum FollowerEvent
    {
        SessionExpired,
        IsNewLeader,
        RebalancingTriggered,
        PotentialInconsistentState,
        FatalError
    }
}