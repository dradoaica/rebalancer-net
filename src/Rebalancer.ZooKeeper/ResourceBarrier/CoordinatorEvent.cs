namespace Rebalancer.ZooKeeper.ResourceBarrier
{
    public enum CoordinatorEvent
    {
        SessionExpired,
        NoLongerCoordinator,
        RebalancingTriggered,
        PotentialInconsistentState,
        FatalError
    }
}