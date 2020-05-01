namespace Rebalancer.ZooKeeper
{
    public enum FollowerExitReason
    {
        PossibleRoleChange,
        Cancelled,
        SessionExpired,
        PotentialInconsistentState,
        FatalError
    }
}