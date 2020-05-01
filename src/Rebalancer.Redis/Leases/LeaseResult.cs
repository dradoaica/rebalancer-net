namespace Rebalancer.Redis.Leases
{
    public enum LeaseResult
    {
        NoLease,
        Granted,
        Denied,
        TransientError,
        Error
    }
}
