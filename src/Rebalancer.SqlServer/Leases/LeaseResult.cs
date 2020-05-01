namespace Rebalancer.SqlServer.Leases
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
