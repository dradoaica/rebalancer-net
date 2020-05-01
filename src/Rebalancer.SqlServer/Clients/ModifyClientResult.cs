namespace Rebalancer.SqlServer.Clients
{
    public enum ModifyClientResult
    {
        Ok,
        FencingTokenViolation,
        Error
    }
}
