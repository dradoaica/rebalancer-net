namespace Rebalancer.Redis.Clients
{
    public enum ModifyClientResult
    {
        Ok,
        FencingTokenViolation,
        Error
    }
}
