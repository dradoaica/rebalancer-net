using System;

namespace Rebalancer.ZooKeeper.Zk
{
    public class ZkSessionExpiredException : Exception
    {
        public ZkSessionExpiredException(string message)
            : base(message)
        {

        }

        public ZkSessionExpiredException(string message, Exception ex)
            : base(message, ex)
        {

        }
    }
}