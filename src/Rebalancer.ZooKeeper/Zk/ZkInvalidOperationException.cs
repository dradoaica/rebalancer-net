using System;

namespace Rebalancer.ZooKeeper.Zk
{
    public class ZkInvalidOperationException : Exception
    {
        public ZkInvalidOperationException(string message)
            : base(message)
        {

        }

        public ZkInvalidOperationException(string message, Exception ex)
            : base(message, ex)
        {

        }
    }
}