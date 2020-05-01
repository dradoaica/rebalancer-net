using System;

namespace Rebalancer.ZooKeeper.Zk
{
    public class ZkOperationCancelledException : Exception
    {
        public ZkOperationCancelledException(string message)
            : base(message)
        {

        }
    }
}