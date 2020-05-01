using System;

namespace Rebalancer.Core
{
    public class RebalancerException : Exception
    {
        public RebalancerException(string message)
            : base(message)
        {
        }

        public RebalancerException(string message, Exception ex)
            : base(message, ex)
        {
        }
    }
}
