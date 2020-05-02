using StackExchange.Redis;
using System;

namespace Rebalancer.Redis.Leases
{
    internal static class TransientErrorDetector
    {
        /// <summary>
        /// Determines whether the specified exception represents a transient failure that can be compensated by a retry.
        /// </summary>
        /// <param name="ex">The exception object to be verified.</param>
        /// <returns>true if the specified exception is considered as transient; otherwise, false.</returns>
        public static bool IsTransient(Exception ex)
        {
            if (ex != null)
            {
                if (ex is TimeoutException || ex is RedisTimeoutException || ex is RedisConnectionException)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
