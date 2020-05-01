using System;

namespace Rebalancer.Core
{
    /// <summary>
    /// Registers functions that create Rebalancer providers
    /// </summary>
    public class Providers
    {
        private static Func<IRebalancerProvider> GetRebalancerProvider;

        public static void Register(Func<IRebalancerProvider> getRebalancerProvider)
        {
            if (GetRebalancerProvider == null)
            {
                GetRebalancerProvider = getRebalancerProvider;
            }
        }

        /// <summary>
        /// Called from inside the RebalancerClient
        /// </summary>
        /// <returns>An IRebalancerProvider implementation</returns>
        /// <exception cref="ProviderException"></exception>
        public static IRebalancerProvider GetProvider()
        {
            if (GetRebalancerProvider == null)
            {
                throw new ProviderException("No provider registered!");
            }

            return GetRebalancerProvider();
        }
    }
}
