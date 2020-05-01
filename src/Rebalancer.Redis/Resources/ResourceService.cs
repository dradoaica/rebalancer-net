using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Resources
{
    /// <summary>
    /// TODO: implement using redis
    /// </summary>
    internal class ResourceService : IResourceService
    {
        private readonly IDatabase cache;

        public ResourceService(IDatabase cache)
        {
            this.cache = cache;
        }

        public Task<List<string>> GetResourcesAsync(string resourceGroup)
        {
            throw new NotImplementedException();
        }
    }
}
