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
        public Task<List<string>> GetResourcesAsync(string resourceGroup)
        {
            throw new NotImplementedException();
        }
    }
}
