using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Resources
{
    public interface IResourceService
    {
        Task<List<string>> GetResourcesAsync(string resourceGroup);
    }
}
