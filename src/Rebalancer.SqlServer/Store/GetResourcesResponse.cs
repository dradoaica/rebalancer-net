using System.Collections.Generic;

namespace Rebalancer.SqlServer.Store
{
    internal class GetResourcesResponse
    {
        public AssignmentStatus AssignmentStatus { get; set; }
        public List<string> Resources { get; set; }
    }
}
