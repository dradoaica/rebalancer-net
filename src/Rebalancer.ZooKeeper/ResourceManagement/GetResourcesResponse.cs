using System.Collections.Generic;

namespace Rebalancer.ZooKeeper.ResourceManagement
{
    public class GetResourcesResponse
    {
        public AssignmentStatus AssignmentStatus { get; set; }
        public List<string> Resources { get; set; }
    }
}