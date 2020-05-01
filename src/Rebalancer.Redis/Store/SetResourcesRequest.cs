using System.Collections.Generic;

namespace Rebalancer.Redis.Store
{
    internal class SetResourcesRequest
    {
        public AssignmentStatus AssignmentStatus { get; set; }
        public List<string> Resources { get; set; }
    }
}
