using System.Collections.Generic;

namespace Rebalancer.ZooKeeper.Tests
{
    public class RunClientResult
    {
        public RunClientResult()
        {
            AssignedResources = new List<string>();
        }

        public IList<string> AssignedResources { get; set; }
        public bool Assigned { get; set; }
        public bool AssignmentCancelled { get; set; }
        public bool AssignmentErrored { get; set; }
    }
}