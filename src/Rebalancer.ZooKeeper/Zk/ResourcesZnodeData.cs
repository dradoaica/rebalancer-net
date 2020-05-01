using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Rebalancer.ZooKeeper.Zk
{
    [Serializable, DataContract(Namespace = "Rebalancer", Name = "ResourcesZnodeData")]
    public class ResourcesZnodeData
    {
        public ResourcesZnodeData()
        {
            Assignments = new List<ResourceAssignment>();
        }

        [DataMember(Name = "Assignments")]
        public List<ResourceAssignment> Assignments { get; set; }
    }
}