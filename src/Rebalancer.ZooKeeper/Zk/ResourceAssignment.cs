using System;
using System.Runtime.Serialization;

namespace Rebalancer.ZooKeeper.Zk
{
    [Serializable, DataContract(Namespace = "Rebalancer", Name = "ResourceAssignment")]
    public class ResourceAssignment
    {
        [DataMember(Name = "r")]
        public string Resource { get; set; }
        [DataMember(Name = "c")]
        public string ClientId { get; set; }
    }
}