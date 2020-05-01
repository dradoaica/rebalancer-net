using System.Collections.Generic;

namespace Rebalancer.ZooKeeper.Zk
{
    public class ClientsZnode
    {
        public int Version { get; set; }
        public List<string> ClientPaths { get; set; }
    }
}