using System;
using System.Collections.Generic;

namespace Rebalancer.Redis.Clients
{
    public class Client
    {
        public Client()
        {
            AssignedResources = new List<string>();
        }

        public Guid ClientId { get; set; }
        public string ResourceGroup { get; set; }
        public DateTime LastKeepAlive { get; set; }
        public DateTime TimeNow { get; set; }
        public ClientStatus ClientStatus { get; set; }
        public CoordinatorStatus CoordinatorStatus { get; set; }
        public int FencingToken { get; set; }
        public List<string> AssignedResources { get; set; }
    }
}
