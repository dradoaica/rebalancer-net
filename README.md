#  Rebalancer

Create Kafka style consumer groups in other technologies. Rebalanser was born of the need for consumer groups with RabbitMQ. But Rebalanser is completely technology agnostic and will balance activity over any group of resources across a group of participating nodes.

##  Use cases

- Create Kafka-like "consumer groups" with messaging technologies like RabbitMQ, SQS, etc.

- Consume a group of resources such as file shares, FTPs, S3 buckets between the instances of a scaled out application.

- Single Active Consumer / Active-Backup

- Create an application cluster that consumes a single resource in a highly available manner. The cluster leader (Coordinator) consumes the single resource and the slaves (Followers) remain idle in backup in case the leader dies.

###  Balancing assignment of multiple resources over multiples nodes

![](https://raw.githubusercontent.com/dradoaica/rebalancer-net-mssql/master/wiki/images/RebalancerMultipleNodesMultipleResources.png)

###  Consuming from a single resource with backup nodes in case of failure

![](https://raw.githubusercontent.com/dradoaica/rebalancer-net-mssql/master/wiki/images/RebalancerBackupNodes.png)

##  Concepts

Rebalanser consists of (or will consist of when complete):

- a set of protocols for different resource allocation algorithms

- a set of TLA+ specifications that verify the protocols

- a set of code libraries for multiple languages and backends

The important terms are:

- Resource Group = Group of Nodes + Group of Resources

When a node becomes active it notifies the other nodes. One node is the Coordinator (leader) and the rest are Followers. The Coordinator has the job to assign resources to nodes. It monitors the coming and going of nodes, as well as changes to the number of resources available to the resource group. When any change happens to the nodes or resources then the Coordinator triggers a rebalancing.

Different rebalancing algorithms exist:

- Leader based resource barrier

- Leader based global barrier

The rebalancing algorithm depends on the backend. With an RDBMS we use the following:

With an RDBMS we use the global barrier algorithm where:

1. The Coordinator orders all Followers to stop activity.

2. Once activity has stopped the Coordinator distributes the resource identifiers equally between the Followers and itself.

3. The final step is that the Coordinator notifies each Follower of the resources it has been assigned and can start its activity (consuming, reading, writing etc).

With ZooKeeper we can use a faster algorithm as is documented in the rebalanser-net-zookeeper [readme](https://github.com/Rebalanser/rebalanser-net-zookeeper)

Other backends are suited to either. With Apache ZooKeeper we can use the faster Resource Barrier algorithm as is documented in the rebalanser-net-zookeeper [readme](https://github.com/Rebalanser/rebalanser-net-zookeeper).

Leader election determines who the Coordinator is. If the Coordinator dies, then a Follower takes its place. Leader election and meta-data storage is performed via a consensus service (ZooKeeper, Etcd, Consul) or an RDBMS with serializable transaction support (SQL Server, Oracle, PostgreSQL). All communication between nodes is also performed via this backing meta-data store.

##  Languages and Backends

Rebalanser is a suite of code libraries. It must be implemented in your language in order to use it. Also, different backends will be available.
