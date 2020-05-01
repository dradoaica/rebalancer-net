using org.apache.zookeeper;
using Rebalancer.Core.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper.Zk
{
    public class ZooKeeperService : Watcher, IZooKeeperService
    {
        private org.apache.zookeeper.ZooKeeper zookeeper;
        private readonly string zookeeperHosts;
        private string clientsPath;
        private string statusPath;
        private string stoppedPath;
        private string resourcesPath;
        private string epochPath;
        private Event.KeeperState keeperState;
        private CancellationToken token;
        private string clientId;
        private bool sessionExpired;

        public ZooKeeperService(string zookeeperHosts)
        {
            this.zookeeperHosts = zookeeperHosts;
            clientId = "-";
            sessionExpired = false;
        }

        public void SessionExpired()
        {
            sessionExpired = true;
        }

        public async Task InitializeGlobalBarrierAsync(string clientsPath,
            string statusPath,
            string stoppedPath,
            string resourcesPath,
            string epochPath)
        {
            this.clientsPath = clientsPath;
            this.statusPath = statusPath;
            this.stoppedPath = stoppedPath;
            this.resourcesPath = resourcesPath;
            this.epochPath = epochPath;

            await EnsurePathAsync(this.clientsPath);
            await EnsurePathAsync(this.epochPath);
            await EnsurePathAsync(this.statusPath, BitConverter.GetBytes(0));
            await EnsurePathAsync(this.stoppedPath);
            await EnsurePathAsync(this.resourcesPath, Encoding.UTF8.GetBytes(JSONSerializer<ResourcesZnodeData>.Serialize(new ResourcesZnodeData())));
        }

        public async Task InitializeResourceBarrierAsync(string clientsPath,
            string resourcesPath,
            string epochPath)
        {
            this.clientsPath = clientsPath;
            this.resourcesPath = resourcesPath;
            this.epochPath = epochPath;

            await EnsurePathAsync(this.clientsPath);
            await EnsurePathAsync(this.epochPath);
            await EnsurePathAsync(this.resourcesPath, Encoding.UTF8.GetBytes(JSONSerializer<ResourcesZnodeData>.Serialize(new ResourcesZnodeData())));
        }

        public Event.KeeperState GetKeeperState()
        {
            return keeperState;
        }

        public async Task<bool> StartSessionAsync(TimeSpan sessionTimeout, TimeSpan connectTimeout, CancellationToken token)
        {
            this.token = token;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            if (zookeeper != null)
            {
                await zookeeper.closeAsync();
            }

            zookeeper = new org.apache.zookeeper.ZooKeeper(
                zookeeperHosts,
                (int)sessionTimeout.TotalMilliseconds,
                this);

            while (keeperState != Event.KeeperState.SyncConnected && sw.Elapsed <= connectTimeout)
            {
                await Task.Delay(50);
            }

            bool connected = keeperState == Event.KeeperState.SyncConnected;
            sessionExpired = !connected;

            return connected;
        }

        public async Task CloseSessionAsync()
        {
            if (zookeeper != null)
            {
                await zookeeper.closeAsync();
            }

            zookeeper = null;
        }

        public override async Task process(WatchedEvent @event)
        {
            keeperState = @event.getState();
            await Task.Yield();
        }

        public async Task<string> CreateClientAsync()
        {
            string actionToPerform = "create client znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    string clientPath = await zookeeper.createAsync(
                        $"{clientsPath}/c_",
                        System.Text.Encoding.UTF8.GetBytes("0"),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);

                    clientId = clientPath.Substring(clientPath.LastIndexOf("/", StringComparison.Ordinal) + 1);

                    return clientPath;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as parent node does not exist", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task DeleteClientAsync(string clientPath)
        {
            string actionToPerform = "delete client znode";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.deleteAsync(clientPath);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, will try again in the next iteration
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired.", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task EnsurePathAsync(string znodePath, byte[] bytesToSet = null)
        {
            string actionToPerform = $"ensure path {znodePath}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    org.apache.zookeeper.data.Stat znodeStat = await zookeeper.existsAsync(znodePath);
                    if (znodeStat == null)
                    {
                        if (bytesToSet == null)
                        {
                            bytesToSet = System.Text.Encoding.UTF8.GetBytes("0");
                        }

                        await zookeeper.createAsync(znodePath,
                            bytesToSet,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                    }

                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    succeeded = true; // the node exists which is what we want
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, will try again in the next iteration
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired.", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> IncrementAndWatchEpochAsync(int currentEpoch, Watcher watcher)
        {
            string actionToPerform = "increment epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    byte[] data = System.Text.Encoding.UTF8.GetBytes("0");
                    org.apache.zookeeper.data.Stat stat = await zookeeper.setDataAsync(epochPath, data, currentEpoch);

                    DataResult dataRes = await zookeeper.getDataAsync(epochPath, watcher);
                    if (dataRes.Stat.getVersion() == stat.getVersion())
                    {
                        return dataRes.Stat.getVersion();
                    }
                    else
                    {
                        throw new ZkStaleVersionException("Between incrementing the epoch and setting a watch the epoch was incremented");
                    }
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} as the current epoch was incremented already.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> GetEpochAsync()
        {
            string actionToPerform = "get the current epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = await zookeeper.getDataAsync(epochPath);
                    return dataResult.Stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<ClientsZnode> GetActiveClientsAsync()
        {
            string actionToPerform = "get the list of active clients";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    ChildrenResult childrenResult = await zookeeper.getChildrenAsync(clientsPath);
                    List<string> childrenPaths = childrenResult.Children.Select(x => $"{clientsPath}/{x}").ToList();
                    return new ClientsZnode()
                    {
                        Version = childrenResult.Stat.getVersion(),
                        ClientPaths = childrenPaths
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the clients node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<StatusZnode> GetStatusAsync()
        {
            string actionToPerform = "get the status znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = await zookeeper.getDataAsync(statusPath);
                    RebalancingStatus status = RebalancingStatus.NotSet;
                    if (dataResult.Stat.getDataLength() > 0)
                    {
                        status = (RebalancingStatus)BitConverter.ToInt32(dataResult.Data, 0);
                    }

                    return new StatusZnode()
                    {
                        RebalancingStatus = status,
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> SetStatus(StatusZnode statusZnode)
        {
            string actionToPerform = "set the status znode";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    byte[] data = BitConverter.GetBytes((int)statusZnode.RebalancingStatus);
                    org.apache.zookeeper.data.Stat stat = await zookeeper.setDataAsync(statusPath, data, statusZnode.Version);
                    return stat.getVersion();
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} due to a bad version number.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task SetFollowerAsStopped(string clientId)
        {
            string actionToPerform = $"set follower {clientId} as stopped";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.createAsync(
                        $"{stoppedPath}/{clientId}",
                        System.Text.Encoding.UTF8.GetBytes("0"),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);

                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the stopped znode does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task SetFollowerAsStarted(string clientId)
        {
            string actionToPerform = $"set follower {clientId} as started";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.deleteAsync($"{stoppedPath}/{clientId}");
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException)
                {
                    succeeded = true;
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<ResourcesZnode> GetResourcesAsync(Watcher childWatcher, Watcher dataWatcher)
        {
            string actionToPerform = "get the list of resources";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = null;
                    if (dataWatcher != null)
                    {
                        dataResult = await zookeeper.getDataAsync(resourcesPath, dataWatcher);
                    }
                    else
                    {
                        dataResult = await zookeeper.getDataAsync(resourcesPath);
                    }

                    ChildrenResult childrenResult = null;
                    if (childWatcher != null)
                    {
                        childrenResult = await zookeeper.getChildrenAsync(resourcesPath, childWatcher);
                    }
                    else
                    {
                        childrenResult = await zookeeper.getChildrenAsync(resourcesPath);
                    }

                    ResourcesZnodeData resourcesZnodeData = JSONSerializer<ResourcesZnodeData>.DeSerialize(
                        System.Text.Encoding.UTF8.GetString(dataResult.Data));

                    if (resourcesZnodeData == null)
                    {
                        resourcesZnodeData = new ResourcesZnodeData();
                    }

                    return new ResourcesZnode()
                    {
                        ResourceAssignments = resourcesZnodeData,
                        Resources = childrenResult.Children,
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> SetResourcesAsync(ResourcesZnode resourcesZnode)
        {
            string actionToPerform = "set resource assignments";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    byte[] data = System.Text.Encoding.UTF8.GetBytes(
                        JSONSerializer<ResourcesZnodeData>.Serialize(resourcesZnode.ResourceAssignments));
                    org.apache.zookeeper.data.Stat stat = await zookeeper.setDataAsync(resourcesPath, data, resourcesZnode.Version);
                    return stat.getVersion();
                }
                catch (KeeperException.BadVersionException e)
                {
                    throw new ZkStaleVersionException($"Could not {actionToPerform} due to a bad version number.", e);
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task RemoveResourceBarrierAsync(string resource)
        {
            string actionToPerform = $"remove resource barrier on {resource}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.deleteAsync($"{resourcesPath}/{resource}/barrier");
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException)
                {
                    succeeded = true;
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task TryPutResourceBarrierAsync(string resource, CancellationToken waitToken, IRebalancerLogger logger)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            string actionToPerform = $"try put resource barrier on {resource}";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.createAsync(
                        $"{resourcesPath}/{resource}/barrier",
                        System.Text.Encoding.UTF8.GetBytes(clientId),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                    succeeded = true;
                }
                catch (KeeperException.NodeExistsException)
                {
                    (bool exists, string owner) = await GetResourceBarrierOwnerAsync(resource);
                    if (exists && owner.Equals(clientId))
                    {
                        succeeded = true;
                    }
                    else
                    {
                        logger.Info(clientId, $"Waiting for {owner} to release its barrier on {resource}");
                        // wait for two seconds, will retry in next iteration
                        for (int i = 0; i < 20; i++)
                        {
                            await WaitFor(TimeSpan.FromMilliseconds(100));
                            if (waitToken.IsCancellationRequested)
                            {
                                throw new ZkOperationCancelledException(
                                    $"Could not {actionToPerform} as the operation was cancelled.");
                            }
                        }
                    }
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the resource node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        private async Task<(bool, string)> GetResourceBarrierOwnerAsync(string resource)
        {
            string actionToPerform = "get resource barrier owner";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = await zookeeper.getDataAsync($"{resourcesPath}/{resource}/barrier");
                    return (true, System.Text.Encoding.UTF8.GetString(dataResult.Data));
                }
                catch (KeeperException.NoNodeException)
                {
                    return (false, string.Empty);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<List<string>> GetStoppedAsync()
        {
            string actionToPerform = "get the list of stopped clients";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    ChildrenResult childrenResult = await zookeeper.getChildrenAsync(stoppedPath);
                    return childrenResult.Children;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the stopped node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> WatchEpochAsync(Watcher watcher)
        {
            string actionToPerform = "set a watch on epoch";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    org.apache.zookeeper.data.Stat stat = await zookeeper.existsAsync(epochPath, watcher);
                    return stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the epoch node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<StatusZnode> WatchStatusAsync(Watcher watcher)
        {
            string actionToPerform = "set a watch on status";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult dataResult = await zookeeper.getDataAsync(statusPath, watcher);
                    return new StatusZnode()
                    {
                        RebalancingStatus = (RebalancingStatus)BitConverter.ToInt32(dataResult.Data, 0),
                        Version = dataResult.Stat.getVersion()
                    };
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the status node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task WatchResourcesChildrenAsync(Watcher watcher)
        {
            string actionToPerform = "set a watch on resource children";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.getChildrenAsync(resourcesPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task<int> WatchResourcesDataAsync(Watcher watcher)
        {
            string actionToPerform = "set a watch on resource data";
            while (true)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    DataResult data = await zookeeper.getDataAsync(resourcesPath, watcher);
                    return data.Stat.getVersion();
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException(
                        $"Could not {actionToPerform} as the resources node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task WatchNodesAsync(Watcher watcher)
        {
            string actionToPerform = "set a watch on clients children";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.getChildrenAsync(clientsPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} as the clients node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        public async Task WatchSiblingNodeAsync(string siblingPath, Watcher watcher)
        {
            string actionToPerform = "set a watch on sibling client";
            bool succeeded = false;
            while (!succeeded)
            {
                await BlockUntilConnected(actionToPerform);

                try
                {
                    await zookeeper.getDataAsync(siblingPath, watcher);
                    succeeded = true;
                }
                catch (KeeperException.NoNodeException e)
                {
                    throw new ZkNoEphemeralNodeWatchException($"Could not {actionToPerform} as the client node does not exist.", e);
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // do nothing, the next iteration will try again
                }
                catch (KeeperException.SessionExpiredException e)
                {
                    throw new ZkSessionExpiredException($"Could not {actionToPerform} as the session has expired: ", e);
                }
                catch (Exception e)
                {
                    throw new ZkInvalidOperationException($"Could not {actionToPerform} due to an unexpected error", e);
                }
            }
        }

        private async Task BlockUntilConnected(string logAction)
        {
            while (!sessionExpired && !token.IsCancellationRequested && keeperState != Event.KeeperState.SyncConnected)
            {
                if (keeperState == Event.KeeperState.Expired)
                {
                    throw new ZkSessionExpiredException($"Could not {logAction} because the session has expired");
                }

                await WaitFor(TimeSpan.FromMilliseconds(100));
            }

            if (token.IsCancellationRequested)
            {
                throw new ZkOperationCancelledException($"Could not {logAction} because the operation was cancelled");
            }

            if (sessionExpired || keeperState == Event.KeeperState.Expired)
            {
                throw new ZkSessionExpiredException($"Could not {logAction} because the session has expired");
            }
        }

        private async Task WaitFor(TimeSpan waitPeriod)
        {
            try
            {
                await Task.Delay(waitPeriod, token);
            }
            catch (TaskCanceledException)
            { }
        }

        private async Task WaitFor(TimeSpan waitPeriod, CancellationToken waitToken)
        {
            try
            {
                await Task.Delay(waitPeriod, waitToken);
            }
            catch (TaskCanceledException)
            { }
        }
    }
}