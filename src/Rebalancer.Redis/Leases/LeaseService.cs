using Rebalancer.Redis.Utils;
using StackExchange.Redis;
using StackExchange.Redis.DataTypes.Collections;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Rebalancer.Redis.Leases
{
    internal class LeaseService : ILeaseService
    {
        private readonly IDatabase cache;

        public LeaseService(IDatabase cache)
        {
            this.cache = cache;
        }

        public Task<LeaseResponse> TryAcquireLeaseAsync(AcquireLeaseRequest acquireLeaseRequest)
        {
            ResourceGroup resourceGroup = null;
            RedisDictionary<string, ResourceGroup> redisDictionary = null;
            try
            {
                string cacheKey = $"{Constants.SCHEMA}:ResourceGroups";
                redisDictionary = new RedisDictionary<string, ResourceGroup>(cache, cacheKey);
                resourceGroup = redisDictionary[acquireLeaseRequest.ResourceGroup];
                if (resourceGroup == null)
                {
                    return Task.FromResult(new LeaseResponse()
                    {
                        Result = LeaseResult.NoLease,
                        Lease = new Lease()
                        {
                            ExpiryPeriod = TimeSpan.FromMinutes(1),
                            HeartbeatPeriod = TimeSpan.FromSeconds(25)
                        }
                    });
                }

                if (resourceGroup.LockedByClientId != renewLeaseRequest.ClientId && (DateTime.UtcNow - resourceGroup.LastCoordinatorRenewal).TotalSeconds <= resourceGroup.LeaseExpirySeconds)
                {
                    return Task.FromResult(new LeaseResponse()
                    {
                        Result = LeaseResult.Denied,
                        Lease = new Lease()
                        {
                            ExpiryPeriod = TimeSpan.FromSeconds(resourceGroup.LeaseExpirySeconds),
                            HeartbeatPeriod = TimeSpan.FromSeconds(resourceGroup.HeartbeatSeconds)
                        }
                    });
                }

                // obtain lock on the record blocking other nodes until the transaction is committed
                resourceGroup.LockedByClientId = acquireLeaseRequest.ClientId;
                resourceGroup.TimeNow = DateTime.UtcNow;
                redisDictionary[resourceGroup.Name] = resourceGroup;

                // determine the response, if the CoordinatorId is empty or expired then grant, else deny
                LeaseResponse response = new LeaseResponse
                {
                    Lease = new Lease()
                };
                if (resourceGroup.CoordinatorId == Guid.Empty || (resourceGroup.TimeNow - resourceGroup.LastCoordinatorRenewal).TotalSeconds > resourceGroup.LeaseExpirySeconds)
                {
                    response.Lease.ResourceGroup = acquireLeaseRequest.ResourceGroup;
                    response.Lease.ClientId = acquireLeaseRequest.ClientId;
                    response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(resourceGroup.LeaseExpirySeconds);
                    response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(resourceGroup.HeartbeatSeconds);
                    response.Lease.FencingToken = ++resourceGroup.FencingToken;
                    response.Result = LeaseResult.Granted;
                    resourceGroup.CoordinatorId = acquireLeaseRequest.ClientId;
                    resourceGroup.LastCoordinatorRenewal = DateTime.UtcNow;
                    resourceGroup.CoordinatorServer = Environment.MachineName;
                    resourceGroup.FencingToken = response.Lease.FencingToken;
                    redisDictionary[resourceGroup.Name] = resourceGroup;
                }
                else
                {
                    response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(resourceGroup.LeaseExpirySeconds);
                    response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(resourceGroup.HeartbeatSeconds);
                    response.Result = LeaseResult.Denied;
                }

                return Task.FromResult(response);
            }
            catch (Exception ex)
            {
                if (resourceGroup != null && redisDictionary != null)
                {
                    resourceGroup.CoordinatorId = Guid.Empty;
                    redisDictionary[resourceGroup.Name] = resourceGroup;
                }

                return Task.FromResult(new LeaseResponse()
                {
                    Result = TransientErrorDetector.IsTransient(ex) ? LeaseResult.TransientError : LeaseResult.Error,
                    Message = "Lease acquisition failure",
                    Exception = ex
                });
            }
        }

        public Task<LeaseResponse> TryRenewLeaseAsync(RenewLeaseRequest renewLeaseRequest)
        {
            ResourceGroup resourceGroup = null;
            RedisDictionary<string, ResourceGroup> redisDictionary = null;
            try
            {
                string cacheKey = $"{Constants.SCHEMA}:ResourceGroups";
                redisDictionary = new RedisDictionary<string, ResourceGroup>(cache, cacheKey);
                resourceGroup = redisDictionary[renewLeaseRequest.ResourceGroup];
                if (resourceGroup == null)
                {
                    return Task.FromResult(new LeaseResponse()
                    {
                        Result = LeaseResult.NoLease
                    });
                }

                if (resourceGroup.LockedByClientId != renewLeaseRequest.ClientId && (DateTime.UtcNow - resourceGroup.LastCoordinatorRenewal).TotalSeconds <= resourceGroup.LeaseExpirySeconds)
                {
                    return Task.FromResult(new LeaseResponse()
                    {
                        Result = LeaseResult.Denied
                    });
                }

                // obtain lock on the record blocking other nodes until the transaction is committed
                resourceGroup.LockedByClientId = renewLeaseRequest.ClientId;
                resourceGroup.TimeNow = DateTime.UtcNow;
                redisDictionary[resourceGroup.Name] = resourceGroup;

                // determine the response, if the CoordinatorId matches the current client id and the fencing token is the same then grant, else deny
                LeaseResponse response = new LeaseResponse
                {
                    Lease = new Lease()
                };
                if (!resourceGroup.CoordinatorId.Equals(renewLeaseRequest.ClientId) || resourceGroup.FencingToken > renewLeaseRequest.FencingToken)
                {
                    return Task.FromResult(new LeaseResponse()
                    {
                        Result = LeaseResult.Denied
                    });
                }

                if ((resourceGroup.TimeNow - resourceGroup.LastCoordinatorRenewal).TotalSeconds <= resourceGroup.LeaseExpirySeconds)
                {
                    response.Lease.ResourceGroup = renewLeaseRequest.ResourceGroup;
                    response.Lease.ClientId = renewLeaseRequest.ClientId;
                    response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(resourceGroup.LeaseExpirySeconds);
                    response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(resourceGroup.HeartbeatSeconds);
                    response.Lease.FencingToken = resourceGroup.FencingToken;
                    response.Result = LeaseResult.Granted;
                    resourceGroup.LastCoordinatorRenewal = DateTime.UtcNow;
                    redisDictionary[resourceGroup.Name] = resourceGroup;
                }
                else
                {
                    response.Result = LeaseResult.Denied;
                }

                return Task.FromResult(response);
            }
            catch (Exception ex)
            {
                if (resourceGroup != null && redisDictionary != null)
                {
                    resourceGroup.CoordinatorId = Guid.Empty;
                    redisDictionary[resourceGroup.Name] = resourceGroup;
                }

                return Task.FromResult(new LeaseResponse()
                {
                    Result = TransientErrorDetector.IsTransient(ex) ? LeaseResult.TransientError : LeaseResult.Error,
                    Message = "Lease acquisition failure",
                    Exception = ex
                });
            }
        }

        public Task RelinquishLeaseAsync(RelinquishLeaseRequest relinquishLeaseRequest)
        {
            string cacheKey = $"{Constants.SCHEMA}:ResourceGroups";
            RedisDictionary<string, ResourceGroup> redisDictionary = new RedisDictionary<string, ResourceGroup>(cache, cacheKey);
            ResourceGroup resourceGroup = redisDictionary.Values.FirstOrDefault(x => x.Name == relinquishLeaseRequest.ResourceGroup && x.CoordinatorId == relinquishLeaseRequest.ClientId && x.FencingToken == relinquishLeaseRequest.FencingToken);
            if (resourceGroup != null)
            {
                resourceGroup.CoordinatorId = Guid.Empty;
                redisDictionary[resourceGroup.Name] = resourceGroup;
            }

            return Task.CompletedTask;
        }
    }
}
