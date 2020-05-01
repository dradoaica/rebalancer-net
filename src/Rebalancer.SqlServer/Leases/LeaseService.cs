using Microsoft.Data.SqlClient;
using Rebalancer.Core.Logging;
using Rebalancer.SqlServer.Connections;
using System;
using System.Data;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Leases
{
    internal class LeaseService : ILeaseService
    {
        private readonly string connectionString;
        private readonly IRebalancerLogger logger;

        public LeaseService(string connectionString, IRebalancerLogger logger)
        {
            this.connectionString = connectionString;
            this.logger = logger;
        }

        public async Task<LeaseResponse> TryAcquireLeaseAsync(AcquireLeaseRequest acquireLeaseRequest)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                SqlCommand command = conn.CreateCommand();
                command.Transaction = transaction;

                try
                {
                    // obtain lock on the record blocking other nodes until the transaction is committed
                    command.CommandText = "UPDATE [RBR].[ResourceGroups] SET LockedByClient = @ClientId WHERE ResourceGroup = @ResourceGroup";
                    command.Parameters.AddWithValue("@ClientId", acquireLeaseRequest.ClientId);
                    command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = acquireLeaseRequest.ResourceGroup;
                    await command.ExecuteNonQueryAsync();

                    // get the resource group (TODO, use OUTPUT on UPDATE query instead of another query)
                    command.Parameters.Clear();
                    command.CommandText = @"SELECT [ResourceGroup]
      ,[CoordinatorId]
      ,[LastCoordinatorRenewal]
      ,[CoordinatorServer]
      ,[LockedByClient]
      ,[FencingToken]
      ,[LeaseExpirySeconds]
      ,[HeartbeatSeconds]
	  ,GETUTCDATE() AS [TimeNow]
FROM [RBR].[ResourceGroups]
WHERE ResourceGroup = @ResourceGroup";
                    command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = acquireLeaseRequest.ResourceGroup;

                    ResourceGroup rg = null;
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            rg = new ResourceGroup()
                            {
                                Name = acquireLeaseRequest.ResourceGroup,
                                CoordinatorId = GetGuidFromNullableGuid(reader, "CoordinatorId"),
                                CoordinatorServer = GetStringFromNullableGuid(reader, "CoordinatorServer"),
                                LastCoordinatorRenewal = GetDateTimeFromNullable(reader, "LastCoordinatorRenewal"),
                                TimeNow = (DateTime)reader["TimeNow"],
                                LockedByClientId = GetGuidFromNullableGuid(reader, "LockedByClient"),
                                FencingToken = (int)reader["FencingToken"],
                                LeaseExpirySeconds = (int)reader["LeaseExpirySeconds"],
                                HeartbeatSeconds = (int)reader["HeartbeatSeconds"]
                            };
                        }
                    }

                    if (rg == null)
                    {
                        return new LeaseResponse() { Result = LeaseResult.NoLease, Lease = new Lease() { ExpiryPeriod = TimeSpan.FromMinutes(1), HeartbeatPeriod = TimeSpan.FromSeconds(25) } };
                    }

                    // determine the response, if the CoordinatorId is empty or expired then grant, else deny
                    LeaseResponse response = new LeaseResponse
                    {
                        Lease = new Lease()
                    };
                    if (rg.CoordinatorId == Guid.Empty || (rg.TimeNow - rg.LastCoordinatorRenewal).TotalSeconds > rg.LeaseExpirySeconds)
                    {
                        response.Lease.ResourceGroup = acquireLeaseRequest.ResourceGroup;
                        response.Lease.ClientId = acquireLeaseRequest.ClientId;
                        response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(rg.LeaseExpirySeconds);
                        response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(rg.HeartbeatSeconds);
                        response.Lease.FencingToken = ++rg.FencingToken;
                        response.Result = LeaseResult.Granted;

                        command.Parameters.Clear();
                        command.CommandText = @"UPDATE [RBR].[ResourceGroups]
   SET [CoordinatorId] = @ClientId
      ,[LastCoordinatorRenewal] = GETUTCDATE()
      ,[CoordinatorServer] = @Server
      ,[FencingToken] = @FencingToken
 WHERE ResourceGroup = @ResourceGroup";
                        command.Parameters.AddWithValue("@ClientId", acquireLeaseRequest.ClientId);
                        command.Parameters.AddWithValue("@FencingToken", response.Lease.FencingToken);
                        command.Parameters.Add("@Server", SqlDbType.NVarChar, 500).Value = Environment.MachineName;
                        command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = acquireLeaseRequest.ResourceGroup;
                        await command.ExecuteNonQueryAsync();
                    }
                    else
                    {
                        response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(rg.LeaseExpirySeconds);
                        response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(rg.HeartbeatSeconds);
                        response.Result = LeaseResult.Denied;
                    }

                    transaction.Commit();

                    return response;
                }
                catch (Exception ex)
                {
                    try
                    {
                        logger.Error("Rolling back lease acquisition: ", ex);
                        transaction.Rollback();
                    }
                    catch (Exception rex)
                    {
                        logger.Error("Rollback of lease acquisition failed: ", rex);
                    }

                    return new LeaseResponse()
                    {
                        Result = TransientErrorDetector.IsTransient(ex) ? LeaseResult.TransientError : LeaseResult.Error,
                        Message = "Lease acquisition failure",
                        Exception = ex
                    };
                }
            }
        }

        public async Task<LeaseResponse> TryRenewLeaseAsync(RenewLeaseRequest renewLeaseRequest)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                SqlCommand command = conn.CreateCommand();
                command.Transaction = transaction;

                try
                {
                    // obtain lock on the record blocking other nodes until the transaction is committed
                    command.CommandText = "UPDATE [RBR].[ResourceGroups] SET LockedByClient = @ClientId WHERE ResourceGroup = @ResourceGroup";
                    command.Parameters.AddWithValue("@ClientId", renewLeaseRequest.ClientId);
                    command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = renewLeaseRequest.ResourceGroup;
                    await command.ExecuteNonQueryAsync();

                    // get the resource group
                    command.Parameters.Clear();
                    command.CommandText = @"SELECT [ResourceGroup]
      ,[CoordinatorId]
      ,[LastCoordinatorRenewal]
      ,[CoordinatorServer]
      ,[LockedByClient]
      ,[FencingToken]
      ,[LeaseExpirySeconds]
      ,[HeartbeatSeconds]
	  ,GETUTCDATE() AS [TimeNow]
FROM [RBR].[ResourceGroups]
WHERE ResourceGroup = @ResourceGroup";
                    command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = renewLeaseRequest.ResourceGroup;

                    ResourceGroup rg = null;
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            rg = new ResourceGroup()
                            {
                                Name = renewLeaseRequest.ResourceGroup,
                                CoordinatorId = GetGuidFromNullableGuid(reader, "CoordinatorId"),
                                CoordinatorServer = GetStringFromNullableGuid(reader, "CoordinatorServer"),
                                LastCoordinatorRenewal = GetDateTimeFromNullable(reader, "LastCoordinatorRenewal"),
                                TimeNow = (DateTime)reader["TimeNow"],
                                LockedByClientId = GetGuidFromNullableGuid(reader, "LockedByClient"),
                                FencingToken = (int)reader["FencingToken"],
                                LeaseExpirySeconds = (int)reader["LeaseExpirySeconds"],
                                HeartbeatSeconds = (int)reader["HeartbeatSeconds"]
                            };
                        }
                    }

                    if (rg == null)
                    {
                        return new LeaseResponse() { Result = LeaseResult.NoLease };
                    }

                    // determine the response, if the CoordinatorId matches the current client id and the fencing token is the same then grant, else deny
                    LeaseResponse response = new LeaseResponse
                    {
                        Lease = new Lease()
                    };
                    if (!rg.CoordinatorId.Equals(renewLeaseRequest.ClientId) || rg.FencingToken > renewLeaseRequest.FencingToken)
                    {
                        return new LeaseResponse() { Result = LeaseResult.Denied };
                    }

                    if ((rg.TimeNow - rg.LastCoordinatorRenewal).TotalSeconds <= rg.LeaseExpirySeconds)
                    {
                        response.Lease.ResourceGroup = renewLeaseRequest.ResourceGroup;
                        response.Lease.ClientId = renewLeaseRequest.ClientId;
                        response.Lease.ExpiryPeriod = TimeSpan.FromSeconds(rg.LeaseExpirySeconds);
                        response.Lease.HeartbeatPeriod = TimeSpan.FromSeconds(rg.HeartbeatSeconds);
                        response.Lease.FencingToken = rg.FencingToken;
                        response.Result = LeaseResult.Granted;

                        command.Parameters.Clear();
                        command.CommandText = @"UPDATE [RBR].[ResourceGroups]
   SET [LastCoordinatorRenewal] = GETUTCDATE()
 WHERE ResourceGroup = @ResourceGroup";
                        command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = renewLeaseRequest.ResourceGroup;
                        await command.ExecuteNonQueryAsync();
                    }
                    else
                    {
                        response.Result = LeaseResult.Denied;
                    }

                    transaction.Commit();

                    return response;
                }
                catch (Exception ex)
                {
                    try
                    {
                        logger.Error("Rolling back lease renewal: ", ex);
                        transaction.Rollback();
                    }
                    catch (Exception rex)
                    {
                        logger.Error("Rollback of lease renewal failed: ", rex);
                    }

                    return new LeaseResponse()
                    {
                        Result = TransientErrorDetector.IsTransient(ex) ? LeaseResult.TransientError : LeaseResult.Error,
                        Message = "Lease renew failure",
                        Exception = ex
                    };
                }
            }
        }

        public async Task RelinquishLeaseAsync(RelinquishLeaseRequest relinquishLeaseRequest)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"UPDATE [RBR].[ResourceGroups]
   SET [CoordinatorId] = NULL
 WHERE ResourceGroup = @ResourceGroup
AND [CoordinatorId] = @ClientId
AND [FencingToken] = @FencingToken";
                command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = relinquishLeaseRequest.ResourceGroup;
                command.Parameters.AddWithValue("@ClientId", relinquishLeaseRequest.ClientId);
                command.Parameters.AddWithValue("@FencingToken", relinquishLeaseRequest.FencingToken);
                await command.ExecuteNonQueryAsync();
            }
        }

        private DateTime GetDateTimeFromNullable(SqlDataReader reader, string key)
        {
            object data = reader[key];
            if (data == DBNull.Value)
            {
                return new DateTime(1980, 1, 1);
            }
            return (DateTime)reader[key];
        }

        private Guid GetGuidFromNullableGuid(SqlDataReader reader, string key)
        {
            object data = reader[key];
            if (data == DBNull.Value)
            {
                return Guid.Empty;
            }
            return (Guid)reader[key];
        }

        private string GetStringFromNullableGuid(SqlDataReader reader, string key)
        {
            object data = reader[key];
            if (data == DBNull.Value)
            {
                return string.Empty;
            }
            return reader[key].ToString();
        }
    }
}
