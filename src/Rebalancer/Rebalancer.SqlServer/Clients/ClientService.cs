using Microsoft.Data.SqlClient;
using Rebalancer.Core;
using Rebalancer.SqlServer.Connections;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Clients
{
    internal class ClientService : IClientService
    {
        private readonly string connectionString;

        public ClientService(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public async Task CreateClientAsync(string resourceGroup, Guid clientId)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"INSERT INTO [RBR].[Clients]
           ([ClientId]
           ,[ResourceGroup]
           ,[LastKeepAlive]
           ,[ClientStatus]
           ,[CoordinatorStatus]
           ,[Resources]
           ,[FencingToken])
     VALUES
           (@ClientId
           ,@ResourceGroup
           ,GETUTCDATE()
           ,@ClientStatus
           ,@CoordinatorStatus
           ,''
           ,1)";
                command.Parameters.Add("@ClientId", SqlDbType.UniqueIdentifier).Value = clientId;
                command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                command.Parameters.Add("@ClientStatus", SqlDbType.TinyInt).Value = ClientStatus.Waiting;
                command.Parameters.Add("@CoordinatorStatus", SqlDbType.TinyInt).Value = CoordinatorStatus.StopActivity;
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<List<Client>> GetActiveClientsAsync(string resourceGroup)
        {
            List<Client> clients = new List<Client>();
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"SELECT [ClientId]
      ,[LastKeepAlive]
      ,[ClientStatus]
      ,[CoordinatorStatus]
      ,GETUTCDATE() AS [TimeNow]
FROM [RBR].[Clients] 
WHERE ResourceGroup = @ResourceGroup
AND (ClientStatus = 0 OR ClientStatus = 1)";
                command.Parameters.Add("@ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        Client client = new Client
                        {
                            ClientStatus = (ClientStatus)(byte)reader["ClientStatus"],
                            CoordinatorStatus = (CoordinatorStatus)(byte)reader["CoordinatorStatus"],
                            ClientId = (Guid)reader["ClientId"],
                            LastKeepAlive = (DateTime)reader["LastKeepAlive"],
                            TimeNow = (DateTime)reader["TimeNow"]
                        };
                        clients.Add(client);
                    }
                }
            }

            return clients;
        }

        public async Task<Client> KeepAliveAsync(Guid clientId)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"UPDATE [RBR].[Clients]
   SET [LastKeepAlive] = GETUTCDATE()
   OUTPUT inserted.*
WHERE ClientId = @ClientId";
                command.Parameters.Add("@ClientId", SqlDbType.UniqueIdentifier).Value = clientId;
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        Client client = new Client
                        {
                            ClientId = clientId,
                            ClientStatus = (ClientStatus)(byte)reader["ClientStatus"],
                            CoordinatorStatus = (CoordinatorStatus)(byte)reader["CoordinatorStatus"],
                            LastKeepAlive = (DateTime)reader["LastKeepAlive"],
                            AssignedResources = reader["Resources"].ToString().Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList()
                        };

                        return client;
                    }
                }
            }

            throw new RebalancerException($"No client exists with id {clientId}");
        }

        public async Task SetClientStatusAsync(Guid clientId, ClientStatus clientStatus)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"UPDATE [RBR].[Clients]
   SET [ClientStatus] = @ClientStatus
WHERE ClientId = @ClientId";

                command.Parameters.Add("@ClientStatus", SqlDbType.TinyInt).Value = clientStatus;
                command.Parameters.Add("@ClientId", SqlDbType.UniqueIdentifier).Value = clientId;
                await command.ExecuteNonQueryAsync();
            }
        }

        public async Task<ModifyClientResult> StartActivityAsync(int fencingToken, List<ClientStartRequest> clientStartRequests)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = @"UPDATE [RBR].[Clients]
   SET [CoordinatorStatus] = @CoordinatorStatus
      ,[Resources] = @Resources
      ,[FencingToken] = @FencingToken
WHERE ClientId = @ClientId
AND FencingToken <= @FencingToken
SELECT @@ROWCOUNT";

                foreach (ClientStartRequest request in clientStartRequests)
                {
                    command.Parameters.Clear();
                    command.Parameters.Add("@CoordinatorStatus", SqlDbType.TinyInt).Value = CoordinatorStatus.ResourcesGranted;
                    command.Parameters.Add("@FencingToken", SqlDbType.Int).Value = fencingToken;
                    command.Parameters.Add("@ClientId", SqlDbType.UniqueIdentifier).Value = request.ClientId;
                    command.Parameters.Add("@Resources", SqlDbType.VarChar, -1).Value = string.Join(",", request.AssignedResources);
                    int result = (int)await command.ExecuteScalarAsync();
                    if (result == 0)
                    {
                        return ModifyClientResult.FencingTokenViolation;
                    }
                }

                return ModifyClientResult.Ok;
            }
        }

        public async Task<ModifyClientResult> StopActivityAsync(int fencingToken, List<Client> clients)
        {
            using (SqlConnection conn = await ConnectionHelper.GetOpenConnectionAsync(connectionString))
            {
                SqlCommand command = conn.CreateCommand();
                command.CommandText = GetSetStatusQuery(clients);
                command.Parameters.Add("@CoordinatorStatus", SqlDbType.TinyInt).Value = CoordinatorStatus.StopActivity;
                command.Parameters.Add("@FencingToken", SqlDbType.Int).Value = fencingToken;

                for (int i = 0; i < clients.Count; i++)
                {
                    command.Parameters.Add($"@Client{i}", SqlDbType.UniqueIdentifier).Value = clients[i].ClientId;
                }

                int rowsUpdated = (int)await command.ExecuteScalarAsync();

                if (rowsUpdated != clients.Count)
                {
                    return ModifyClientResult.FencingTokenViolation;
                }

                return ModifyClientResult.Ok;
            }
        }

        private string GetSetStatusQuery(List<Client> clients)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(@"UPDATE [RBR].[Clients]
   SET [CoordinatorStatus] = @CoordinatorStatus
      ,[Resources] = ''
      ,[FencingToken] = @FencingToken
WHERE ClientId IN (");

            for (int i = 0; i < clients.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(",");
                }

                sb.Append($"@Client{i}");
            }

            sb.Append(@")
AND FencingToken <= @FencingToken
SELECT @@ROWCOUNT");

            return sb.ToString();
        }
    }
}
