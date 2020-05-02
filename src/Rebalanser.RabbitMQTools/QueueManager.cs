using Microsoft.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Rebalanser.RabbitMQTools
{
    public class QueueManager
    {
        private static string ConnStr;
        private static HttpClient Client;

        public static void Initialize(string connectionString, RabbitConnection rabbitConnection)
        {
            ConnStr = connectionString;

            Client = new HttpClient
            {
                BaseAddress = new Uri($"http://{rabbitConnection.Host}:{rabbitConnection.ManagementPort}/api/")
            };
            byte[] byteArray = Encoding.ASCII.GetBytes($"{rabbitConnection.Username}:{rabbitConnection.Password}");
            Client.DefaultRequestHeaders.Authorization
                = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
        }

        public static void EnsureResourceGroup(string resourceGroup, int leaseExpirySeconds)
        {
            bool rgExists = false;

            using (SqlConnection conn = new SqlConnection(ConnStr))
            {
                conn.Open();
                SqlCommand command = conn.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM RBR.ResourceGroups WHERE ResourceGroup = @ResourceGroup";
                command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                int count = (int)command.ExecuteScalar();
                rgExists = count == 1;

                if (!rgExists)
                {
                    command.Parameters.Clear();
                    command.CommandText = $"INSERT INTO RBR.ResourceGroups(ResourceGroup, FencingToken, LeaseExpirySeconds) VALUES(@ResourceGroup, 1, @LeaseExpirySeconds)";
                    command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                    command.Parameters.Add("LeaseExpirySeconds", SqlDbType.Int).Value = leaseExpirySeconds;
                    command.ExecuteNonQuery();
                    Console.WriteLine("Created consumer group");
                }
                else
                {
                    command.Parameters.Clear();
                    command.CommandText = $"UPDATE RBR.ResourceGroups SET LeaseExpirySeconds = @LeaseExpirySeconds WHERE ResourceGroup = @ResourceGroup";
                    command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                    command.Parameters.Add("LeaseExpirySeconds", SqlDbType.Int).Value = leaseExpirySeconds;
                    command.ExecuteNonQuery();
                    Console.WriteLine("Consumer group exists");
                }
            }
        }

        public static async Task ReconcileQueuesSqlAsync(string resourceGroup, string queuePrefix)
        {
            List<string> rabbitQueues = await GetQueuesFromRabbitMQAsync(queuePrefix);
            Console.WriteLine($"{rabbitQueues.Count} matching queues in RabbitMQ");
            List<string> sqlQueues = GetQueuesFromSqlServer(resourceGroup);
            Console.WriteLine($"{sqlQueues.Count} matching registered queues in SQL Server backend");

            foreach (string queue in rabbitQueues)
            {
                if (!sqlQueues.Any(x => x == queue))
                {
                    InsertQueueSql(resourceGroup, queue);
                    Console.WriteLine("Added queue to backend");
                }
            }

            foreach (string queue in sqlQueues)
            {
                if (!rabbitQueues.Any(x => x == queue))
                {
                    DeleteQueueSql(resourceGroup, queue);
                    Console.WriteLine("Removed queue from backend");
                }
            }
        }

        public static async Task AddQueueSqlAsync(string resourceGroup, string exchangeName, string queuePrefix)
        {
            string lastQueue = await GetMaxQueueAsync(queuePrefix);
            if (lastQueue == null)
            {
                string queueName = queuePrefix + "_0001";
                await PutQueueRabbitMQAsync(exchangeName, queueName);
                InsertQueueSql(resourceGroup, queueName);
                Console.WriteLine($"Queue {queueName} added to consumer group {resourceGroup}");
            }
            else
            {
                string qNumber = lastQueue.Substring(lastQueue.IndexOf("_") + 1);
                int nextNumber = (int.Parse(qNumber)) + 1;
                string queueName = queuePrefix + "_" + nextNumber.ToString().PadLeft(4, '0');
                await PutQueueRabbitMQAsync(exchangeName, queueName);
                InsertQueueSql(resourceGroup, queueName);
                Console.WriteLine($"Queue {queueName} added to consumer group {resourceGroup}");
            }
        }

        public static async Task RemoveQueueSqlAsync(string resourceGroup, string queuePrefix)
        {
            string lastQueue = await GetMaxQueueAsync(queuePrefix);
            if (lastQueue != null)
            {
                await DeleteQueueRabbitMQAsync(lastQueue);
                DeleteQueueSql(resourceGroup, lastQueue);
                Console.WriteLine($"Queue {lastQueue} removed from consumer group {resourceGroup}");
            }
        }

        public static async Task<List<string>> GetQueuesAsync(string queuePrefix)
        {
            return await GetQueuesFromRabbitMQAsync(queuePrefix);
        }

        private static async Task<string> GetMaxQueueAsync(string queuePrefix)
        {
            List<string> queuesRabbit = await GetQueuesFromRabbitMQAsync(queuePrefix);

            return queuesRabbit.OrderBy(x => x).LastOrDefault();
        }

        private static async Task<List<string>> GetQueuesFromRabbitMQAsync(string queuePrefix)
        {
            HttpResponseMessage response = await Client.GetAsync("queues");
            string json = await response.Content.ReadAsStringAsync();
            JArray queues = JArray.Parse(json);
            List<string> queueNames = queues.Select(x => x["name"].Value<string>()).Where(x => x.StartsWith(queuePrefix)).ToList();

            return queueNames;
        }

        private static List<string> GetQueuesFromSqlServer(string resourceGroup)
        {
            List<string> queues = new List<string>();

            using (SqlConnection conn = new SqlConnection(ConnStr))
            {
                conn.Open();
                SqlCommand command = conn.CreateCommand();
                command.CommandText = "SELECT ResourceName FROM RBR.Resources WHERE ResourceGroup = @ResourceGroup";
                command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        queues.Add(reader.GetString(0));
                    }
                }
            }

            return queues;
        }

        private static async Task PutQueueRabbitMQAsync(string exchange, string queueName, string vhost = "%2f")
        {
            if (vhost == "/")
            {
                vhost = "%2f";
            }

            StringContent createExchangeContent = new StringContent("{\"type\":\"x-consistent-hash\",\"auto_delete\":false,\"durable\":true,\"internal\":false,\"arguments\":{}}", Encoding.UTF8, "application/json");
            HttpResponseMessage createExchangeResponse = await Client.PutAsync($"exchanges/{vhost}/{exchange}", createExchangeContent);
            if (!createExchangeResponse.IsSuccessStatusCode)
            {
                throw new Exception("Failed to create exchange");
            }

            StringContent createQueueContent = new StringContent("{ \"durable\":true}", Encoding.UTF8, "application/json");
            HttpResponseMessage createQueueResponse = await Client.PutAsync($"queues/{vhost}/{queueName}", createQueueContent);
            if (!createQueueResponse.IsSuccessStatusCode)
            {
                throw new Exception("Failed to create queue");
            }

            StringContent createBindingsContent = new StringContent("{\"routing_key\":\"10\",\"arguments\":{}}", Encoding.UTF8, "application/json");
            HttpResponseMessage createBindingsResponse = await Client.PostAsync($"bindings/{vhost}/e/{exchange}/q/{queueName}", createBindingsContent);
            if (!createBindingsResponse.IsSuccessStatusCode)
            {
                throw new Exception("Failed to create exchange to queue bindings");
            }
        }

        private static void InsertQueueSql(string resourceGroup, string queueName)
        {
            using (SqlConnection conn = new SqlConnection(ConnStr))
            {
                conn.Open();
                SqlCommand command = conn.CreateCommand();
                command.CommandText = $"INSERT INTO RBR.Resources(ResourceGroup, ResourceName) VALUES(@ResourceGroup, @ResourceName)";
                command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                command.Parameters.Add("ResourceName", SqlDbType.VarChar, 1000).Value = queueName;
                command.ExecuteNonQuery();
            }
        }

        private static async Task DeleteQueueRabbitMQAsync(string queueName, string vhost = "%2f")
        {
            if (vhost == "/")
            {
                vhost = "%2f";
            }

            HttpResponseMessage response = await Client.DeleteAsync($"queues/{vhost}/{queueName}");
        }

        private static void DeleteQueueSql(string resourceGroup, string queueName)
        {
            using (SqlConnection conn = new SqlConnection(ConnStr))
            {
                conn.Open();
                SqlCommand command = conn.CreateCommand();
                command.CommandText = $"DELETE FROM RBR.Resources WHERE ResourceGroup = @ResourceGroup AND ResourceName = @ResourceName";
                command.Parameters.Add("ResourceGroup", SqlDbType.VarChar, 100).Value = resourceGroup;
                command.Parameters.Add("ResourceName", SqlDbType.VarChar, 1000).Value = queueName;
                command.ExecuteNonQuery();
            }
        }
    }
}
