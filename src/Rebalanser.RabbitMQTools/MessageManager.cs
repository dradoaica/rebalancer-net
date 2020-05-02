using RabbitMQ.Client;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Rebalanser.RabbitMQTools
{
    public class MessageManager
    {
        private static HttpClient Client;
        private static RabbitConnection RabbitConn;

        public static void Initialize(RabbitConnection rabbitConnection)
        {
            Client.BaseAddress = new Uri($"http://{rabbitConnection.Host}:{rabbitConnection.ManagementPort}/api/");
            Client = new HttpClient();
            byte[] byteArray = Encoding.ASCII.GetBytes($"{rabbitConnection.Username}:{rabbitConnection.Password}");
            Client.DefaultRequestHeaders.Authorization
                = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));

            RabbitConn = rabbitConnection;
        }

        public static async Task SendMessagesViaHttpAsync(string exchange, string routingKeyPrefix, int count, string vhost = "%2f")
        {
            if (vhost == "/")
            {
                vhost = "%2f";
            }

            for (int i = 0; i < count; i++)
            {
                string routingKey = routingKeyPrefix + i;
                StringContent content = new StringContent("{\"properties\":{},\"routing_key\":\"" + routingKey + "\",\"payload\":\"" + routingKey + "\",\"payload_encoding\":\"string\"}", Encoding.UTF8, "application/json");
                HttpResponseMessage response = await Client.PostAsync($"exchanges/{vhost}/{exchange}/publish", content);
            }
        }

        public static void SendMessagesViaClient(string exchange, string routingKeyPrefix, int count)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = RabbitConn.Host, Port = RabbitConn.Port, VirtualHost = RabbitConn.VirtualHost, UserName = RabbitConn.Username, Password = RabbitConn.Password };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    for (int i = 0; i < count; i++)
                    {
                        string routingKey = routingKeyPrefix + i;
                        string message = routingKeyPrefix + i;
                        byte[] body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: exchange,
                                             routingKey: routingKey,
                                             basicProperties: null,
                                             body: body);
                    }
                }
            }
        }


    }
}
