using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Rebalancer.Core;
using Rebalancer.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalanser.RabbitMq.ExampleWithSqlServerBackend
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            RunAsync().Wait();
        }

        private static List<ClientTask> clientTasks;

        private static async Task RunAsync()
        {
            Providers.Register(() => new SqlServerProvider("Server=(local);Database=RabbitMqScaling;Trusted_Connection=true;"));
            clientTasks = new List<ClientTask>();

            using (RebalancerClient context = new RebalancerClient())
            {
                context.OnAssignment += (sender, args) =>
                {
                    AssignedResources queues = context.GetAssignedResources();
                    foreach (string queue in queues.Resources)
                    {
                        StartConsuming(queue);
                    }
                };

                context.OnUnassignment += (sender, args) =>
                {
                    LogInfo("Consumer subscription cancelled");
                    StopAllConsumption();
                };

                context.OnAborted += (sender, args) =>
                {
                    LogInfo($"Error: {args.AbortReason}, Exception: {args.Exception.Message}");
                };

                await context.StartAsync("NotificationsGroup", new ClientOptions() { AutoRecoveryOnError = true, RestartDelay = TimeSpan.FromSeconds(30) });

                Console.WriteLine("Press enter to shutdown");
                while (!Console.KeyAvailable)
                {
                    Thread.Sleep(100);
                }

                StopAllConsumption();
                Task.WaitAll(clientTasks.Select(x => x.Client).ToArray());
            }
        }

        private static void StartConsuming(string queueName)
        {
            LogInfo("Subscription started for queue: " + queueName);
            CancellationTokenSource cts = new CancellationTokenSource();

            Task task = Task.Factory.StartNew(() =>
            {
                try
                {
                    ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
                    using (IConnection connection = factory.CreateConnection())
                    using (IModel channel = connection.CreateModel())
                    {
                        EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                        consumer.Received += (model, ea) =>
                        {
                            byte[] body = ea.Body.ToArray();
                            string message = Encoding.UTF8.GetString(body);
                            LogInfo($"{queueName} Received {message}");
                        };
                        channel.BasicConsume(queue: queueName,
                                             autoAck: true,
                                             consumer: consumer);

                        while (!cts.Token.IsCancellationRequested)
                        {
                            Thread.Sleep(100);
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError(ex.ToString());
                }

                if (cts.Token.IsCancellationRequested)
                {
                    LogInfo("Cancellation signal received for " + queueName);
                }
                else
                {
                    LogInfo("Consumer stopped for " + queueName);
                }
            }, TaskCreationOptions.LongRunning);

            clientTasks.Add(new ClientTask() { Cts = cts, Client = task });
        }

        private static void StopAllConsumption()
        {
            foreach (ClientTask ct in clientTasks)
            {
                ct.Cts.Cancel();
            }
        }

        private static void LogInfo(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: INFO  : {text}");
        }

        private static void LogError(string text)
        {
            Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR  : {text}");
        }
    }
}
