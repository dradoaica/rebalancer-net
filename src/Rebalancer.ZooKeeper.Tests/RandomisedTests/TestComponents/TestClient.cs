using Rebalancer.Core;
using Rebalancer.Core.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Rebalancer.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class TestClient
    {
        public string Id { get; set; }
        public RebalancerClient Client { get; set; }
        public IList<string> Resources { get; set; }
        public bool Started { get; set; }
        public string ResourceGroup { get; set; }
        public ClientOptions ClientOptions { get; set; }
        public ResourceMonitor Monitor { get; set; }
        public static int ClientNumber;

        private TimeSpan onStartTime;
        private TimeSpan onStopTime;
        private readonly bool randomiseTimes;
        private readonly Random rand;

        public TestClient(ResourceMonitor resourceMonitor,
            string resourceGroup,
            ClientOptions clientOptions,
            TimeSpan onStartTime,
            TimeSpan onStopTime,
            bool randomiseTimes)
        {
            ResourceGroup = resourceGroup;
            ClientOptions = clientOptions;
            Monitor = resourceMonitor;
            Resources = new List<string>();

            this.onStartTime = onStartTime;
            this.onStopTime = onStopTime;
            this.randomiseTimes = randomiseTimes;
            rand = new Random(Guid.NewGuid().GetHashCode());
        }

        public async Task StartAsync(IRebalancerLogger logger)
        {
            CreateNewClient(logger);
            await Client.StartAsync(ResourceGroup, ClientOptions);
            Started = true;
        }

        public async Task StopAsync()
        {
            await Client.StopAsync(TimeSpan.FromSeconds(30));
            Started = false;
        }

        public async Task PerformActionAsync(IRebalancerLogger logger)
        {
            if (Started)
            {
                logger.Info("TEST RUNNER", "Stopping client");
                Monitor.RegisterRemoveClient(Id);
                await StopAsync();
                logger.Info("TEST RUNNER", "Stopped client");
            }
            else
            {
                logger.Info("TEST RUNNER", "Starting client");
                await StartAsync(logger);
                logger.Info("TEST RUNNER", "Started client");
            }
        }

        private void CreateNewClient(IRebalancerLogger logger)
        {
            Id = $"Client{ClientNumber}";
            ClientNumber++;
            Monitor.RegisterAddClient(Id);
            Client = new RebalancerClient();
            Client.OnAssignment += (sender, args) =>
            {
                Resources = args.Resources;
                foreach (string resource in args.Resources)
                {
                    Monitor.ClaimResource(resource, Id);
                }

                if (onStartTime > TimeSpan.Zero)
                {
                    if (randomiseTimes)
                    {
                        double waitTime = onStartTime.TotalMilliseconds * rand.NextDouble();
                        Thread.Sleep((int)waitTime);
                    }
                    else
                    {
                        Thread.Sleep(onStartTime);
                    }
                }
            };

            Client.OnUnassignment += (sender, args) =>
            {
                foreach (string resource in Resources)
                {
                    Monitor.ReleaseResource(resource, Id);
                }

                Resources.Clear();

                if (onStopTime > TimeSpan.Zero)
                {
                    if (randomiseTimes)
                    {
                        double waitTime = onStopTime.TotalMilliseconds * rand.NextDouble();
                        Thread.Sleep((int)waitTime);
                    }
                    else
                    {
                        Thread.Sleep(onStopTime);
                    }
                }
            };

            Client.OnAborted += (sender, args) =>
            {
                logger.Info("CLIENT", $"CLIENT ABORTED: {args.AbortReason}");
            };
        }
    }
}