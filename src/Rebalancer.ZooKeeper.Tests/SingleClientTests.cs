using Rebalancer.Core;
using Rebalancer.ZooKeeper.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Rebalancer.ZooKeeper.Tests
{
    public class SingleClientTests : IDisposable
    {
        private readonly ZkHelper zkHelper;

        public SingleClientTests()
        {
            zkHelper = new ZkHelper();
        }

        [Fact]
        public async Task ResourceBarrier_GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            Providers.Register(GetResourceBarrierProvider);
            await GivenSingleClient_ThenGetsAllResourcesAssigned();
        }

        [Fact]
        public async Task GlobalBarrier_GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            Providers.Register(GetGlobalBarrierProvider);
            await GivenSingleClient_ThenGetsAllResourcesAssigned();
        }

        private async Task GivenSingleClient_ThenGetsAllResourcesAssigned()
        {
            // ARRANGE
            string groupName = Guid.NewGuid().ToString();
            await zkHelper.InitializeAsync("/rebalancer", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await zkHelper.PrepareResourceGroupAsync(groupName, "res", 5);

            List<string> expectedAssignedResources = new List<string>() { "res0", "res1", "res2", "res3", "res4" };

            // ACT
            (RebalancerClient client, List<TestEvent> testEvents) = CreateClient();
            await client.StartAsync(groupName, new ClientOptions() { AutoRecoveryOnError = false });

            await Task.Delay(TimeSpan.FromSeconds(10));

            // ASSERT
            Assert.Equal(1, testEvents.Count);
            Assert.Equal(EventType.Assignment, testEvents[0].EventType);
            Assert.True(ResourcesMatch(expectedAssignedResources, testEvents[0].Resources.ToList()));

            await client.StopAsync(TimeSpan.FromSeconds(30));
        }

        private (RebalancerClient, List<TestEvent> testEvents) CreateClient()
        {
            RebalancerClient client1 = new RebalancerClient();
            List<TestEvent> testEvents = new List<TestEvent>();
            client1.OnAssignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Assignment,
                    Resources = args.Resources
                });
            };

            client1.OnUnassignment += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Unassignment
                });
            };

            client1.OnAborted += (sender, args) =>
            {
                testEvents.Add(new TestEvent()
                {
                    EventType = EventType.Error
                });
                Console.WriteLine($"OnAborted: {args.Exception.ToString()}");
            };

            return (client1, testEvents);
        }

        private bool ResourcesMatch(List<string> expectedRes, List<string> actualRes)
        {
            return expectedRes.OrderBy(x => x).SequenceEqual(actualRes.OrderBy(x => x));
        }

        private IRebalancerProvider GetResourceBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts,
                "/rebalancer",
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(5),
                RebalancingMode.ResourceBarrier,
                new TestOutputLogger());
        }

        private IRebalancerProvider GetGlobalBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts,
                "/rebalancer",
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(5),
                RebalancingMode.GlobalBarrier,
                new TestOutputLogger());
        }

        public void Dispose()
        {
            if (zkHelper != null)
            {
                Task.Run(async () => await zkHelper.CloseAsync()).Wait();
            }
        }
    }
}