using Rebalancer.Core;
using Rebalancer.ZooKeeper.Tests.Helpers;
using Rebalancer.ZooKeeper.Tests.RandomisedTests.TestComponents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Rebalancer.ZooKeeper.Tests.RandomisedTests
{

    public class RandomisedResourceBarrierTests : IDisposable
    {
        private readonly ZkHelper zkHelper;
        private RandomConfig currentConfig;

        public RandomisedResourceBarrierTests()
        {
            zkHelper = new ZkHelper();
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_LongInterval_LowClient_LowResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 3,
                ResourceCount = 6,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_LowClient_LowResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 3,
                ResourceCount = 6,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_LongInterval_MediumClient_LowResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 5,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_MediumClient_LowResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 5,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_LongInterval_MediumClient_MediumResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_MediumClient_MediumResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(60),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedReleaseBarrierTest_LongInterval_LargeClient_LargeResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 30,
                ResourceCount = 200,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromMinutes(5),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck,
                StartUpClientInterval = TimeSpan.FromSeconds(2),
                MinimumRebalancingInterval = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromMinutes(1),
                SessionTimeout = TimeSpan.FromMinutes(1)
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_LargeClient_LargeResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 30,
                ResourceCount = 200,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromMinutes(5),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck,
                StartUpClientInterval = TimeSpan.FromSeconds(2),
                MinimumRebalancingInterval = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromMinutes(1),
                SessionTimeout = TimeSpan.FromMinutes(1)
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_VeryLargeClient_VeryLargeResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 100,
                ResourceCount = 1000,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromMinutes(5),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck,
                StartUpClientInterval = TimeSpan.FromSeconds(2),
                MinimumRebalancingInterval = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromMinutes(1),
                SessionTimeout = TimeSpan.FromMinutes(1)
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_LongInterval_SmallClient_LargeResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 200,
                TestDuration = TimeSpan.FromMinutes(480),
                MaxInterval = TimeSpan.FromMinutes(5),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck,
                StartUpClientInterval = TimeSpan.FromSeconds(2),
                MinimumRebalancingInterval = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromMinutes(1),
                SessionTimeout = TimeSpan.FromMinutes(1)
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_LongInterval_SmallClient_LargeResource_FullCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 200,
                TestDuration = TimeSpan.FromMinutes(480),
                MaxInterval = TimeSpan.FromMinutes(5),
                RandomiseInterval = false,
                CheckType = CheckType.FullCheck,
                StartUpClientInterval = TimeSpan.FromSeconds(2),
                MinimumRebalancingInterval = TimeSpan.FromMinutes(2),
                ConnectTimeout = TimeSpan.FromMinutes(1),
                SessionTimeout = TimeSpan.FromMinutes(1)
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_ShortRandomInterval_MediumClient_MediumResource_ConditionalCheck()
        {
            // the wait period is set quite high as running the test long enough
            // can induce a scenario similar to:
            // a very short interval causes an in progress rebalancing to cancel
            // once cancelled the new rebalancing commences, then during this new rebalancing,
            // a session expired event happens towards the end causing the cancellation of the current
            // rebalancing, this rebalancing #3 is still in progress 60 seconds after the
            // the sub second wait period, causing the "all resources assigned" check to fail

            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(10),
                RandomiseInterval = true,
                CheckType = CheckType.ConditionalCheck,
                ConditionalCheckInterval = 5,
                ConditionalCheckWaitPeriod = TimeSpan.FromMinutes(2)
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_ShortRandomInterval_MediumClient_MediumResource_ConditionalCheck()
        {
            // the wait period is set quite high as running the test long enough
            // can induce a scenario similar to:
            // a very short interval causes an in progress rebalancing to cancel
            // once cancelled the new rebalancing commences, then during this new rebalancing,
            // a session expired event happens towards the end causing the cancellation of the current
            // rebalancing, this rebalancing #3 is still in progress 60 seconds after the
            // the sub second wait period, causing the "all resources assigned" check to fail

            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(10),
                RandomiseInterval = true,
                CheckType = CheckType.ConditionalCheck,
                ConditionalCheckInterval = 5,
                ConditionalCheckWaitPeriod = TimeSpan.FromMinutes(2)
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_ShortRandomInterval_MediumClient_MediumResource_OnAssignmentDelay_ConditionalCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(10),
                RandomiseInterval = true,
                CheckType = CheckType.ConditionalCheck,
                ConditionalCheckInterval = 5,
                ConditionalCheckWaitPeriod = TimeSpan.FromMinutes(2),
                OnAssignmentDelay = TimeSpan.FromSeconds(30)
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task GlobalBarrier_RandomisedTest_ShortRandomInterval_MediumClient_MediumResource_OnAssignmentDelay_ConditionalCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 10,
                ResourceCount = 60,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(10),
                RandomiseInterval = true,
                CheckType = CheckType.ConditionalCheck,
                ConditionalCheckInterval = 5,
                ConditionalCheckWaitPeriod = TimeSpan.FromMinutes(2),
                OnAssignmentDelay = TimeSpan.FromSeconds(30)
            };

            currentConfig = config;
            Providers.Register(GetGlobalBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_ShortInterval_LowClient_LowResource_DoubleAssignmentCheck()
        {
            RandomConfig config = new RandomConfig()
            {
                ClientCount = 3,
                ResourceCount = 6,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(5),
                RandomiseInterval = false,
                CheckType = CheckType.DoubleAssignmentCheck
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        [Fact]
        public async Task ResourceBarrier_RandomisedTest_ShortInterval_LowClient_LowResource_EventHandlerWait_DoubleAssignmentCheck()
        {
            // Thread.Sleep is used which reduces concurrency so this test is of
            // limited utility. But it does put more stress on the logic.

            RandomConfig config = new RandomConfig()
            {
                ClientCount = 3,
                ResourceCount = 6,
                TestDuration = TimeSpan.FromMinutes(120),
                MaxInterval = TimeSpan.FromSeconds(5),
                MinimumRebalancingInterval = TimeSpan.FromSeconds(4),
                RandomiseInterval = false,
                CheckType = CheckType.DoubleAssignmentCheck,
                OnStartEventHandlerTime = TimeSpan.FromSeconds(10),
                OnStopEventHandlerTime = TimeSpan.FromSeconds(10),
                RandomiseEventHandlerTimes = true
            };

            currentConfig = config;
            Providers.Register(GetResourceBarrierProvider);

            await RandomisedTest(config);
        }

        private async Task RandomisedTest(RandomConfig config)
        {
            // ARRANGE
            TestOutputLogger testLogger = new TestOutputLogger();
            string groupName = Guid.NewGuid().ToString();
            await zkHelper.InitializeAsync("/rebalancer", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
            await zkHelper.PrepareResourceGroupAsync(groupName, "res", config.ResourceCount);

            ResourceMonitor resourceMonitor = new ResourceMonitor();
            List<int> resSuffixes = new List<int>();
            for (int i = 0; i < config.ResourceCount; i++)
            {
                resourceMonitor.CreateResource($"res{i}");
                resSuffixes.Add(i);
            }

            ClientOptions clientOptions = new ClientOptions()
            {
                AutoRecoveryOnError = true,
                RestartDelay = TimeSpan.FromSeconds(10),
                OnAssignmentDelay = config.OnAssignmentDelay
            };

            List<TestClient> clients = new List<TestClient>();
            for (int i = 0; i < config.ClientCount; i++)
            {
                clients.Add(new TestClient(resourceMonitor,
                    groupName,
                    clientOptions,
                    config.OnStartEventHandlerTime,
                    config.OnStopEventHandlerTime,
                    config.RandomiseEventHandlerTimes));
            }

            for (int i = 0; i < config.ClientCount; i++)
            {
                if (config.StartUpClientInterval.TotalMilliseconds > 0)
                {
                    await Task.Delay(config.StartUpClientInterval);
                }

                await clients[i].StartAsync(testLogger);
            }

            await Task.Delay(TimeSpan.FromSeconds(30));

            // ACT
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Random rand = new Random(Guid.NewGuid().GetHashCode());
            int testCounter = 0;
            while (sw.Elapsed < config.TestDuration)
            {
                testLogger.Info("TEST RUNNER", "Test " + testCounter);

                // action == 0 -> add/remove a client
                // action == 1 -> add/remove a resource
                int action = rand.Next(2);
                if (action == 0)
                {
                    int clientIndex = rand.Next(config.ClientCount);
                    await clients[clientIndex].PerformActionAsync(testLogger);
                }
                else
                {
                    // resAction == 0 && resources exist -> remove a resource
                    // else add a resource
                    int resAction = rand.Next(2);
                    if (resAction == 0 || !resSuffixes.Any())
                    {
                        int resSuffix = resSuffixes.Any() ? resSuffixes.Max() + 1 : 0;
                        resSuffixes.Add(resSuffix);
                        resourceMonitor.AddResource($"res{resSuffix}");
                        testLogger.Info("TEST RUNNER", "Adding a resource");
                        await zkHelper.AddResourceAsync(groupName, $"res{resSuffix}");
                        testLogger.Info("TEST RUNNER", "Added a resource");
                    }
                    else
                    {
                        int index = rand.Next(resSuffixes.Count);
                        int resSuffix = resSuffixes[index];
                        resSuffixes.RemoveAt(index);
                        resourceMonitor.RemoveResource($"res{resSuffix}");
                        testLogger.Info("TEST RUNNER", "Removing a resource");
                        await zkHelper.DeleteResourceAsync(groupName, $"res{resSuffix}");
                        testLogger.Info("TEST RUNNER", "Removed a resource");
                    }
                }

                // wait for the configured period of time before making asserts
                // this gives the necessary time for rebalancing
                TimeSpan currentTestInterval;
                if (config.RandomiseInterval)
                {
                    currentTestInterval = TimeSpan.FromMilliseconds(rand.Next((int)config.MaxInterval.TotalMilliseconds));
                }
                else
                {
                    currentTestInterval = config.MaxInterval;
                }

                await Task.Delay(currentTestInterval);

                resourceMonitor.PrintEvents($"/home/jack/tmp/rebalancer-zk/test-{groupName}");

                // check for double assignments. All test scenarios must check this. No matter
                // what happens, we can never allow double assignments - ever
                if (resourceMonitor.DoubleAssignmentsExist())
                {
                    foreach (object violation in resourceMonitor.GetDoubleAssignments())
                    {
                        testLogger.Error("TEST RUNNER", violation.ToString());
                    }
                }
                Assert.False(resourceMonitor.DoubleAssignmentsExist());

                // depending on the type of test, we'll ensure that all resources have been assigned
                // some tests that have extremely short durations between events do not leave enough time
                // for rebalancing and so do not perform this check. The conditional check will
                // only perform this check when a long enough time period has been allowed for rebalancing to complete
                if (config.CheckType == CheckType.FullCheck)
                {
                    if (clients.Any(x => x.Started))
                    {
                        testLogger.Info("TEST RUNNER", "Perform all resources assigned check");
                        Assert.True(resourceMonitor.AllResourcesAssigned());
                    }
                }
                else if (config.CheckType == CheckType.ConditionalCheck)
                {
                    if (testCounter % config.ConditionalCheckInterval == 0 && clients.Any(x => x.Started))
                    {
                        testLogger.Info("TEST RUNNER", "Grace period before all resources assigned check");
                        await Task.Delay(config.ConditionalCheckWaitPeriod);
                        testLogger.Info("TEST RUNNER", "Perform all resources assigned check");
                        Assert.True(resourceMonitor.AllResourcesAssigned());
                    }
                }

                testCounter++;
            }

            // clean up
            for (int i = 0; i < config.ClientCount; i++)
            {
                await clients[i].StopAsync();
            }
        }

        private IRebalancerProvider GetResourceBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts,
                "/rebalancer",
                currentConfig.SessionTimeout,
                currentConfig.ConnectTimeout,
                currentConfig.MinimumRebalancingInterval,
                RebalancingMode.ResourceBarrier,
                new TestOutputLogger());
        }

        private IRebalancerProvider GetGlobalBarrierProvider()
        {
            return new ZooKeeperProvider(ZkHelper.ZooKeeperHosts,
                "/rebalancer",
                currentConfig.SessionTimeout,
                currentConfig.ConnectTimeout,
                currentConfig.MinimumRebalancingInterval,
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