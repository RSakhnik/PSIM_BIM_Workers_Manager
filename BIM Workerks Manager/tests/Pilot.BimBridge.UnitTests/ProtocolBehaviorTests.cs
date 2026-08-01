using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.UnitTests
{
    [TestFixture]
    public sealed class ProtocolBehaviorTests
    {
        private readonly List<string> _directories = new List<string>();

        [TearDown]
        public void TearDown()
        {
            foreach (var directory in _directories.Where(Directory.Exists))
                Directory.Delete(directory, true);
        }

        [Test]
        public void ReRegistrationInvalidatesOldRegistration()
        {
            using (var context = Create())
            {
                var oldRegistration = Register(context.Runtime, "same-instance");
                var currentRegistration = Register(context.Runtime, "same-instance");
                var oldHeartbeat = context.Runtime.Heartbeat(new WorkerHeartbeatRequestDto
                {
                    RegistrationId = oldRegistration.RegistrationId
                });
                Assert.That(currentRegistration.RegistrationId, Is.Not.EqualTo(oldRegistration.RegistrationId));
                Assert.That(oldHeartbeat.Error.Code, Is.EqualTo(BimBridgeErrorCodes.WorkerNotRegistered));
                Assert.That(oldHeartbeat.RegistrationIsValid, Is.False);
            }
        }

        [Test]
        public void UnsupportedProtocolAndWorkerLimitAreRejected()
        {
            using (var context = Create(options => options.Workers.MaxRegisteredWorkers = 1))
            {
                var badVersion = context.Runtime.Register(new RegisterWorkerRequestDto
                {
                    ProtocolVersion = "2.0",
                    WorkerInstanceId = "bad"
                });
                Register(context.Runtime, "first");
                var limited = Register(context.Runtime, "second");
                Assert.That(badVersion.Error.Code, Is.EqualTo(BimBridgeErrorCodes.UnsupportedProtocolVersion));
                Assert.That(limited.Error.Code, Is.EqualTo(BimBridgeErrorCodes.WorkerLimitReached));
            }
        }

        [Test]
        public async Task SubmitWakesCompatibleLongPoll()
        {
            using (var context = Create())
            {
                var worker = Register(context.Runtime, "worker");
                var wait = context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    WaitTimeoutSeconds = 2
                }, CancellationToken.None);
                await Task.Delay(50);
                var task = context.Runtime.Submit(NewTask());
                var response = await wait;
                Assert.That(response.HasTask, Is.True);
                Assert.That(response.Assignment.TaskId, Is.EqualTo(task.TaskId));
            }
        }

        [Test]
        public async Task TwentyWorkersCanWaitWithoutBlockingThreads()
        {
            using (var context = Create())
            {
                var waits = Enumerable.Range(0, 20)
                    .Select(index => Register(context.Runtime, "worker-" + index))
                    .Select(worker => context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                    {
                        RegistrationId = worker.RegistrationId,
                        WaitTimeoutSeconds = 1
                    }, CancellationToken.None))
                    .ToArray();
                var responses = await Task.WhenAll(waits);
                Assert.That(responses.All(response => !response.HasTask && response.Error == null), Is.True);
                Assert.That(context.Runtime.GetStatistics().ActiveLongPolls, Is.Zero);
            }
        }

        [Test]
        public async Task OnlyOneWorkerReceivesTask()
        {
            using (var context = Create())
            {
                var firstWorker = Register(context.Runtime, "first");
                var secondWorker = Register(context.Runtime, "second");
                var task = context.Runtime.Submit(NewTask());
                var first = await context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = firstWorker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                var second = await context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = secondWorker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                Assert.That(first.HasTask, Is.True);
                Assert.That(first.Assignment.TaskId, Is.EqualTo(task.TaskId));
                Assert.That(second.HasTask, Is.False);
            }
        }

        [Test]
        public async Task RunningCancellationIsReturnedByHeartbeatAndAcknowledged()
        {
            using (var context = Create())
            {
                var worker = Register(context.Runtime, "worker");
                var task = context.Runtime.Submit(NewTask());
                var assignment = await context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                context.Runtime.StartTask(new StartTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId
                });
                context.Runtime.Cancel(task.TaskId);
                var heartbeat = context.Runtime.Heartbeat(new WorkerHeartbeatRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    CurrentTaskId = task.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId
                });
                var acknowledged = context.Runtime.Fail(new FailTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId,
                    CancellationAcknowledged = true
                });
                Assert.That(heartbeat.CancellationRequested, Is.True);
                Assert.That(acknowledged.State, Is.EqualTo(BimTaskStates.Cancelled));
            }
        }

        [Test]
        public async Task LateAssignmentCannotStartAfterRequeue()
        {
            using (var context = Create())
            {
                var firstWorker = Register(context.Runtime, "first");
                var task = context.Runtime.Submit(NewTask());
                var oldAssignment = await context.Runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = firstWorker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                context.Clock.Advance(TimeSpan.FromSeconds(31));
                context.Runtime.RunLeaseCheck();
                var rejected = context.Runtime.StartTask(new StartTaskRequestDto
                {
                    RegistrationId = firstWorker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = oldAssignment.Assignment.AssignmentId
                });
                Assert.That(rejected.Error.Code, Is.EqualTo(BimBridgeErrorCodes.LeaseLost));
            }
        }

        [Test]
        public void QueueCapacityAndPayloadLimitAreEnforced()
        {
            using (var context = Create(options =>
            {
                options.Tasks.QueueCapacity = 1;
                options.Tasks.MaxPayloadBytes = 2;
            }))
            {
                context.Runtime.Submit(NewTask());
                var full = context.Runtime.Submit(NewTask());
                var largeRequest = NewTask();
                largeRequest.Payload = new byte[] { 1, 2, 3 };
                var large = context.Runtime.Submit(largeRequest);
                Assert.That(full.Error.Code, Is.EqualTo(BimBridgeErrorCodes.QueueFull));
                Assert.That(large.Error.Code, Is.EqualTo(BimBridgeErrorCodes.PayloadTooLarge));
            }
        }

        [Test]
        public void OneThousandConcurrentSubmissionsRemainConsistent()
        {
            using (var context = Create())
            {
                var responses = new BimTaskAcceptedDto[1000];
                Parallel.For(0, responses.Length, index =>
                {
                    var request = NewTask();
                    request.ClientRequestId = Guid.NewGuid();
                    responses[index] = context.Runtime.Submit(request);
                });
                Assert.That(responses.All(response => response.IsSuccess), Is.True);
                Assert.That(responses.Select(response => response.TaskId).Distinct().Count(), Is.EqualTo(1000));
                Assert.That(context.Runtime.GetStatistics().SubmittedTasks, Is.EqualTo(1000));
                Assert.That(context.Runtime.Submit(NewTask()).Error.Code, Is.EqualTo(BimBridgeErrorCodes.QueueFull));
            }
        }

        private TestContext Create(Action<BimBridgeOptions> configure = null)
        {
            var directory = Path.Combine(
                Environment.GetEnvironmentVariable("BIM_BRIDGE_TEST_ROOT") ?? @"C:\Users\roman\Desktop\BWM tests",
                "Unit",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            _directories.Add(directory);
            var options = TestOptions.Create(directory);
            configure?.Invoke(options);
            var clock = new MutableClock(new DateTime(2026, 7, 31, 8, 0, 0, DateTimeKind.Utc));
            return new TestContext(new BimBridgeRuntime(options, clock, new FileSystemBimResultStore(directory), new SilentLog(), false), clock);
        }

        private static RegisterWorkerResponseDto Register(BimBridgeRuntime runtime, string id)
        {
            return runtime.Register(new RegisterWorkerRequestDto
            {
                ProtocolVersion = BimBridgeProtocol.Version,
                WorkerInstanceId = id,
                Capabilities = new[] { "READ" },
                AvailableModelKeys = new[] { "MODEL-A" },
                MaxParallelTasks = 1
            });
        }

        private static SubmitBimTaskRequestDto NewTask()
        {
            return new SubmitBimTaskRequestDto
            {
                ClientRequestId = Guid.NewGuid(),
                ProtocolVersion = BimBridgeProtocol.Version,
                OperationCode = "READ_PROPERTIES",
                RequiredCapabilities = new[] { "READ" },
                TargetModelKey = "MODEL-A",
                Payload = new byte[] { 1 },
                CanRetry = true,
                MaxAttempts = 2
            };
        }

        private sealed class TestContext : IDisposable
        {
            public TestContext(BimBridgeRuntime runtime, MutableClock clock)
            {
                Runtime = runtime;
                Clock = clock;
            }
            public BimBridgeRuntime Runtime { get; }
            public MutableClock Clock { get; }
            public void Dispose() { Runtime.Dispose(); }
        }
    }
}
