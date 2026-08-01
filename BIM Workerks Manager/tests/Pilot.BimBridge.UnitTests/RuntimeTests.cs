using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.UnitTests
{
    [TestFixture]
    public sealed class RuntimeTests
    {
        private string _directory;
        private MutableClock _clock;
        private BimBridgeRuntime _runtime;

        [SetUp]
        public void SetUp()
        {
            _directory = Path.Combine(
                Environment.GetEnvironmentVariable("BIM_BRIDGE_TEST_ROOT") ??
                @"C:\Users\roman\Desktop\BWM tests",
                "Unit",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
            _clock = new MutableClock(new DateTime(2026, 7, 31, 8, 0, 0, DateTimeKind.Utc));
            var options = TestOptions.Create(_directory);
            _runtime = new BimBridgeRuntime(options, _clock, new FileSystemBimResultStore(_directory), new SilentLog(), false);
        }

        [TearDown]
        public void TearDown()
        {
            _runtime.Dispose();
            if (Directory.Exists(_directory))
                Directory.Delete(_directory, true);
        }

        [Test]
        public void DuplicateClientRequestIsIdempotent()
        {
            var requestId = Guid.NewGuid();
            var first = _runtime.Submit(TaskRequest(requestId));
            var second = _runtime.Submit(TaskRequest(requestId));
            Assert.That(second.TaskId, Is.EqualTo(first.TaskId));
            Assert.That(_runtime.GetStatistics().SubmittedTasks, Is.EqualTo(1));
        }

        [Test]
        public async Task AssignmentHonorsCapabilityAndModel()
        {
            var unsuitable = Register("worker-a", new[] { "OTHER" }, new[] { "MODEL-A" });
            var suitable = Register("worker-b", new[] { "READ" }, new[] { "MODEL-A" });
            _runtime.Submit(TaskRequest(Guid.NewGuid()));

            var miss = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = unsuitable.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);
            var hit = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = suitable.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);

            Assert.That(miss.HasTask, Is.False);
            Assert.That(hit.HasTask, Is.True);
        }

        [Test]
        public async Task ExpiredLeaseIsRequeuedWithNewAssignment()
        {
            var firstWorker = Register("worker-a", new[] { "READ" }, new[] { "MODEL-A" });
            var accepted = _runtime.Submit(TaskRequest(Guid.NewGuid()));
            var first = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = firstWorker.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);

            _clock.Advance(TimeSpan.FromSeconds(31));
            _runtime.RunLeaseCheck();
            var secondWorker = Register("worker-b", new[] { "READ" }, new[] { "MODEL-A" });
            var second = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = secondWorker.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);

            Assert.That(second.HasTask, Is.True);
            Assert.That(second.Assignment.TaskId, Is.EqualTo(accepted.TaskId));
            Assert.That(second.Assignment.AssignmentId, Is.Not.EqualTo(first.Assignment.AssignmentId));
            Assert.That(second.Assignment.AttemptNumber, Is.EqualTo(2));
        }

        [Test]
        public async Task SecondLongPollForSameWorkerIsRejected()
        {
            var worker = Register("worker-a", new[] { "READ" }, new[] { "MODEL-A" });
            var first = _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = worker.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);
            var second = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto
            {
                RegistrationId = worker.RegistrationId,
                WaitTimeoutSeconds = 1
            }, CancellationToken.None);
            Assert.That(second.Error.Code, Is.EqualTo(BimBridgeErrorCodes.WorkerAlreadyWaiting));
            await first;
        }

        private RegisterWorkerResponseDto Register(string id, string[] capabilities, string[] models)
        {
            return _runtime.Register(new RegisterWorkerRequestDto
            {
                ProtocolVersion = BimBridgeProtocol.Version,
                WorkerInstanceId = id,
                Capabilities = capabilities,
                AvailableModelKeys = models,
                MaxParallelTasks = 1
            });
        }

        private static SubmitBimTaskRequestDto TaskRequest(Guid requestId)
        {
            return new SubmitBimTaskRequestDto
            {
                ClientRequestId = requestId,
                ProtocolVersion = BimBridgeProtocol.Version,
                OperationCode = "READ_PROPERTIES",
                RequiredCapabilities = new[] { "READ" },
                TargetModelKey = "MODEL-A",
                Payload = new byte[] { 1, 2, 3 },
                CanRetry = true,
                MaxAttempts = 2
            };
        }
    }

    internal static class TestOptions
    {
        public static BimBridgeOptions Create(string directory)
        {
            return new BimBridgeOptions
            {
                Workers = new WorkerOptions
                {
                    MaxRegisteredWorkers = 20,
                    MaxParallelTasksPerWorker = 1,
                    HeartbeatIntervalSeconds = 15,
                    WorkerOfflineAfterSeconds = 45,
                    LongPollingTimeoutSeconds = 2
                },
                Tasks = new TaskOptions
                {
                    QueueCapacity = 1000,
                    AssignmentLeaseSeconds = 60,
                    StartTaskTimeoutSeconds = 30,
                    DefaultMaxAttempts = 2,
                    DefaultTaskTtlMinutes = 60,
                    MaxPayloadBytes = 1024 * 1024,
                    RetryableOperationCodes = new[] { "READ_PROPERTIES" }
                },
                Results = new ResultOptions
                {
                    Directory = directory,
                    ChunkSizeBytes = 4,
                    MaxResultBytes = 1024,
                    MaxParallelUploads = 8,
                    CompletedResultTtlMinutes = 60,
                    IncompleteUploadTtlMinutes = 30
                },
                Maintenance = new MaintenanceOptions
                {
                    LeaseCheckIntervalSeconds = 10,
                    CleanupIntervalSeconds = 60,
                    FailedTaskTtlMinutes = 60,
                    CancelledTaskTtlMinutes = 30,
                    WorkerRegistrationTtlMinutes = 10
                }
            };
        }
    }

    internal sealed class SilentLog : IBimBridgeLog
    {
        public void Info(string message) { }
        public void Warning(string message) { }
        public void Error(string message, Exception exception) { }
    }
}
