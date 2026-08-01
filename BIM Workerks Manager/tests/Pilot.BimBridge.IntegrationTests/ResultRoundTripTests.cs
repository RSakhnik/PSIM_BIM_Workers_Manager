using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.IntegrationTests
{
    [TestFixture]
    public sealed class ResultRoundTripTests
    {
        [Test]
        public async Task MultiChunkResultCanBeCompletedAndRead()
        {
            var directory = Path.Combine(
                Environment.GetEnvironmentVariable("BIM_BRIDGE_TEST_ROOT") ??
                @"C:\Users\roman\Desktop\BWM tests",
                "Integration",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = CreateOptions(directory);
            var clock = new MutableClock(DateTime.UtcNow);
            using (var runtime = new BimBridgeRuntime(options, clock, new FileSystemBimResultStore(directory), new SilentIntegrationLog(), false))
            {
                var worker = runtime.Register(new RegisterWorkerRequestDto
                {
                    ProtocolVersion = BimBridgeProtocol.Version,
                    WorkerInstanceId = "result-worker",
                    Capabilities = new[] { "READ" },
                    AvailableModelKeys = new[] { "MODEL-A" }
                });
                var task = runtime.Submit(new SubmitBimTaskRequestDto
                {
                    ClientRequestId = Guid.NewGuid(),
                    ProtocolVersion = BimBridgeProtocol.Version,
                    OperationCode = "READ_PROPERTIES",
                    RequiredCapabilities = new[] { "READ" },
                    TargetModelKey = "MODEL-A",
                    Payload = Array.Empty<byte>()
                });
                var wait = await runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                runtime.StartTask(new StartTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = wait.Assignment.AssignmentId
                });

                var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
                string sha;
                using (var hash = SHA256.Create())
                    sha = BitConverter.ToString(hash.ComputeHash(bytes)).Replace("-", "").ToLowerInvariant();
                var begin = await runtime.BeginUploadAsync(new BeginResultUploadRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = wait.Assignment.AssignmentId,
                    TotalLength = bytes.Length,
                    TotalChunks = 2,
                    Sha256 = sha,
                    ContentType = "application/octet-stream"
                }, CancellationToken.None);
                await runtime.UploadChunkAsync(new UploadResultChunkRequestDto
                {
                    UploadId = begin.UploadId,
                    ChunkIndex = 0,
                    Data = new byte[] { 1, 2, 3, 4 }
                }, CancellationToken.None);
                await runtime.UploadChunkAsync(new UploadResultChunkRequestDto
                {
                    UploadId = begin.UploadId,
                    ChunkIndex = 1,
                    Data = new byte[] { 5, 6 }
                }, CancellationToken.None);
                var completed = await runtime.CompleteAsync(new CompleteTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = task.TaskId,
                    AssignmentId = wait.Assignment.AssignmentId,
                    UploadId = begin.UploadId
                }, CancellationToken.None);

                var first = await runtime.ReadResultChunkAsync(task.TaskId, 0, CancellationToken.None);
                var second = await runtime.ReadResultChunkAsync(task.TaskId, 1, CancellationToken.None);
                Assert.That(completed.State, Is.EqualTo(BimTaskStates.Completed));
                Assert.That(first.Data, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                Assert.That(second.Data, Is.EqualTo(new byte[] { 5, 6 }));
            }
            Directory.Delete(directory, true);
        }

        private static BimBridgeOptions CreateOptions(string directory)
        {
            var options = new BimBridgeOptions();
            options.Results.Directory = directory;
            options.Results.ChunkSizeBytes = 4;
            options.Results.MaxResultBytes = 1024;
            options.Tasks.RetryableOperationCodes = new[] { "READ_PROPERTIES" };
            return options;
        }
    }

    internal sealed class SilentIntegrationLog : IBimBridgeLog
    {
        public void Info(string message) { }
        public void Warning(string message) { }
        public void Error(string message, Exception exception) { }
    }
}
