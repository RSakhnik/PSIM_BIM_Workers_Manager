using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.IntegrationTests
{
    [TestFixture]
    public sealed class ResultValidationTests
    {
        [Test]
        public async Task MissingAndOversizedChunksAreRejectedAndDuplicateIsIdempotent()
        {
            var directory = Path.Combine(
                Environment.GetEnvironmentVariable("BIM_BRIDGE_TEST_ROOT") ?? @"C:\Users\roman\Desktop\BWM tests",
                "Integration",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            var options = new BimBridgeOptions();
            options.Results.Directory = directory;
            options.Results.ChunkSizeBytes = 4;
            options.Results.MaxResultBytes = 6;
            options.Tasks.RetryableOperationCodes = new[] { "READ_PROPERTIES" };
            using (var runtime = new BimBridgeRuntime(options, new SystemClock(), new FileSystemBimResultStore(directory), new SilentIntegrationLog(), false))
            {
                var worker = runtime.Register(new RegisterWorkerRequestDto
                {
                    ProtocolVersion = BimBridgeProtocol.Version,
                    WorkerInstanceId = "validation-worker",
                    Capabilities = new[] { "READ" },
                    AvailableModelKeys = new[] { "MODEL-A" }
                });
                var accepted = runtime.Submit(new SubmitBimTaskRequestDto
                {
                    ClientRequestId = Guid.NewGuid(),
                    ProtocolVersion = BimBridgeProtocol.Version,
                    OperationCode = "READ_PROPERTIES",
                    RequiredCapabilities = new[] { "READ" },
                    TargetModelKey = "MODEL-A"
                });
                var assignment = await runtime.WaitForTaskAsync(new WaitForTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    WaitTimeoutSeconds = 1
                }, CancellationToken.None);
                runtime.StartTask(new StartTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = accepted.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId
                });
                var tooLarge = await runtime.BeginUploadAsync(new BeginResultUploadRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = accepted.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId,
                    TotalLength = 7,
                    TotalChunks = 2
                }, CancellationToken.None);
                var begin = await runtime.BeginUploadAsync(new BeginResultUploadRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = accepted.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId,
                    TotalLength = 6,
                    TotalChunks = 2
                }, CancellationToken.None);
                var first = await runtime.UploadChunkAsync(new UploadResultChunkRequestDto
                {
                    UploadId = begin.UploadId,
                    ChunkIndex = 0,
                    Data = new byte[] { 1, 2, 3, 4 }
                }, CancellationToken.None);
                var duplicate = await runtime.UploadChunkAsync(new UploadResultChunkRequestDto
                {
                    UploadId = begin.UploadId,
                    ChunkIndex = 0,
                    Data = new byte[] { 1, 2, 3, 4 }
                }, CancellationToken.None);
                var incomplete = await runtime.CompleteAsync(new CompleteTaskRequestDto
                {
                    RegistrationId = worker.RegistrationId,
                    TaskId = accepted.TaskId,
                    AssignmentId = assignment.Assignment.AssignmentId,
                    UploadId = begin.UploadId
                }, CancellationToken.None);
                Assert.That(tooLarge.Error.Code, Is.EqualTo(BimBridgeErrorCodes.ResultTooLarge));
                Assert.That(first.IsSuccess, Is.True);
                Assert.That(duplicate.IsSuccess, Is.True);
                Assert.That(duplicate.ReceivedChunks, Is.EqualTo(1));
                Assert.That(incomplete.Error.Code, Is.EqualTo(BimBridgeErrorCodes.UploadIncomplete));
            }
            Directory.Delete(directory, true);
        }
    }
}
