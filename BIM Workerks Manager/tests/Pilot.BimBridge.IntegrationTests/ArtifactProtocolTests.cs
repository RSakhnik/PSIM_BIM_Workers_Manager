using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class ArtifactProtocolTests
    {
        private string _root;
        private BimBridgeOptions _options;
        private MutableClock _clock;
        private BimBridgeRuntime _runtime;
        private Guid _registration;
        private BeginArtifactUploadDto _begin;
        private static readonly CancellationToken Token = CancellationToken.None;
        private static string Hash(byte[] bytes)
        { using (var sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", ""); }
        private Guid Register(string name = "stable-worker") => _runtime.Register(new RegisterWorkerRequestDto {
            ProtocolVersion = "1.0", WorkerInstanceId = name, Capabilities = new[]{"READ"} }).RegistrationId;
        private BimBridgeRuntime Runtime() => new BimBridgeRuntime(_options, _clock, new FileSystemBimResultStore(_root), new SilentIntegrationLog(), false);
        [SetUp] public async Task Setup()
        {
            _root = Path.Combine(Path.GetTempPath(), "bim-protocol-tests", Guid.NewGuid().ToString("N"));
            _options = new BimBridgeOptions(); _options.Results.Directory = _root;
            _options.Results.ChunkSizeBytes = 4; _options.Results.MaxResultBytes = 1024;
            _clock = new MutableClock(DateTime.UtcNow); _runtime = Runtime(); _registration = Register();
            var accepted = _runtime.Submit(new SubmitBimTaskRequestDto { ClientRequestId = Guid.NewGuid(),
                ProtocolVersion = "1.0", OperationCode = "READ", RequiredCapabilities = new[]{"READ"} });
            var assigned = await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=_registration,WaitTimeoutSeconds=1},Token);
            _runtime.StartTask(new StartTaskRequestDto {RegistrationId=_registration,TaskId=accepted.TaskId,AssignmentId=assigned.Assignment.AssignmentId});
            _begin = new BeginArtifactUploadDto {TransferId=Guid.NewGuid(),WorkerInstanceId="stable-worker",
                Descriptor=new BeginResultUploadRequestDto {RegistrationId=_registration,TaskId=accepted.TaskId,AssignmentId=assigned.Assignment.AssignmentId,
                    TotalLength=6,TotalChunks=2,Sha256=Hash(new byte[]{1,2,3,4,5,6}),ContentType="application/octet-stream"}};
        }
        [TearDown] public void Cleanup() {_runtime.Dispose();Directory.Delete(_root,true);}
        private ArtifactTransferRequestDto Identity(string name="stable-worker") => new ArtifactTransferRequestDto {
            TransferId=_begin.TransferId,RegistrationId=_registration,WorkerInstanceId=name};
        private Task<ArtifactChunkResponseDto> Chunk(int index,byte[] bytes,string hash=null) => _runtime.WriteArtifactChunkAsync(
            new ArtifactChunkRequestDto {Transfer=Identity(),ChunkIndex=index,Data=bytes,Sha256=hash??Hash(bytes)},Token);
        [Test] public async Task ProcessingDurationExcludesUploaderQueueRetryAndManagerClockTimeAndSurvivesRestart()
        {
            _begin.WorkerProcessingWallMs=12.5;
            _begin.SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=_begin.Descriptor.TaskId,
                AssignmentId=_begin.Descriptor.AssignmentId,AttemptNumber=1,SourceElements=3,TraversedNodes=8,
                SdkTraversalReadMs=4.25,SdkVerticesReadMs=2.5,BoundsCpuMs=1.75,
                BinaryEncodingActiveMs=1.25,ArtifactWriteWallMs=3.5,ArtifactFinalizeWallMs=0.75,
                SdkDispatchWaitWallMs=9.5,SpoolPreparationWallMs=1.75,
                WorkerMeshCacheEntries=3,WorkerMeshCacheBytes=512,WorkerMeshCacheEvictions=2,
                WorkerSnapshotIndexEntries=1,WorkerSnapshotIndexBytes=256,WorkerSnapshotIndexEvictions=4};
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            var preparationMs=_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ResultPreparationWallMs;
            Assert.That(preparationMs,Is.GreaterThanOrEqualTo(0));
            var duplicate=await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            Assert.That(duplicate.IsSuccess,Is.True,duplicate.Error?.Code+": "+duplicate.Error?.Message);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ResultPreparationWallMs,Is.EqualTo(preparationMs));
            _clock.Advance(TimeSpan.FromMinutes(5));_runtime.RunLeaseCheck();_registration=Register();
            await Chunk(0,new byte[]{1,2,3,4});await Chunk(1,new byte[]{5,6});
            var completion=Identity();completion.UploadWallMs=7.25;
            completion.UploadRetryCount=2;completion.UploadRetryWaitMs=3.5;
            Assert.That((await _runtime.CompleteArtifactAsync(completion,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ProcessingWallMs,Is.EqualTo(12.5));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.TraversedNodes,Is.EqualTo(8));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).PreparedAtUtc,Is.Not.Null);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).QueueWaitMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).HashVerificationMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ArtifactPublicationMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadWallMs,Is.EqualTo(7.25));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadRetryCount,Is.EqualTo(2));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadRetryWaitMs,Is.EqualTo(3.5));
            Assert.That(_runtime.GetStatistics().AverageProcessingWallMsByWorker,Is.Empty);
            _runtime.Dispose();_runtime=Runtime();
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ProcessingWallMs,Is.EqualTo(12.5));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).WorkerInstanceId,Is.EqualTo("stable-worker"));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.SdkVerticesReadMs,Is.EqualTo(2.5));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).PreparedAtUtc,Is.Not.Null);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ResultPreparationWallMs,Is.EqualTo(preparationMs));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.BinaryEncodingActiveMs,Is.EqualTo(1.25));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.ArtifactFinalizeWallMs,Is.EqualTo(0.75));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.SdkDispatchWaitWallMs,Is.EqualTo(9.5));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.SpoolPreparationWallMs,Is.EqualTo(1.75));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.WorkerMeshCacheBytes,Is.EqualTo(512));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.WorkerSnapshotIndexEvictions,Is.EqualTo(4));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).QueueWaitMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).HashVerificationMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ArtifactPublicationMs,Is.GreaterThanOrEqualTo(0));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadWallMs,Is.EqualTo(7.25));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadRetryCount,Is.EqualTo(2));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).UploadRetryWaitMs,Is.EqualTo(3.5));
        }
        [Test] public async Task UnreportedProcessingDurationRemainsUnavailable()
        {
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ProcessingWallMs,Is.Null);
        }
        [Test] public async Task InvalidProcessingDurationDoesNotBlockArtifact()
        {
            _begin.WorkerProcessingWallMs=double.NaN;
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).ProcessingWallMs,Is.Null);
        }
        [Test] public async Task LegacyTelemetryPassesWithoutSdkValidation()
        {
            _begin.SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=_begin.Descriptor.TaskId,
                AssignmentId=_begin.Descriptor.AssignmentId,AttemptNumber=1,
                BinaryEncodingActiveMs=double.NaN};
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(double.IsNaN(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.BinaryEncodingActiveMs.Value),Is.True);
        }
        [TestCase(double.NaN,0)]
        [TestCase(1.0,-1)]
        public async Task LegacySnapshotIndexTelemetryIsNotInterpreted(double wallMs,long hits)
        {
            _begin.SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=_begin.Descriptor.TaskId,
                AssignmentId=_begin.Descriptor.AssignmentId,AttemptNumber=1,
                SnapshotIndexWallMs=wallMs,SnapshotIndexHits=hits};
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).SpatialPerformance.SnapshotIndexHits,Is.EqualTo(hits));
        }
        private static DiagnosticEnvelopeDto Envelope(byte[] bytes,int version=1) => new DiagnosticEnvelopeDto {
            ContentType="application/vnd.example.unknown+json",SchemaVersion=version,
            PayloadBytes=bytes,Sha256=Hash(bytes)};
        [Test] public async Task OpaqueDiagnosticsRoundTripAcrossResumeAndRestart()
        {
            var payload=new byte[]{0,1,2,3,4,255};
            _begin.Diagnostics=Envelope(payload,77);
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).Diagnostics.PayloadBytes,Is.EqualTo(payload));
            await Chunk(0,new byte[]{1,2,3,4});
            _runtime.Dispose();_runtime=Runtime();_registration=Register();_begin.Descriptor.RegistrationId=_registration;
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).MissingRanges[0].Start,Is.EqualTo(1));
            await Chunk(1,new byte[]{5,6});
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).IsSuccess,Is.True);
            var status=_runtime.GetTaskStatus(_begin.Descriptor.TaskId);
            Assert.That(status.Diagnostics.SchemaVersion,Is.EqualTo(77));
            Assert.That(status.Diagnostics.PayloadBytes,Is.EqualTo(payload));
            Assert.That(status.Diagnostics.Sha256,Is.EqualTo(Hash(payload)));
            _runtime.Dispose();_runtime=Runtime();
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).Diagnostics.PayloadBytes,Is.EqualTo(payload));
        }
        [Test] public async Task ConflictingRetryDiagnosticsCannotReplaceFirstPayload()
        {
            _begin.Diagnostics=Envelope(new byte[]{8});
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            _begin.Diagnostics=Envelope(new byte[]{9});
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).Diagnostics.PayloadBytes,Is.EqualTo(new byte[]{8}));
        }
        [Test] public async Task InvalidOrOversizeDiagnosticsDoNotBlockArtifact()
        {
            _begin.Diagnostics=Envelope(new byte[]{1});_begin.Diagnostics.Sha256=Hash(new byte[]{2});
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).Diagnostics,Is.Null);
            _begin.Diagnostics=Envelope(new byte[DiagnosticTransport.MaxPayloadBytes+1]);
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            await Chunk(0,new byte[]{1,2,3,4});await Chunk(1,new byte[]{5,6});
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).IsSuccess,Is.True);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).Diagnostics,Is.Null);
        }
        [Test] public async Task InvalidUploadRetryTelemetryDoesNotBlockCompletion()
        {
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).IsSuccess,Is.True);
            await Chunk(0,new byte[]{1,2,3,4});await Chunk(1,new byte[]{5,6});
            var completion=Identity();completion.UploadRetryCount=-1;
            completion.UploadRetryWaitMs=double.NaN;
            Assert.That((await _runtime.CompleteArtifactAsync(completion,Token)).IsSuccess,Is.True);
            var status=_runtime.GetTaskStatus(_begin.Descriptor.TaskId);
            Assert.That(status.UploadRetryCount,Is.Null);
            Assert.That(status.UploadRetryWaitMs,Is.Null);
        }
        [Test] public async Task LostAcksReconnectAndLeaseExpiryNeverRepeatExtraction()
        {
            var first=await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            Assert.That(first.IsSuccess,Is.True);
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).TransferId,Is.EqualTo(first.TransferId));
            Assert.That((await Chunk(1,new byte[]{5,6})).IsSuccess,Is.True);
            Assert.That((await Chunk(1,new byte[]{5,6})).AlreadyPresent,Is.True);
            _clock.Advance(TimeSpan.FromMinutes(2));_runtime.RunLeaseCheck();
            _registration=Register();_begin.Descriptor.RegistrationId=_registration;
            var resumed=await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            Assert.That(resumed.IsSuccess,Is.True);
            Assert.That(resumed.MissingRanges.Length,Is.EqualTo(1));
            Assert.That(resumed.MissingRanges[0].Start,Is.Zero);
            Assert.That(resumed.MissingRanges[0].Count,Is.EqualTo(1));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).State,Is.EqualTo(ArtifactProtocol.ResultUploading));
            Assert.That((await Chunk(0,new byte[]{1,2,3,4})).IsSuccess,Is.True);
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).State,Is.EqualTo("COMPLETED"));
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).State,Is.EqualTo("COMPLETED"));
        }
        [Test] public async Task RestartRecoversTransferAndCompletedTask()
        {
            await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            await Chunk(0,new byte[]{1,2,3,4});
            _runtime.Dispose();_runtime=Runtime();_registration=Register();_begin.Descriptor.RegistrationId=_registration;
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).MissingRanges[0].Start,Is.EqualTo(1));
            await Chunk(1,new byte[]{5,6});
            await _runtime.CompleteArtifactAsync(Identity(),Token);
            _runtime.Dispose();_runtime=Runtime();_registration=Register();
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).State,Is.EqualTo("COMPLETED"));
            Assert.That((await _runtime.GetResultMetadataAsync(_begin.Descriptor.TaskId,Token)).TotalLength,Is.EqualTo(6));
        }
        [Test] public async Task ConflictsAndWrongOwnerAreExplicitErrors()
        {
            await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            await Chunk(0,new byte[]{1,2,3,4});
            Assert.That((await Chunk(1,new byte[]{5,6},new string('0',64))).Error.Code,Is.EqualTo("INTEGRITY_ERROR"));
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).Error.Code,Is.EqualTo("UPLOAD_INCOMPLETE"));
            Assert.That((await Chunk(0,new byte[]{4,3,2,1})).Error.Code,Is.EqualTo("CHUNK_CONFLICT"));
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).TaskError.Code,Is.EqualTo("CHUNK_CONFLICT"));
            _begin.Descriptor.ContentType="application/x-ndjson";
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).Error.Code,Is.EqualTo("TRANSFER_CONFLICT"));
            _registration=Register("other-worker");
            Assert.That((await _runtime.ArtifactStatusAsync(Identity("other-worker"),Token)).Error.Code,Is.EqualTo("TRANSFER_OWNER_MISMATCH"));
        }
        [Test] public async Task TransferRetentionIsIndependentOfAssignmentLease()
        {
            await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            _clock.Advance(TimeSpan.FromMinutes(_options.Results.IncompleteUploadTtlMinutes+1));
            _registration=Register();
            Assert.That((await _runtime.ArtifactStatusAsync(Identity(),Token)).Error.Code,Is.EqualTo("TRANSFER_EXPIRED"));
            await _runtime.RunCleanupAsync(Token);
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).State,Is.EqualTo("EXPIRED"));
            Assert.That((await _runtime.ArtifactStatusAsync(Identity(),Token)).Error.Code,Is.EqualTo("UPLOAD_NOT_FOUND"));
        }
        [Test] public async Task PreparedLocalArtifactCanBeAdoptedWhenFirstBeginNeverReachedServer()
        {
            var reservation=await _runtime.ReserveArtifactAsync(new ReserveArtifactDto {Transfer=Identity(),
                TaskId=_begin.Descriptor.TaskId,AssignmentId=_begin.Descriptor.AssignmentId},Token);
            Assert.That(reservation.IsSuccess,Is.True);
            // SDK result is now durable on worker, but the network fails before
            // the first Begin. Register abandons the execution session normally.
            _clock.Advance(TimeSpan.FromMinutes(2));_runtime.RunLeaseCheck();
            _registration=Register();_begin.Descriptor.RegistrationId=_registration;
            var adopted=await _runtime.BeginOrResumeArtifactAsync(_begin,Token);
            Assert.That(adopted.IsSuccess,Is.True,adopted.Error?.Code);
            await Chunk(0,new byte[]{1,2,3,4});await Chunk(1,new byte[]{5,6});
            Assert.That((await _runtime.CompleteArtifactAsync(Identity(),Token)).State,Is.EqualTo("COMPLETED"));
        }
        [Test] public async Task CancellationSurvivesRestartAndRejectsLateReservedArtifact()
        {
            await _runtime.ReserveArtifactAsync(new ReserveArtifactDto {Transfer=Identity(),TaskId=_begin.Descriptor.TaskId,
                AssignmentId=_begin.Descriptor.AssignmentId},Token);
            _runtime.Cancel(_begin.Descriptor.TaskId);
            _runtime.Dispose();_runtime=Runtime();_registration=Register();_begin.Descriptor.RegistrationId=_registration;
            Assert.That(_runtime.GetTaskStatus(_begin.Descriptor.TaskId).State,Is.EqualTo("CANCELLED"));
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(_begin,Token)).Error.Code,Is.EqualTo("TASK_CANCELLED"));
        }
    }
}
