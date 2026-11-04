using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;
using WorkerSettings=AdvancedTools.BimBridge.BimBridgeSettings;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class WorkerArtifactRoundTripTests
    {
        [TestCase(1,false)]
        [TestCase(1,true)]
        [TestCase(3,false)]
        [TestCase(3,true)]
        public async Task TransferWindowDiagnostic(int window,bool diagnostics)
        {
            var root=Path.Combine(Path.GetTempPath(),"bim-transfer-benchmark",Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
            try
            {
                var options=new BimBridgeOptions();options.Results.Directory=Path.Combine(root,"server");options.Results.ChunkSizeBytes=65536;
                using(var runtime=new BimBridgeRuntime(options,new SystemClock(),new FileSystemBimResultStore(options.Results.Directory),new SilentIntegrationLog(),false))
                {
                    var registration=runtime.Register(new RegisterWorkerRequestDto {ProtocolVersion="1.0",WorkerInstanceId="bench",Capabilities=new[]{"READ"}}).RegistrationId;
                    runtime.Submit(new SubmitBimTaskRequestDto {ProtocolVersion="1.0",ClientRequestId=Guid.NewGuid(),OperationCode="READ",RequiredCapabilities=new[]{"READ"}});
                    var assigned=await runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=registration,WaitTimeoutSeconds=1},CancellationToken.None);
                    runtime.StartTask(new StartTaskRequestDto {RegistrationId=registration,TaskId=assigned.Assignment.TaskId,AssignmentId=assigned.Assignment.AssignmentId});
                    var bytes=Enumerable.Range(0,65536*12).Select(i=>(byte)i).ToArray();string hash;
                    using(var sha=SHA256.Create())hash=BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-","");
                    var path=Path.Combine(root,"source");File.WriteAllBytes(path,bytes);
                    var settings=new WorkerSettings {ResultsDirectory=Path.Combine(root,"worker"),MaxInFlightChunks=window};
                    using var spool=new DurableArtifactSpool(settings,"bench",null);
                    var artifact=spool.Prepare(new PreparedResult {Path=path,Length=bytes.Length,Sha256=hash,
                        SpatialPerformance=diagnostics?new SpatialTaskPerformanceDto {TaskId=assigned.Assignment.TaskId,
                            AssignmentId=assigned.Assignment.AssignmentId,AttemptNumber=1,SourceElements=12}:null},
                        assigned.Assignment,options.Results.ChunkSizeBytes);
                    var client=new FaultClient(runtime,false);var watch=System.Diagnostics.Stopwatch.StartNew();
                    await new ResumableArtifactUploader(client,settings,null).UploadAsync(artifact,registration,CancellationToken.None);
                    watch.Stop();spool.Remove(artifact);
                    Assert.That(client.Peak,Is.LessThanOrEqualTo(window));
                    var diagnosticBytes=runtime.GetTaskStatus(assigned.Assignment.TaskId).Diagnostics?.PayloadBytes.Length??0;
                    var report=Newtonsoft.Json.JsonConvert.SerializeObject(new {Window=window,Bytes=bytes.Length,
                        ElapsedMilliseconds=watch.Elapsed.TotalMilliseconds,ChunkCalls=client.ChunkCalls,PeakInFlight=client.Peak,
                        Retries=0,ResumedBytes=0,InjectedChunkLatencyMilliseconds=20,DiagnosticBytes=diagnosticBytes},Newtonsoft.Json.Formatting.Indented);
                    TestContext.WriteLine(report);
                    var output=Environment.GetEnvironmentVariable("BIM_BRIDGE_BENCHMARK_OUTPUT");
                    if(!string.IsNullOrEmpty(output)) {Directory.CreateDirectory(output);File.WriteAllText(Path.Combine(output,
                        "transfer-"+window+"-"+(diagnostics?"with":"without")+"-diagnostics.json"),report);}
                }
            }
            finally {Directory.Delete(root,true);}
        }
        [Test] public async Task RealWorkerUploaderRetriesLostAcksAndPipelinesWithinBound()
        {
            var root=Path.Combine(Path.GetTempPath(),"bim-worker-roundtrip",Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            try
            {
                var options=new BimBridgeOptions();options.Results.Directory=Path.Combine(root,"server");options.Results.ChunkSizeBytes=4;
                using(var runtime=new BimBridgeRuntime(options,new SystemClock(),new FileSystemBimResultStore(options.Results.Directory),new SilentIntegrationLog(),false))
                {
                    var registration=runtime.Register(new RegisterWorkerRequestDto {ProtocolVersion="1.0",WorkerInstanceId="worker",Capabilities=new[]{"READ"}}).RegistrationId;
                    var accepted=runtime.Submit(new SubmitBimTaskRequestDto {ProtocolVersion="1.0",ClientRequestId=Guid.NewGuid(),OperationCode="READ",RequiredCapabilities=new[]{"READ"}});
                    var wait=await runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=registration,WaitTimeoutSeconds=1},CancellationToken.None);
                    runtime.StartTask(new StartTaskRequestDto {RegistrationId=registration,TaskId=accepted.TaskId,AssignmentId=wait.Assignment.AssignmentId});
                    var bytes=Enumerable.Range(0,64).Select(x=>(byte)x).ToArray();string hash;
                    using(var sha=SHA256.Create()) hash=BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-","");
                    var source=Path.Combine(root,"source");File.WriteAllBytes(source,bytes);
                    var settings=new WorkerSettings {ResultsDirectory=Path.Combine(root,"worker"),MaxInFlightChunks=3};
                    using var spool=new DurableArtifactSpool(settings,"worker",null);
                    var artifact=spool.Prepare(new PreparedResult {Path=source,Length=bytes.Length,Sha256=hash,ContentType="application/octet-stream",
                        SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=accepted.TaskId,
                            AssignmentId=wait.Assignment.AssignmentId,AttemptNumber=1,SourceElements=5}},wait.Assignment,4);
                    var client=new FaultClient(runtime);
                    await new ResumableArtifactUploader(client,settings,null,null,spool.RecordRetry)
                        .UploadAsync(artifact,registration,CancellationToken.None);
                    Assert.That(client.BeginCalls,Is.EqualTo(2));
                    Assert.That(client.DuplicateChunkAcks,Is.EqualTo(1));
                    Assert.That(artifact.UploadRetryCount,Is.EqualTo(2));
                    Assert.That(artifact.UploadRetryWaitMs,Is.GreaterThan(0));
                    var status=runtime.GetTaskStatus(accepted.TaskId);
                    Assert.That(status.UploadRetryCount,Is.EqualTo(2));
                    Assert.That(status.UploadRetryWaitMs,Is.GreaterThan(0));
                    Assert.That(status.Diagnostics,Is.Not.Null);
                    Assert.That(status.Diagnostics.ContentType,Is.EqualTo("application/vnd.pilot.bimbridge.spatial-performance+json"));
                    var diagnosticText=System.Text.Encoding.UTF8.GetString(status.Diagnostics.PayloadBytes);
                    Assert.That(Newtonsoft.Json.JsonConvert.DeserializeObject<SpatialTaskPerformanceDto>(diagnosticText).SourceElements,Is.EqualTo(5));
                    using(var sha=SHA256.Create())Assert.That(BitConverter.ToString(sha.ComputeHash(status.Diagnostics.PayloadBytes)).Replace("-",""),
                        Is.EqualTo(status.Diagnostics.Sha256));
                    Assert.That(client.Peak,Is.InRange(2,3));
                    Assert.That(spool.Pending().Length,Is.EqualTo(1)); // uploader cannot delete before caller observes completion
                    var metadata=await runtime.GetResultMetadataAsync(accepted.TaskId,CancellationToken.None);
                    Assert.That(metadata.Sha256,Is.EqualTo(hash.ToLowerInvariant()));
                    spool.Remove(artifact);Assert.That(spool.Pending(),Is.Empty);
                }
            }
            finally {Directory.Delete(root,true);}
        }
        private sealed class FaultClient:IBimBridgeArtifactClient
        {
            private readonly BimBridgeRuntime _runtime;
            public int BeginCalls,DuplicateChunkAcks,Peak,ChunkCalls;
            private int _active,_lostChunk;
            private readonly bool _faults;
            public FaultClient(BimBridgeRuntime runtime,bool faults=true) {_runtime=runtime;_faults=faults;}
            public Task<ArtifactUploadStatusDto> ReserveArtifactAsync(ReserveArtifactDto request)=>_runtime.ReserveArtifactAsync(request,CancellationToken.None);
            public async Task<ArtifactUploadStatusDto> BeginOrResumeAsync(BeginArtifactUploadDto request)
            {
                var result=await _runtime.BeginOrResumeArtifactAsync(request,CancellationToken.None);
                if(++BeginCalls==1&&_faults) throw new IOException("injected lost Begin response");
                return result;
            }
            public Task<ArtifactUploadStatusDto> UploadStatusAsync(ArtifactTransferRequestDto request)=>_runtime.ArtifactStatusAsync(request,CancellationToken.None);
            public async Task<ArtifactChunkResponseDto> WriteArtifactChunkAsync(ArtifactChunkRequestDto request)
            {
                var current=Interlocked.Increment(ref _active);Peak=Math.Max(Peak,current);
                Interlocked.Increment(ref ChunkCalls);
                try
                {
                    await Task.Delay(20);
                    var result=await _runtime.WriteArtifactChunkAsync(request,CancellationToken.None);
                    if(result.AlreadyPresent) Interlocked.Increment(ref DuplicateChunkAcks);
                    if(_faults&&request.ChunkIndex==0 && Interlocked.Exchange(ref _lostChunk,1)==0) throw new IOException("injected lost chunk ACK");
                    return result;
                }
                finally {Interlocked.Decrement(ref _active);}
            }
            public Task<CompleteTaskResponseDto> CompleteArtifactAsync(ArtifactTransferRequestDto request)=>_runtime.CompleteArtifactAsync(request,CancellationToken.None);
        }
    }
}
