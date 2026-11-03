using System;
using System.Collections.Concurrent;
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
    public sealed class MultiWorkerTests
    {
        private string _root;
        private BimBridgeRuntime _runtime;
        private MutableClock _clock;
        private BimBridgeOptions _options;
        [SetUp] public void Setup()
        {
            _root=Path.Combine(Path.GetTempPath(),"multi-worker-tests",Guid.NewGuid().ToString("N"));
            _clock=new MutableClock(DateTime.UtcNow);_options=new BimBridgeOptions();
            _options.Results.Directory=_root;_options.Results.ChunkSizeBytes=4;
            _options.Tasks.RetryableOperationCodes=new[]{"READ"};
            _runtime=new BimBridgeRuntime(_options,_clock,new FileSystemBimResultStore(_root),new SilentIntegrationLog(),false);
        }
        [TearDown] public void Cleanup()
        {
            _runtime.Dispose();var full=Path.GetFullPath(_root);
            if(!full.StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(),"multi-worker-tests"))+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new Exception("Unsafe cleanup");
            Directory.Delete(full,true);
        }
        private Guid Register(string name,string capability="READ",string model="MODEL")
        {
            var response=_runtime.Register(new RegisterWorkerRequestDto {ProtocolVersion="1.0",WorkerInstanceId=name,
                AvailableModelKeys=new[]{model},Capabilities=new[]{capability}});
            Assert.That(response.IsSuccess,Is.True);return response.RegistrationId;
        }
        private Guid Submit(string preferred=null)
        {
            var result=_runtime.Submit(new SubmitBimTaskRequestDto {ProtocolVersion="1.0",ClientRequestId=Guid.NewGuid(),OperationCode="READ",
                RequiredCapabilities=new[]{"READ"},TargetModelKey="MODEL",PreferredWorkerInstanceId=preferred,CanRetry=true,MaxAttempts=3});
            Assert.That(result.IsSuccess,Is.True);return result.TaskId;
        }
        private Task<WaitForTaskResponseDto> Next(Guid worker,CancellationToken token=default)=>_runtime.WaitForTaskAsync(
            new WaitForTaskRequestDto {RegistrationId=worker,WaitTimeoutSeconds=1},token);
        private async Task Complete(Guid registration,string name,BimTaskAssignmentDto assignment,Task hold=null)
        {
            Assert.That(_runtime.StartTask(new StartTaskRequestDto {RegistrationId=registration,TaskId=assignment.TaskId,AssignmentId=assignment.AssignmentId}).IsSuccess,Is.True);
            var data=assignment.TaskId.ToByteArray().Take(4).ToArray();string hash;
            using(var sha=SHA256.Create())hash=BitConverter.ToString(sha.ComputeHash(data)).Replace("-","");
            var identity=new ArtifactTransferRequestDto {TransferId=Guid.NewGuid(),RegistrationId=registration,WorkerInstanceId=name};
            Assert.That((await _runtime.ReserveArtifactAsync(new ReserveArtifactDto {Transfer=identity,TaskId=assignment.TaskId,AssignmentId=assignment.AssignmentId},default)).IsSuccess,Is.True);
            Assert.That((await _runtime.BeginOrResumeArtifactAsync(new BeginArtifactUploadDto {TransferId=identity.TransferId,WorkerInstanceId=name,
                Descriptor=new BeginResultUploadRequestDto {RegistrationId=registration,TaskId=assignment.TaskId,AssignmentId=assignment.AssignmentId,
                    TotalLength=4,TotalChunks=1,Sha256=hash}},default)).IsSuccess,Is.True);
            if(hold!=null)await hold;
            Assert.That((await _runtime.WriteArtifactChunkAsync(new ArtifactChunkRequestDto {Transfer=identity,ChunkIndex=0,Data=data,Sha256=hash},default)).IsSuccess,Is.True);
            Assert.That((await _runtime.CompleteArtifactAsync(identity,default)).IsSuccess,Is.True);
        }
        [Test] public async Task MultipleWorkersDrainSharedQueue()
        {
            var workers=Enumerable.Range(0,4).Select(i=>Register("w"+i)).ToArray();
            var tasks=Enumerable.Range(0,20).Select(_=>Submit()).ToArray();
            var seen=new ConcurrentDictionary<Guid,int>();var ticket=0;
            await Task.WhenAll(workers.Select((w,i)=>Task.Run(async()=> {
                while(Interlocked.Increment(ref ticket)<=20)
                {
                    var next=await Next(w);Assert.That(next.HasTask,Is.True);
                    seen.AddOrUpdate(next.Assignment.TaskId,1,(_,n)=>n+1);
                    Assert.That((await Next(w)).Error.Code,Is.EqualTo(BimBridgeErrorCodes.WorkerBusy));
                    await Complete(w,"w"+i,next.Assignment);
                }
            })));
            Assert.That(seen.Keys,Is.EquivalentTo(tasks));Assert.That(seen.Values.All(n=>n==1),Is.True);
            Assert.That(_runtime.GetStatistics().CompletedTasksByWorker.Values.Sum(),Is.EqualTo(20));
            Assert.That(_runtime.GetStatistics().EligibleWorkers,Is.Zero); // READ-only workers.
        }
        [Test] public void EligibleWorkersCountsOnlySpatialCapableOnlineRegistrations()
        {
            Register("read-only");
            _runtime.Register(new RegisterWorkerRequestDto {ProtocolVersion="1.0",WorkerInstanceId="spatial",
                AvailableModelKeys=new[]{"MODEL"},Capabilities=new[]{ArtifactProtocol.ExportSpatial,ArtifactProtocol.BinarySpatial}});
            Assert.That(_runtime.GetStatistics().EligibleWorkers,Is.EqualTo(1));
            _clock.Advance(TimeSpan.FromSeconds(46));_runtime.RunLeaseCheck();
            Assert.That(_runtime.GetStatistics().EligibleWorkers,Is.Zero);
        }
        [Test] public async Task NoTaskAssignedTwice()
        {
            var workers=Enumerable.Range(0,4).Select(i=>Register("w"+i)).ToArray();
            Enumerable.Range(0,20).ToList().ForEach(_=>Submit());
            var assignments=await Task.WhenAll(workers.Select(w=>Next(w)));
            Assert.That(assignments.Select(a=>a.Assignment.TaskId).Distinct().Count(),Is.EqualTo(4));
            Assert.That(_runtime.GetStatistics().BusyWorkers,Is.EqualTo(4));
            Assert.That(_runtime.GetStatistics().QueuedTasks,Is.EqualTo(16));
        }
        [Test] public async Task FasterWorkerReceivesMoreTasks()
        {
            var slow=Register("slow");var fast=Register("fast");Enumerable.Range(0,20).ToList().ForEach(_=>Submit());
            var slowTask=(await Next(slow)).Assignment;
            for(var i=0;i<19;i++)await Complete(fast,"fast",(await Next(fast)).Assignment);
            await Complete(slow,"slow",slowTask);
            var stats=_runtime.GetStatistics();Assert.That(stats.AssignmentsByWorker["fast"],Is.EqualTo(19));
            Assert.That(stats.AssignmentsByWorker["slow"],Is.EqualTo(1));
        }
        [Test] public Task WorkerCapabilityFiltering()=>AssertWorkerFiltering("OTHER","MODEL");
        [Test] public Task WorkerModelFiltering()=>AssertWorkerFiltering("READ","OTHER");
        private async Task AssertWorkerFiltering(string capability,string model)
        {
            var bad=Register("bad",capability,model);var good=Register("good");var id=Submit();
            using(var cancel=new CancellationTokenSource(30))
                Assert.That((await Next(bad,cancel.Token)).HasTask,Is.False);
            Assert.That((await Next(good)).Assignment.TaskId,Is.EqualTo(id));
        }
        [Test] public async Task LostWorkerTaskRequeued()
        {
            var old=Register("old");var id=Submit();var first=(await Next(old)).Assignment;
            _clock.Advance(TimeSpan.FromSeconds(31));_runtime.RunLeaseCheck();
            var other=Register("other");var second=(await Next(other)).Assignment;
            Assert.That(second.TaskId,Is.EqualTo(id));Assert.That(second.AssignmentId,Is.Not.EqualTo(first.AssignmentId));
            Assert.That(_runtime.StartTask(new StartTaskRequestDto {RegistrationId=old,TaskId=id,AssignmentId=first.AssignmentId}).IsSuccess,Is.False);
        }
        [TestCase("identity")] [TestCase("utf-8")] public async Task PinnedSpatialShardRequeuesEvenWithLegacyRetryWhitelist(string encoding)
        {
            var old=Register("old",ArtifactProtocol.ExportSpatial);
            var id=_runtime.Submit(new SubmitBimTaskRequestDto {ClientRequestId=Guid.NewGuid(),ProtocolVersion="1.0",
                OperationCode=ArtifactProtocol.ExportSpatial,RequiredCapabilities=new[]{ArtifactProtocol.ExportSpatial},
                TargetModelKey="MODEL",CanRetry=true,MaxAttempts=3,PayloadEncoding=encoding,
                Payload=System.Text.Encoding.UTF8.GetBytes("{\"ModelVersionUtc\":\"2026-01-01T00:00:00Z\"}")}).TaskId;
            var first=(await Next(old)).Assignment;_clock.Advance(TimeSpan.FromSeconds(31));_runtime.RunLeaseCheck();
            var other=Register("other",ArtifactProtocol.ExportSpatial);var second=(await Next(other)).Assignment;
            Assert.That(second.TaskId,Is.EqualTo(id));Assert.That(second.AssignmentId,Is.Not.EqualTo(first.AssignmentId));
        }
        [Test] public async Task StaleReservedArtifactCannotStealNewAssignment()
        {
            var old=Register("old");var id=Submit();var first=(await Next(old)).Assignment;
            _runtime.StartTask(new StartTaskRequestDto {RegistrationId=old,TaskId=id,AssignmentId=first.AssignmentId});
            var transfer=new ArtifactTransferRequestDto {TransferId=Guid.NewGuid(),RegistrationId=old,WorkerInstanceId="old"};
            await _runtime.ReserveArtifactAsync(new ReserveArtifactDto {Transfer=transfer,TaskId=id,AssignmentId=first.AssignmentId},default);
            _clock.Advance(TimeSpan.FromSeconds(61));_runtime.RunLeaseCheck();
            var other=Register("other");var second=(await Next(other)).Assignment;
            old=Register("old");
            var stale=await _runtime.BeginOrResumeArtifactAsync(new BeginArtifactUploadDto {TransferId=transfer.TransferId,
                WorkerInstanceId="old",Descriptor=new BeginResultUploadRequestDto {RegistrationId=old,TaskId=id,AssignmentId=first.AssignmentId,
                    TotalLength=4,TotalChunks=1,Sha256=new string('0',64)}},default);
            Assert.That(stale.Error.Code,Is.EqualTo("ASSIGNMENT_MISMATCH"));
            Assert.That(_runtime.StartTask(new StartTaskRequestDto {RegistrationId=other,TaskId=id,AssignmentId=second.AssignmentId}).IsSuccess,Is.True);
        }
        [Test] public async Task PreferenceRaisesPriorityWithoutHardAffinity()
        {
            var a=Register("a");var b=Register("b");var ordinary=Submit();var preferred=Submit("b");
            Assert.That((await Next(b)).Assignment.TaskId,Is.EqualTo(preferred));
            Assert.That((await Next(a)).Assignment.TaskId,Is.EqualTo(ordinary));
            var c=Register("c");var fallback=Submit("absent-worker");
            Assert.That((await Next(c)).Assignment.TaskId,Is.EqualTo(fallback));
        }
        [TestCase(true)] [TestCase(false)]
        public async Task ConcurrentTransfersRespectUploadAndDiskQuotas(bool uploadLimit)
        {
            if(uploadLimit)_options.Results.MaxParallelUploads=1;
            else _options.Results.ArtifactDiskQuotaBytes=4;
            var a=Register("a");var b=Register("b");Submit();Submit();
            var first=(await Next(a)).Assignment;
            var release=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var held=Complete(a,"a",first,release.Task);
            try
            {
                var deadline=DateTime.UtcNow.AddSeconds(3);
                while(_runtime.GetStatistics().PreparedArtifacts<1&&DateTime.UtcNow<deadline)await Task.Delay(10);
                var second=(await Next(b)).Assignment;
                _runtime.StartTask(new StartTaskRequestDto {RegistrationId=b,TaskId=second.TaskId,AssignmentId=second.AssignmentId});
                var identity=new ArtifactTransferRequestDto {RegistrationId=b,TransferId=Guid.NewGuid(),WorkerInstanceId="b"};
                await _runtime.ReserveArtifactAsync(new ReserveArtifactDto {Transfer=identity,TaskId=second.TaskId,AssignmentId=second.AssignmentId},default);
                var denied=await _runtime.BeginOrResumeArtifactAsync(new BeginArtifactUploadDto {TransferId=identity.TransferId,WorkerInstanceId="b",
                    Descriptor=new BeginResultUploadRequestDto {RegistrationId=b,TaskId=second.TaskId,AssignmentId=second.AssignmentId,
                        TotalLength=4,TotalChunks=1,Sha256=new string('0',64)}},default);
                Assert.That(denied.Error.Code,Is.EqualTo(uploadLimit?"TOO_MANY_UPLOADS":"RESOURCE_LIMIT"));
                Assert.That(denied.Error.IsRetryable,Is.True);
                Assert.That(_runtime.GetStatistics().ActiveUploads,Is.EqualTo(1));
                Assert.That(_runtime.GetStatistics().BusyWorkers,Is.EqualTo(1));
            }
            finally{release.TrySetResult(true);await held;}
            Assert.That((await _runtime.GetResultMetadataAsync(first.TaskId,default)).TotalLength,Is.EqualTo(4));
        }
        [Test] public async Task ConcurrentTransfersRespectLimitAndPermitNextSdkBeforeUploadCompletes()
        {
            _options.Results.MaxParallelUploads=4;
            var workers=Enumerable.Range(0,4).Select(i=>Register("w"+i)).ToArray();
            Enumerable.Range(0,5).ToList().ForEach(_=>Submit());
            var first=await Task.WhenAll(workers.Select(w=>Next(w)));
            var gate=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var uploads=workers.Select((w,i)=>Complete(w,"w"+i,first[i].Assignment,gate.Task)).ToArray();
            try
            {
                var deadline=DateTime.UtcNow.AddSeconds(3);
                while(_runtime.GetStatistics().PreparedArtifacts<4&&DateTime.UtcNow<deadline)await Task.Delay(10);
                Assert.That(_runtime.GetStatistics().PreparedArtifacts,Is.EqualTo(4));
                var second=await Next(workers[0]);Assert.That(second.HasTask,Is.True);
                Assert.That(uploads.All(t=>!t.IsCompleted),Is.True);
            }
            finally{gate.TrySetResult(true);await Task.WhenAll(uploads);}
            Assert.That(_runtime.GetStatistics().CompletedTasks,Is.EqualTo(4));
            Assert.That(_runtime.GetStatistics().BusyWorkers,Is.EqualTo(1),"old transfer must not release newer execution");
        }
    }
}
