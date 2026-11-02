using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class WorkerPipelineTests
    {
        private string _root;
        private BimBridgeSettings _settings;
        private DurableArtifactSpool _spool;
        [SetUp] public void Setup()
        {
            _root=Path.Combine(Path.GetTempPath(),"worker-pipeline",Guid.NewGuid().ToString("N"));
            _settings=new BimBridgeSettings {ResultsDirectory=_root,MaxArtifactBytes=100,MaxPreparedArtifactBytes=200,ArtifactDiskQuotaBytes=200};
            _spool=new DurableArtifactSpool(_settings,"machine:user:durable-1",null);
        }
        [TearDown] public void Cleanup()
        {
            _spool.Dispose();
            var path=Path.GetFullPath(_root);
            if(!path.StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(),"worker-pipeline"))+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new Exception("Unsafe cleanup");
            Directory.Delete(path,true);
        }
        private DurableArtifact Prepare(int length=10,double? duration=null)
        {
            var path=Path.Combine(_root,Guid.NewGuid()+".input");File.WriteAllBytes(path,new byte[length]);
            return _spool.Prepare(new PreparedResult {Path=path,Length=length,Sha256=new string('0',64)},
                new BimTaskAssignmentDto {TaskId=Guid.NewGuid(),AssignmentId=Guid.NewGuid()},4,null,duration);
        }
        private static TaskCompletionSource<bool> Signal()=>new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        [Test] public async Task NextSdkStartsWhilePreviousArtifactUploadIsStillRunning()
        {
            var uploadStarted=Signal();var release=Signal();var cancelled=new CancellationTokenSource();
            var active=0;var peak=0;
            var pipeline=new PreparedArtifactPipeline(_spool,_settings,async(a,t)=> {
                peak=Math.Max(peak,Interlocked.Increment(ref active));_spool.MarkPrepared(a);uploadStarted.TrySetResult(true);
                await release.Task.WaitAsync(t);Interlocked.Decrement(ref active);
            },e=>Assert.Fail(e.ToString()));
            Prepare();var delivery=pipeline.RunAsync(cancelled.Token);
            try
            {
                await uploadStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
                await pipeline.WaitForCapacityAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(2));
                Assert.That(release.Task.IsCompleted,Is.False,"SDK B must not wait for upload A");
                Prepare();Assert.That(pipeline.PreparedQueueDepth,Is.EqualTo(2));
                Assert.That(pipeline.CanAccept,Is.False,"third SDK task must pause");
                var next=pipeline.WaitForCapacityAsync(cancelled.Token);
                await Task.Delay(150);Assert.That(next.IsCompleted,Is.False);
                release.TrySetResult(true);await next.WaitAsync(TimeSpan.FromSeconds(3));
                Assert.That(peak,Is.EqualTo(1),"one artifact uploader per worker");
            }
            finally{cancelled.Cancel();await delivery;cancelled.Dispose();}
        }
        [Test] public async Task DisconnectPreservesQueueAndPausesAtLimitThenReconnectDrains()
        {
            var disconnected=Signal();var online=false;var cancellation=new CancellationTokenSource();
            var pipeline=new PreparedArtifactPipeline(_spool,_settings,(a,t)=> {
                _spool.MarkPrepared(a);
                if(!online){disconnected.TrySetResult(true);throw new IOException("offline");}
                return Task.CompletedTask;
            },e=>{});
            Prepare();var delivery=pipeline.RunAsync(cancellation.Token);
            try
            {
                await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(2));
                await pipeline.WaitForCapacityAsync(cancellation.Token);Prepare();
                Assert.That(pipeline.CanAccept,Is.False);
                Assert.That(_spool.Pending().All(a=>File.Exists(a.Path)),Is.True);
                online=true;
                await pipeline.WaitForCapacityAsync(cancellation.Token).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.That(pipeline.UploadRetryWaitMs,Is.GreaterThan(0));
            }
            finally{cancellation.Cancel();await delivery;cancellation.Dispose();}
        }
        [Test] public void ByteBackpressureAndIdentityLockSurviveRestart()
        {
            var pipeline=new PreparedArtifactPipeline(_spool,_settings,(a,t)=>Task.CompletedTask,e=>{});
            Assert.Throws<BimBridgeException>(()=>Prepare(110));
            Assert.That(pipeline.PreparedQueueDepth,Is.Zero);
        }
        [Test] public async Task TerminalTransferRetainsBytesWithoutBlockingOtherPreparedArtifacts()
        {
            var first=Prepare();var second=Prepare();var cancellation=new CancellationTokenSource();
            var delivered=new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var pipeline=new PreparedArtifactPipeline(_spool,_settings,(a,t)=> {
                if(a.TransferId==first.TransferId)throw new BimBridgeException("TRANSFER_EXPIRED","expired");
                _spool.MarkPrepared(a);delivered.TrySetResult(true);return Task.CompletedTask;
            },e=>{});
            var run=pipeline.RunAsync(cancellation.Token);
            try
            {
                await delivered.Task.WaitAsync(TimeSpan.FromSeconds(2));
                Assert.That(_spool.IsQuarantined(first),Is.True);
                Assert.That(File.Exists(first.Path),Is.True);
                Assert.That(_spool.FailedDeliveries,Is.EqualTo(1));
                Assert.That(_spool.OccupiedBytes,Is.GreaterThanOrEqualTo(first.Descriptor.TotalLength));
            }
            finally{cancellation.Cancel();await run;cancellation.Dispose();}
        }
        [Test] public void DistinctDurableWorkerIdsHaveIndependentSpools()
        {
            var otherSettings=new BimBridgeSettings {ResultsDirectory=_root,DurableWorkerId="another-physical-worker"};
            using(var other=new DurableArtifactSpool(otherSettings,"machine-2:user:another-physical-worker",null))
            {
                Prepare();Assert.That(_spool.Pending().Length,Is.EqualTo(1));Assert.That(other.Pending(),Is.Empty);
            }
        }
        [Test] public void LatePrepareAcknowledgementCannotResurrectACompletedArtifact()
        {
            var artifact=Prepare();_spool.Remove(artifact);_spool.MarkPrepared(artifact);
            Assert.That(_spool.Pending(),Is.Empty);Assert.That(File.Exists(artifact.StatePath),Is.False);
        }
        [Test] public void DurablePreparedQueueRecoveredWithoutRepeatingSdk()
        {
            var artifact=Prepare(100,12.5);_spool.MarkPrepared(artifact);
            Assert.Throws<BimBridgeException>(()=>new DurableArtifactSpool(_settings,"other",null));
            _spool.Dispose();_spool=new DurableArtifactSpool(_settings,"machine:user:durable-1",null);
            Assert.That(_spool.Pending().Single().TransferId,Is.EqualTo(artifact.TransferId));
            Assert.That(_spool.Pending().Single().ManagerPrepared,Is.True);
            Assert.That(_spool.Pending().Single().WorkerProcessingWallMs,Is.EqualTo(12.5));
            _settings.MaxPreparedArtifactBytes=150;
            var pipeline=new PreparedArtifactPipeline(_spool,_settings,(a,t)=>Task.CompletedTask,e=>{});
            Assert.That(pipeline.CanAccept,Is.False,"100 retained + 100 reserved exceeds 150 byte cap");
            Assert.That(File.Exists(artifact.Path),Is.True);
        }
    }
}
