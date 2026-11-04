using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using ProtoBuf;
namespace Pilot.BimBridge.UnitTests
{
    public sealed class ParallelContractTests
    {
        [ProtoContract] [ProtoInclude(109,typeof(OldStatus))] [ProtoInclude(111,typeof(OldChunk))]
        [ProtoInclude(114,typeof(OldStatistics))]
        public abstract class OldResponse
        { [ProtoMember(1)] public BimBridgeErrorDto Error {get;set;} }
        [ProtoContract] public sealed class OldBegin
        {
            [ProtoMember(1)] public Guid TransferId {get;set;}
            [ProtoMember(2)] public string WorkerInstanceId {get;set;}
            [ProtoMember(3)] public BeginResultUploadRequestDto Descriptor {get;set;}
        }
        [ProtoContract] public sealed class OldStatus:OldResponse
        { [ProtoMember(1)] public Guid TaskId {get;set;} [ProtoMember(2)] public string State {get;set;} }
        [ProtoContract] public sealed class OldChunk:OldResponse
        { [ProtoMember(1)] public Guid TaskId {get;set;} [ProtoMember(2)] public int ChunkIndex {get;set;}
          [ProtoMember(3)] public byte[] Data {get;set;} [ProtoMember(4)] public bool IsLast {get;set;} }
        [ProtoContract] public sealed class OldStatistics:OldResponse
        { [ProtoMember(1)] public long SubmittedTasks {get;set;} [ProtoMember(7)] public int ActiveLongPolls {get;set;} }
        [Test] public void AdditiveStatisticsAndStatusRemainReadableByLegacyContracts()
        {
            using(var stream=new MemoryStream())
            {
                Serializer.Serialize<BimBridgeResponseDto>(stream,new BimBridgeStatisticsDto {SubmittedTasks=12,ActiveLongPolls=3,
                    AssignmentsTotal=10,AssignmentsByWorker=new Dictionary<string,long>{{"worker",10}},
                    CompletedTasksByWorker=new Dictionary<string,long>{{"worker",8}},
                    AverageProcessingWallMsByWorker=new Dictionary<string,double>{{"worker",15.5}}});
                stream.Position=0;var old=(OldStatistics)Serializer.Deserialize<OldResponse>(stream);
                Assert.That(old.SubmittedTasks,Is.EqualTo(12));Assert.That(old.ActiveLongPolls,Is.EqualTo(3));
                stream.Position=0;var current=(BimBridgeStatisticsDto)Serializer.Deserialize<BimBridgeResponseDto>(stream);
                Assert.That(current.AssignmentsByWorker["worker"],Is.EqualTo(10));
                Assert.That(current.AverageProcessingWallMsByWorker["worker"],Is.EqualTo(15.5));
            }
            using(var stream=new MemoryStream())
            {
                var id=Guid.NewGuid();Serializer.Serialize<BimBridgeResponseDto>(stream,new BimTaskStatusDto {
                    TaskId=id,State="COMPLETED",WorkerInstanceId="worker",ProcessingWallMs=15.5,
                    SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=id,AttemptNumber=1,SdkVerticesReadMs=2.5,
                        BinaryEncodingActiveMs=1.25,ArtifactWriteWallMs=3.5,ModelAccessWallMs=4.5,
                        SnapshotIndexWallMs=1.5,SnapshotIndexHits=2,
                        WorkerMeshCacheBytes=512,WorkerSnapshotIndexEvictions=3},
                    AssignmentId=Guid.NewGuid(),AttemptQueueWaitMs=1.75,
                    TaskAcceptanceWallMs=0.5,AssignmentWallMs=0.75,ResultPreparationWallMs=1.5,
                    UploadRetryCount=2,UploadRetryWaitMs=3.5,
                    Diagnostics=new DiagnosticEnvelopeDto {ContentType="application/vnd.example+json",SchemaVersion=42,
                        PayloadBytes=new byte[]{1,2},Sha256=new string('A',64)},
                    PriorAttempts=new[]{new TaskAttemptTelemetryDto {AttemptNumber=1,
                        AssignmentId=Guid.NewGuid(),WorkerInstanceId="old",QueueWaitMs=2.25,
                        Diagnostics=new DiagnosticEnvelopeDto {ContentType="application/octet-stream",SchemaVersion=3,
                            PayloadBytes=new byte[]{9},Sha256=new string('B',64)},
                        AssignmentWallMs=0.25,ResultPreparationWallMs=0.5}}});
                stream.Position=0;var old=(OldStatus)Serializer.Deserialize<OldResponse>(stream);
                Assert.That(old.TaskId,Is.EqualTo(id));Assert.That(old.State,Is.EqualTo("COMPLETED"));
                stream.Position=0;var updated=(BimTaskStatusDto)Serializer.Deserialize<BimBridgeResponseDto>(stream);
                Assert.That(updated.SpatialPerformance.SdkVerticesReadMs,Is.EqualTo(2.5));
                Assert.That(updated.SpatialPerformance.BinaryEncodingActiveMs,Is.EqualTo(1.25));
                Assert.That(updated.SpatialPerformance.ModelAccessWallMs,Is.EqualTo(4.5));
                Assert.That(updated.SpatialPerformance.SnapshotIndexHits,Is.EqualTo(2));
                Assert.That(updated.SpatialPerformance.WorkerMeshCacheBytes,Is.EqualTo(512));
                Assert.That(updated.SpatialPerformance.WorkerSnapshotIndexEvictions,Is.EqualTo(3));
                Assert.That(updated.AttemptQueueWaitMs,Is.EqualTo(1.75));
                Assert.That(updated.PriorAttempts[0].QueueWaitMs,Is.EqualTo(2.25));
                Assert.That(updated.TaskAcceptanceWallMs,Is.EqualTo(0.5));
                Assert.That(updated.AssignmentWallMs,Is.EqualTo(0.75));
                Assert.That(updated.ResultPreparationWallMs,Is.EqualTo(1.5));
                Assert.That(updated.UploadRetryCount,Is.EqualTo(2));
                Assert.That(updated.UploadRetryWaitMs,Is.EqualTo(3.5));
                Assert.That(updated.Diagnostics.PayloadBytes,Is.EqualTo(new byte[]{1,2}));
                Assert.That(updated.PriorAttempts[0].Diagnostics.PayloadBytes,Is.EqualTo(new byte[]{9}));
                Assert.That(updated.PriorAttempts[0].AssignmentWallMs,Is.EqualTo(0.25));
                Assert.That(updated.PriorAttempts[0].ResultPreparationWallMs,Is.EqualTo(0.5));
            }
        }
        [Test] public void WorkerMeasuredDurationIsAnOptionalAdditivePrepareField()
        {
            var current=Serializer.DeepClone(new BeginArtifactUploadDto {TransferId=Guid.NewGuid(),WorkerProcessingWallMs=12.5,
                SpatialPerformance=new SpatialTaskPerformanceDto {SourceElements=4},
                Diagnostics=new DiagnosticEnvelopeDto {ContentType="application/octet-stream",SchemaVersion=7,
                    PayloadBytes=new byte[]{3,4},Sha256=new string('C',64)}});
            Assert.That(current.WorkerProcessingWallMs,Is.EqualTo(12.5));
            Assert.That(current.SpatialPerformance.SourceElements,Is.EqualTo(4));
            Assert.That(current.Diagnostics.PayloadBytes,Is.EqualTo(new byte[]{3,4}));
            Assert.That(Serializer.DeepClone(new BeginArtifactUploadDto()).WorkerProcessingWallMs,Is.Null);
            using(var stream=new MemoryStream())
            {
                Serializer.Serialize(stream,current);stream.Position=0;
                var legacy=Serializer.Deserialize<OldBegin>(stream);Assert.That(legacy.TransferId,Is.EqualTo(current.TransferId));
                stream.SetLength(0);stream.Position=0;Serializer.Serialize(stream,legacy);stream.Position=0;
                Assert.That(Serializer.Deserialize<BeginArtifactUploadDto>(stream).WorkerProcessingWallMs,Is.Null);
                stream.Position=0;
                Assert.That(Serializer.Deserialize<BeginArtifactUploadDto>(stream).SpatialPerformance,Is.Null);
                stream.Position=0;
                Assert.That(Serializer.Deserialize<BeginArtifactUploadDto>(stream).Diagnostics,Is.Null);
            }
        }
        [Test] public void LegacyStatusDoesNotInventProcessingTime()
        {
            using(var stream=new MemoryStream())
            {
                Serializer.Serialize<OldResponse>(stream,new OldStatus {TaskId=Guid.NewGuid(),State="COMPLETED"});
                stream.Position=0;var current=(BimTaskStatusDto)Serializer.Deserialize<BimBridgeResponseDto>(stream);
                Assert.That(current.ProcessingWallMs,Is.Null);Assert.That(current.WorkerInstanceId,Is.Null);
            }
        }
        [Test] public void ChunkServingTimeIsAdditiveAndUnknownForOldManager()
        {
            using(var stream=new MemoryStream())
            {
                var current=new BimResultChunkDto {TaskId=Guid.NewGuid(),ChunkIndex=2,
                    Data=new byte[]{1,2},IsLast=true,ManagerServeWallMs=1.5};
                Serializer.Serialize<BimBridgeResponseDto>(stream,current);
                stream.Position=0;var old=(OldChunk)Serializer.Deserialize<OldResponse>(stream);
                Assert.That(old.Data,Is.EqualTo(new byte[]{1,2}));
                stream.SetLength(0);stream.Position=0;
                Serializer.Serialize<OldResponse>(stream,old);
                stream.Position=0;var restored=(BimResultChunkDto)Serializer.Deserialize<BimBridgeResponseDto>(stream);
                Assert.That(restored.ManagerServeWallMs,Is.Null);
            }
        }
    }
}
