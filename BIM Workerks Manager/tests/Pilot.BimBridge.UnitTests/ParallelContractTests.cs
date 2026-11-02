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
        [ProtoContract] [ProtoInclude(109,typeof(OldStatus))] [ProtoInclude(114,typeof(OldStatistics))]
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
                    TaskId=id,State="COMPLETED",WorkerInstanceId="worker",ProcessingWallMs=15.5});
                stream.Position=0;var old=(OldStatus)Serializer.Deserialize<OldResponse>(stream);
                Assert.That(old.TaskId,Is.EqualTo(id));Assert.That(old.State,Is.EqualTo("COMPLETED"));
            }
        }
        [Test] public void WorkerMeasuredDurationIsAnOptionalAdditivePrepareField()
        {
            var current=Serializer.DeepClone(new BeginArtifactUploadDto {TransferId=Guid.NewGuid(),WorkerProcessingWallMs=12.5});
            Assert.That(current.WorkerProcessingWallMs,Is.EqualTo(12.5));
            Assert.That(Serializer.DeepClone(new BeginArtifactUploadDto()).WorkerProcessingWallMs,Is.Null);
            using(var stream=new MemoryStream())
            {
                Serializer.Serialize(stream,current);stream.Position=0;
                var legacy=Serializer.Deserialize<OldBegin>(stream);Assert.That(legacy.TransferId,Is.EqualTo(current.TransferId));
                stream.SetLength(0);stream.Position=0;Serializer.Serialize(stream,legacy);stream.Position=0;
                Assert.That(Serializer.Deserialize<BeginArtifactUploadDto>(stream).WorkerProcessingWallMs,Is.Null);
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
    }
}
