using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class StoredArtifactTaskState
    {
        public Guid TaskId,ClientRequestId;
        public Guid? AssignmentId;
        public Guid? UploadId;
        public int AttemptNumber;
        public string State,OperationCode,ExecutionWorkerInstanceId;
        public string[] RequiredCapabilities;
        public string TargetModelKey,PreferredWorkerInstanceId,PayloadContentType,PayloadEncoding;
        public byte[] Payload;
        public int Priority,MaxAttempts;
        public bool CanRetry;
        public DateTime? ExpiresAtUtc;
        public double? WorkerProcessingWallMs;
        public double? QueueWaitMs;
        public double? AttemptQueueWaitMs;
        public double? TaskAcceptanceWallMs;
        public double? AssignmentWallMs;
        public double? ResultPreparationWallMs;
        public List<TaskAttemptTelemetryDto> PriorAttempts;
        public double? HashVerificationMs;
        public double? ArtifactPublicationMs;
        public double? UploadWallMs;
        public int? UploadRetryCount;
        public double? UploadRetryWaitMs;
        public SpatialTaskPerformanceDto SpatialPerformance;
        public DiagnosticEnvelopeDto Diagnostics;
        public DateTime CreatedAtUtc;
        public DateTime? CompletedAtUtc;
        public DateTime? PreparedAtUtc;
        public DateTime? AssignedAtUtc;
        public DateTime? StartedAtUtc;
        public BimTaskErrorDto Error;
        public TaskRecord ToTask()=>new TaskRecord {TaskId=TaskId,ClientRequestId=ClientRequestId,State=State,
            OperationCode=OperationCode,CreatedAtUtc=CreatedAtUtc,CompletedAtUtc=CompletedAtUtc,
            RequiredCapabilities=RequiredCapabilities??Array.Empty<string>(),TargetModelKey=TargetModelKey,
            PreferredWorkerInstanceId=PreferredWorkerInstanceId,PayloadContentType=PayloadContentType,
            PayloadEncoding=PayloadEncoding,Payload=Payload??Array.Empty<byte>(),Priority=Priority,
            MaxAttempts=MaxAttempts,CanRetry=CanRetry,ExpiresAtUtc=ExpiresAtUtc,
            PreparedAtUtc=PreparedAtUtc,AssignedAtUtc=AssignedAtUtc,StartedAtUtc=StartedAtUtc,
            AssignmentId=AssignmentId,UploadId=UploadId,AttemptNumber=AttemptNumber,Error=Error,
            ExecutionWorkerInstanceId=ExecutionWorkerInstanceId,WorkerProcessingWallMs=WorkerProcessingWallMs,
            SpatialPerformance=SpatialPerformance,Diagnostics=Diagnostics,QueueWaitMs=QueueWaitMs,HashVerificationMs=HashVerificationMs,
            AttemptQueueWaitMs=AttemptQueueWaitMs,PriorAttempts=PriorAttempts??new List<TaskAttemptTelemetryDto>(),
            TaskAcceptanceWallMs=TaskAcceptanceWallMs,AssignmentWallMs=AssignmentWallMs,
            ResultPreparationWallMs=ResultPreparationWallMs,
            ArtifactPublicationMs=ArtifactPublicationMs,
            UploadWallMs=UploadWallMs,UploadRetryCount=UploadRetryCount,
            UploadRetryWaitMs=UploadRetryWaitMs};
    }
    internal sealed partial class FileSystemBimResultStore
    {
        private string TaskStateRoot=>Path.Combine(_root,".task-states");
        internal void SaveTaskState(TaskRecord task)
        {
            Directory.CreateDirectory(TaskStateRoot);
            var state=new StoredArtifactTaskState {TaskId=task.TaskId,ClientRequestId=task.ClientRequestId,State=task.State,
                OperationCode=task.OperationCode,CreatedAtUtc=task.CreatedAtUtc,CompletedAtUtc=task.CompletedAtUtc,
                RequiredCapabilities=task.RequiredCapabilities,TargetModelKey=task.TargetModelKey,
                PreferredWorkerInstanceId=task.PreferredWorkerInstanceId,PayloadContentType=task.PayloadContentType,
                PayloadEncoding=task.PayloadEncoding,
                Payload=task.State==BimTaskStates.Completed || task.State==BimTaskStates.Failed ||
                    task.State==BimTaskStates.Cancelled || task.State==BimTaskStates.Expired ? null : task.Payload,
                Priority=task.Priority,
                MaxAttempts=task.MaxAttempts,CanRetry=task.CanRetry,ExpiresAtUtc=task.ExpiresAtUtc,
                PreparedAtUtc=task.PreparedAtUtc,AssignedAtUtc=task.AssignedAtUtc,StartedAtUtc=task.StartedAtUtc,
                AssignmentId=task.AssignmentId,UploadId=task.UploadId,AttemptNumber=task.AttemptNumber,Error=task.Error,
                ExecutionWorkerInstanceId=task.ExecutionWorkerInstanceId,WorkerProcessingWallMs=task.WorkerProcessingWallMs,
                SpatialPerformance=task.SpatialPerformance,Diagnostics=task.Diagnostics,QueueWaitMs=task.QueueWaitMs,HashVerificationMs=task.HashVerificationMs,
                AttemptQueueWaitMs=task.AttemptQueueWaitMs,PriorAttempts=task.PriorAttempts,
                TaskAcceptanceWallMs=task.TaskAcceptanceWallMs,AssignmentWallMs=task.AssignmentWallMs,
                ResultPreparationWallMs=task.ResultPreparationWallMs,
                ArtifactPublicationMs=task.ArtifactPublicationMs,
                UploadWallMs=task.UploadWallMs,UploadRetryCount=task.UploadRetryCount,
                UploadRetryWaitMs=task.UploadRetryWaitMs};
            var path=Path.Combine(TaskStateRoot,task.TaskId.ToString("N")+".json");var temporary=path+".tmp";
            File.WriteAllText(temporary,JsonConvert.SerializeObject(state));
            using(var stream=new FileStream(temporary,FileMode.Open,FileAccess.ReadWrite,FileShare.None))stream.Flush(true);
            if(File.Exists(path))File.Replace(temporary,path,null);else File.Move(temporary,path);
        }
        internal StoredArtifactTaskState ReadTaskState(Guid taskId)
        {
            var path=Path.Combine(TaskStateRoot,taskId.ToString("N")+".json");
            return File.Exists(path)?JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path)):null;
        }
        internal IEnumerable<StoredArtifactTaskState> RecoverTaskStates()
        {
            if(!Directory.Exists(TaskStateRoot))yield break;
            foreach(var path in Directory.EnumerateFiles(TaskStateRoot,"*.json"))
                yield return JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path));
        }
        internal void CleanupTaskStates(DateTime now,TimeSpan retention)
        {
            if(!Directory.Exists(TaskStateRoot))return;
            foreach(var path in Directory.EnumerateFiles(TaskStateRoot,"*.json"))
            {
                var state=JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path));
                if(state.CompletedAtUtc.HasValue&&now-state.CompletedAtUtc.Value>retention)File.Delete(path);
            }
        }
    }
}
