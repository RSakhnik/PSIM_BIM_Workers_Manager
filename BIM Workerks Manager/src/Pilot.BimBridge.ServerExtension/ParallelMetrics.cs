using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Pilot.BimBridge.Contracts;
namespace Pilot.BimBridge.ServerExtension
{
    internal sealed partial class BimBridgeRuntime
    {
        private readonly Dictionary<string,long> _assignmentsByWorker=new Dictionary<string,long>();
        private readonly Dictionary<string,long> _completedByWorker=new Dictionary<string,long>();
        private readonly Dictionary<string,double> _processingWallByWorker=new Dictionary<string,double>();
        private readonly Dictionary<string,long> _processingSamples=new Dictionary<string,long>();
        private readonly Stopwatch _metricsLifetime=Stopwatch.StartNew();
        private int _activeDownloads;
        private long _downloadedBytes;
        private void RecordCompletionLocked(TaskRecord task,string owner=null)
        {
            owner=owner??task.ExecutionWorkerInstanceId;
            if(string.IsNullOrEmpty(owner))return;
            _completedByWorker.TryGetValue(owner,out var count);_completedByWorker[owner]=count+1;
            if(!task.WorkerProcessingWallMs.HasValue)return;
            _processingSamples.TryGetValue(owner,out var samples);_processingSamples[owner]=samples+1;
            _processingWallByWorker.TryGetValue(owner,out var total);
            _processingWallByWorker[owner]=total+task.WorkerProcessingWallMs.Value;
        }
        private void AddParallelStatistics(BimBridgeStatisticsDto result)
        {
            result.QueuedTasks=_tasks.Values.Count(t=>t.State==BimTaskStates.Queued);
            result.RunningTasks=_tasks.Values.Count(t=>t.State==BimTaskStates.Assigned||t.State==BimTaskStates.Running||t.State==BimTaskStates.CancelRequested);
            result.WaitingWorkers=_waiters.Count;
            result.BusyWorkers=_workers.Values.Count(w=>!w.IsStale&&w.CurrentTaskId.HasValue);
            result.AssignmentsByWorker=new Dictionary<string,long>(_assignmentsByWorker);
            result.AssignmentsTotal=_assignmentsByWorker.Values.Sum();
            result.CompletedTasksByWorker=new Dictionary<string,long>(_completedByWorker);
            result.AverageProcessingWallMsByWorker=_processingSamples.ToDictionary(x=>x.Key,x=>_processingWallByWorker[x.Key]/x.Value);
            result.ActiveUploads=_uploads.Count;
            result.PreparedArtifacts=_tasks.Values.Count(t=>t.State==ArtifactProtocol.ResultPrepared||t.State==ArtifactProtocol.ResultUploading);
            result.ActiveDownloads=Volatile.Read(ref _activeDownloads);
            result.DownloadedBytes=Interlocked.Read(ref _downloadedBytes);
            var seconds=Math.Max(.001,_metricsLifetime.Elapsed.TotalSeconds);
            result.UploadBytesPerSecond=result.UploadedBytes/seconds;
            result.DownloadBytesPerSecond=result.DownloadedBytes/seconds;
            result.ManagerShaDurationMs=(_results as FileSystemBimResultStore)?.ShaDurationMs??0;
        }
    }
}
