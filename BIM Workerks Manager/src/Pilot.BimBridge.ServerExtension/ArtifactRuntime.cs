using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed partial class BimBridgeRuntime
    {
        private readonly SemaphoreSlim _artifactBeginGate = new SemaphoreSlim(1, 1);
        private static bool ValidDuration(double value)=>!double.IsNaN(value)&&!double.IsInfinity(value)&&value>=0;
        private static bool ValidDuration(double? value)=>!value.HasValue||ValidDuration(value.Value);
        private static bool ValidCount(long? value)=>!value.HasValue||value.Value>=0;
        private static bool ValidSpatialPerformance(SpatialTaskPerformanceDto profile,BeginResultUploadRequestDto descriptor)
        {
            return profile.SchemaVersion==1 && profile.TaskId==descriptor.TaskId &&
                profile.AssignmentId==descriptor.AssignmentId && profile.AttemptNumber>=1 &&
                profile.SourceElements>=0 && profile.TraversedNodes>=0 && profile.Instances>=0 &&
                profile.UniqueMeshes>=0 && ValidCount(profile.MeshCacheHits) && ValidCount(profile.MeshCacheMisses) &&
                ValidCount(profile.SnapshotIndexHits) && ValidCount(profile.SnapshotIndexMisses) &&
                ValidCount(profile.WorkerMeshCacheEntries) && ValidCount(profile.WorkerMeshCacheBytes) &&
                ValidCount(profile.WorkerMeshCacheEvictions) && ValidCount(profile.WorkerSnapshotIndexEntries) &&
                ValidCount(profile.WorkerSnapshotIndexBytes) && ValidCount(profile.WorkerSnapshotIndexEvictions) &&
                ValidDuration(profile.SdkTraversalReadMs) && ValidDuration(profile.SdkMeshAcquireMs) &&
                ValidDuration(profile.SdkVerticesReadMs) && ValidDuration(profile.BoundsCpuMs) &&
                ValidDuration(profile.BinaryEncodingActiveMs) && ValidDuration(profile.ArtifactWriteWallMs) &&
                ValidDuration(profile.ArtifactFinalizeWallMs) && ValidDuration(profile.ModelAccessWallMs) &&
                ValidDuration(profile.SnapshotIndexWallMs) && ValidDuration(profile.SdkDispatchWaitWallMs) &&
                ValidDuration(profile.SpoolPreparationWallMs);
        }
        private FileSystemBimResultStore ArtifactStore => _results as FileSystemBimResultStore
            ?? throw new InvalidOperationException("Resumable artifacts require a durable result store.");

        private BimBridgeErrorDto ValidateTransferOwnerLocked(ArtifactTransferRequestDto request)
        {
            if (request == null || request.TransferId == Guid.Empty || string.IsNullOrWhiteSpace(request.WorkerInstanceId))
                return ResponseErrors.Create("INVALID_REQUEST", "Transfer identity is required.");
            if (!TryGetActiveWorkerLocked(request.RegistrationId, out var worker, out var error)) return error;
            if (worker.WorkerInstanceId != request.WorkerInstanceId)
                return ResponseErrors.Create("TRANSFER_OWNER_MISMATCH", "Registration does not own this worker identity.");
            return null;
        }

        private async Task<UploadRecord> OwnedTransferAsync(ArtifactTransferRequestDto request, CancellationToken token)
        {
            lock (_gate)
            {
                var error = ValidateTransferOwnerLocked(request);
                if (error != null) throw new ResultStoreException(error.Code, error.Message);
            }
            var upload = await ArtifactStore.LoadUploadAsync(request.TransferId, token).ConfigureAwait(false);
            if (upload == null) throw new ResultStoreException("UPLOAD_NOT_FOUND", "Transfer was not found.");
            if (upload.WorkerInstanceId != request.WorkerInstanceId)
                throw new ResultStoreException("TRANSFER_OWNER_MISMATCH", "Transfer belongs to another worker.");
            var completed = await _results.GetMetadataAsync(upload.TaskId, token).ConfigureAwait(false);
            if (completed == null && _clock.UtcNow >= upload.CreatedAtUtc.AddMinutes(_options.Results.IncompleteUploadTtlMinutes))
                throw new ResultStoreException("TRANSFER_EXPIRED", "Incomplete transfer retention expired.");
            lock (_gate)
            {
                if (_tasks.TryGetValue(upload.TaskId, out var task) &&
                    (task.State == BimTaskStates.Cancelled || task.State == BimTaskStates.CancelRequested))
                    throw new ResultStoreException("TASK_CANCELLED", "Task cancellation was requested.");
                if(task?.State==BimTaskStates.Failed && task.Error!=null)
                    throw new ResultStoreException(task.Error.Code,task.Error.Message);
            }
            return upload;
        }

        private async Task<ArtifactUploadStatusDto> TransferStatusAsync(UploadRecord upload, CancellationToken token)
        {
            var completed = await _results.GetMetadataAsync(upload.TaskId, token).ConfigureAwait(false);
            var missing = completed == null ? await ArtifactStore.MissingChunksAsync(upload, token).ConfigureAwait(false) : Array.Empty<int>();
            var ranges = new List<MissingChunkRangeDto>();
            foreach (var index in missing)
            {
                var last = ranges.LastOrDefault();
                if (last != null && last.Start + last.Count == index) last.Count++;
                else ranges.Add(new MissingChunkRangeDto { Start = index, Count = 1 });
            }
            return new ArtifactUploadStatusDto { TransferId = upload.UploadId, ChunkSizeBytes = upload.ChunkSizeBytes,
                MissingRanges = ranges.ToArray(), Completed = completed != null,
                ExpiresAtUtc = completed == null ? upload.CreatedAtUtc.AddMinutes(_options.Results.IncompleteUploadTtlMinutes)
                    : completed.CreatedAtUtc.AddMinutes(_options.Results.CompletedResultTtlMinutes) };
        }

        public async Task<ArtifactUploadStatusDto> BeginOrResumeArtifactAsync(BeginArtifactUploadDto request, CancellationToken token)
        {
            if (request?.Descriptor == null || request.TransferId == Guid.Empty)
                return ResponseErrors.Error<ArtifactUploadStatusDto>("INVALID_REQUEST", "Transfer descriptor is required.");
            var preparationStarted=System.Diagnostics.Stopwatch.GetTimestamp();
            var identity = new ArtifactTransferRequestDto { TransferId = request.TransferId,
                WorkerInstanceId = request.WorkerInstanceId, RegistrationId = request.Descriptor.RegistrationId };
            await _artifactBeginGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                lock (_gate)
                {
                    var error = ValidateTransferOwnerLocked(identity);
                    if (error != null) return new ArtifactUploadStatusDto { Error = error };
                }
                if(request.WorkerProcessingWallMs.HasValue && (double.IsNaN(request.WorkerProcessingWallMs.Value)||
                    double.IsInfinity(request.WorkerProcessingWallMs.Value)||request.WorkerProcessingWallMs.Value<0))
                    return ResponseErrors.Error<ArtifactUploadStatusDto>("INVALID_REQUEST","Processing duration must be finite and nonnegative.");
                var descriptor = request.Descriptor;
                var performance=request.SpatialPerformance;
                if(performance!=null && !ValidSpatialPerformance(performance,descriptor))
                {
                    _log.Warning("Invalid spatial performance profile ignored for task " + descriptor.TaskId);
                    performance=null;
                }
                if (descriptor.TotalLength > _options.Results.MaxArtifactBytes)
                    return ResponseErrors.Error<ArtifactUploadStatusDto>("RESULT_TOO_LARGE", "Artifact exceeds the configured byte limit.");
                if (descriptor.TotalLength < 0 ||
                    descriptor.Sha256 == null || descriptor.Sha256.Length != 64 || !descriptor.Sha256.All(Uri.IsHexDigit))
                    return ResponseErrors.Error<ArtifactUploadStatusDto>("INVALID_REQUEST", "A bounded artifact with SHA-256 is required.");
                var previous = await ArtifactStore.LoadUploadAsync(request.TransferId, token).ConfigureAwait(false);
                var reservation = ArtifactStore.ReadReservation(request.TransferId);
                if(previous==null)
                {
                    var stored=await _results.GetStoredBytesAsync(token).ConfigureAwait(false);
                    long pending;
                    lock(_gate)pending=_uploads.Values.Sum(value=>value.TotalLength);
                    if(stored+pending>_options.Results.ArtifactDiskQuotaBytes-descriptor.TotalLength)
                        return ResponseErrors.Error<ArtifactUploadStatusDto>("RESOURCE_LIMIT","Artifact disk quota reached.",true);
                }
                UploadRecord upload;
                lock (_gate)
                {
                    TaskRecord task;
                    if (previous == null)
                    {
                        if(reservation!=null)
                        {
                            var terminal=ArtifactStore.ReadTaskState(descriptor.TaskId);
                            if(terminal!=null&&(terminal.State==BimTaskStates.Cancelled||terminal.State==BimTaskStates.CancelRequested))
                                return ResponseErrors.Error<ArtifactUploadStatusDto>("TASK_CANCELLED","The original task was cancelled.");
                            if(reservation.WorkerInstanceId!=request.WorkerInstanceId || reservation.TaskId!=descriptor.TaskId || reservation.AssignmentId!=descriptor.AssignmentId)
                                return ResponseErrors.Error<ArtifactUploadStatusDto>("TRANSFER_CONFLICT","Descriptor differs from its pre-extraction reservation.");
                            if(_clock.UtcNow>=reservation.CreatedAtUtc.AddMinutes(_options.Results.ReservationTtlMinutes))
                                return ResponseErrors.Error<ArtifactUploadStatusDto>("TRANSFER_EXPIRED","Artifact reservation retention expired.");
                            if(!_tasks.TryGetValue(descriptor.TaskId,out task))
                            {
                                task=new TaskRecord {TaskId=reservation.TaskId,ClientRequestId=reservation.ClientRequestId,OperationCode=reservation.OperationCode,
                                    CreatedAtUtc=reservation.TaskCreatedAtUtc,State=BimTaskStates.Running};
                                _tasks[task.TaskId]=task;_clientRequests[task.ClientRequestId]=task.TaskId;
                            }
                            if(task.AssignmentId.HasValue&&task.AssignmentId.Value!=descriptor.AssignmentId)
                                return ResponseErrors.Error<ArtifactUploadStatusDto>("ASSIGNMENT_MISMATCH","Task has been reassigned; stale artifact cannot replace the active execution.");
                            if(task.State==BimTaskStates.Cancelled || task.State==BimTaskStates.CancelRequested || task.State==BimTaskStates.Completed)
                                return ResponseErrors.Error<ArtifactUploadStatusDto>("TASK_ALREADY_COMPLETED","Task is cancelled or already has a completed result.");
                        }
                        else if (!TryValidateAssignmentLocked(descriptor.RegistrationId, descriptor.TaskId, descriptor.AssignmentId,
                            out _, out task, out var error)) return new ArtifactUploadStatusDto { Error = error };
                        if ((reservation==null && task.State != BimTaskStates.Running) || task.UploadId.HasValue)
                            return ResponseErrors.Error<ArtifactUploadStatusDto>("INVALID_REQUEST", "Task is not available for result preparation.");
                        if (_uploads.Count >= _options.Results.MaxParallelUploads)
                            return ResponseErrors.Error<ArtifactUploadStatusDto>("TOO_MANY_UPLOADS", "Upload concurrency limit reached.", true);
                    }
                    else
                    {
                        if (previous.WorkerInstanceId != request.WorkerInstanceId)
                            return ResponseErrors.Error<ArtifactUploadStatusDto>("TRANSFER_OWNER_MISMATCH", "Transfer belongs to another worker.");
                        _tasks.TryGetValue(previous.TaskId, out task);
                    }
                    upload = new UploadRecord { UploadId = request.TransferId, TaskId = descriptor.TaskId,
                        AssignmentId = descriptor.AssignmentId, RegistrationId = descriptor.RegistrationId,
                        WorkerInstanceId = request.WorkerInstanceId, TotalLength = descriptor.TotalLength,
                        WorkerProcessingWallMs=previous?.WorkerProcessingWallMs??request.WorkerProcessingWallMs,
                        SpatialPerformance=previous?.SpatialPerformance??performance,
                        QueueWaitMs=previous?.QueueWaitMs??task?.QueueWaitMs,
                        TotalChunks = descriptor.TotalChunks, ChunkSizeBytes = previous?.ChunkSizeBytes ?? _options.Results.ChunkSizeBytes,
                        Sha256 = descriptor.Sha256, ContentType = descriptor.ContentType, ContentEncoding = descriptor.ContentEncoding,
                        FileName = descriptor.FileName, ResultKind = descriptor.ResultKind,
                        CreatedAtUtc = previous?.CreatedAtUtc ?? _clock.UtcNow,
                        ClientRequestId = previous?.ClientRequestId ?? task.ClientRequestId,
                        OperationCode = previous?.OperationCode ?? task.OperationCode,
                        TaskCreatedAtUtc = previous?.TaskCreatedAtUtc ?? task.CreatedAtUtc };
                    if (previous == null)
                    {
                        task.PreparedAtUtc=_clock.UtcNow;
                        task.WorkerProcessingWallMs=upload.WorkerProcessingWallMs;
                        task.SpatialPerformance=upload.SpatialPerformance;
                        task.QueueWaitMs=upload.QueueWaitMs;
                        task.State = ArtifactProtocol.ResultPrepared;
                        task.UploadId = upload.UploadId;
                        _uploads[upload.UploadId] = upload;
                        ReleaseWorkerLocked(task);
                    }
                }
                try { await _results.BeginUploadAsync(upload, token).ConfigureAwait(false); }
                catch
                {
                    if (previous == null) lock (_gate)
                    {
                        _uploads.Remove(upload.UploadId);
                        // Keep preparation failed rather than requeueing an SDK
                        // extraction whose artifact already exists on the worker.
                        var task = _tasks[upload.TaskId]; task.State = BimTaskStates.Failed;
                        task.CompletedAtUtc = _clock.UtcNow;
                        task.Error = new BimTaskErrorDto { Code = "RESULT_PREPARATION_FAILED", Message = "Durable transfer initialization failed." };
                    }
                    throw;
                }
                if(previous==null)
                {
                    lock(_gate)
                    {
                        if(_tasks.TryGetValue(upload.TaskId,out var preparedTask))
                        {
                            preparedTask.ResultPreparationWallMs=
                                (System.Diagnostics.Stopwatch.GetTimestamp()-preparationStarted)*1000d/System.Diagnostics.Stopwatch.Frequency;
                            TryPersistTelemetryState(preparedTask);
                        }
                    }
                }
                upload = await OwnedTransferAsync(identity, token).ConfigureAwait(false);
                lock (_gate)
                {
                    if (!_tasks.ContainsKey(upload.TaskId))
                    {
                        _tasks[upload.TaskId] = new TaskRecord { TaskId = upload.TaskId, ClientRequestId = upload.ClientRequestId,
                            OperationCode = upload.OperationCode, CreatedAtUtc = upload.TaskCreatedAtUtc,
                            State = ArtifactProtocol.ResultPrepared, UploadId = upload.UploadId,
                            PreparedAtUtc=upload.CreatedAtUtc,
                            ExecutionWorkerInstanceId=upload.WorkerInstanceId,WorkerProcessingWallMs=upload.WorkerProcessingWallMs,
                            SpatialPerformance=upload.SpatialPerformance };
                        _tasks[upload.TaskId].QueueWaitMs=upload.QueueWaitMs;
                        _clientRequests[upload.ClientRequestId] = upload.TaskId;
                    }
                    if (_tasks[upload.TaskId].State != BimTaskStates.Completed) _uploads[upload.UploadId] = upload;
                }
                return await TransferStatusAsync(upload, token).ConfigureAwait(false);
            }
            catch (ResultStoreException error) { return ResponseErrors.Error<ArtifactUploadStatusDto>(error.Code, error.Message); }
            finally { _artifactBeginGate.Release(); }
        }

        public async Task<ArtifactUploadStatusDto> ArtifactStatusAsync(ArtifactTransferRequestDto request, CancellationToken token)
        {
            try { return await TransferStatusAsync(await OwnedTransferAsync(request, token).ConfigureAwait(false), token).ConfigureAwait(false); }
            catch (ResultStoreException error) { return ResponseErrors.Error<ArtifactUploadStatusDto>(error.Code, error.Message); }
        }

        public async Task<ArtifactChunkResponseDto> WriteArtifactChunkAsync(ArtifactChunkRequestDto request, CancellationToken token)
        {
            if (request?.Transfer == null || request.Data == null || request.Data.Length > _options.Results.ChunkSizeBytes)
                return ResponseErrors.Error<ArtifactChunkResponseDto>("INVALID_REQUEST", "Bounded chunk and transfer are required.");
            try
            {
                var upload = await OwnedTransferAsync(request.Transfer, token).ConfigureAwait(false);
                string hash;
                using (var sha = SHA256.Create()) hash = BitConverter.ToString(sha.ComputeHash(request.Data)).Replace("-", "");
                if (!string.Equals(hash, request.Sha256, StringComparison.OrdinalIgnoreCase))
                    return ResponseErrors.Error<ArtifactChunkResponseDto>("INTEGRITY_ERROR", "Chunk SHA-256 mismatch.");
                var written = await _results.WriteChunkAsync(upload, request.ChunkIndex, request.Data, token).ConfigureAwait(false);
                if (written) Interlocked.Add(ref _uploadedBytes, request.Data.Length);
                lock (_gate) if (_tasks.TryGetValue(upload.TaskId, out var task)) task.State = ArtifactProtocol.ResultUploading;
                return new ArtifactChunkResponseDto { ChunkIndex = request.ChunkIndex, AlreadyPresent = !written };
            }
            catch (ResultStoreException error)
            {
                if(error.Code=="CHUNK_CONFLICT")FailPreparedArtifact(request.Transfer.TransferId,error);
                return ResponseErrors.Error<ArtifactChunkResponseDto>(error.Code, error.Message);
            }
        }

        public async Task<CompleteTaskResponseDto> CompleteArtifactAsync(ArtifactTransferRequestDto request, CancellationToken token)
        {
            try
            {
                var upload = await OwnedTransferAsync(request, token).ConfigureAwait(false);
                var metadata = await _results.CompleteUploadAsync(upload, upload.ChunkSizeBytes, _clock.UtcNow, token).ConfigureAwait(false);
                bool cancelled;
                lock (_gate)
                {
                    if (!_tasks.TryGetValue(upload.TaskId, out var task))
                    {
                        task = new TaskRecord { TaskId = upload.TaskId, ClientRequestId = upload.ClientRequestId,
                            OperationCode = upload.OperationCode, CreatedAtUtc = upload.TaskCreatedAtUtc };
                        _tasks[task.TaskId] = task; _clientRequests[task.ClientRequestId] = task.TaskId;
                    }
                    task.WorkerProcessingWallMs=upload.WorkerProcessingWallMs;
                    task.SpatialPerformance=upload.SpatialPerformance;
                    task.QueueWaitMs=upload.QueueWaitMs;
                    task.HashVerificationMs=metadata.HashVerificationMs;
                    if(metadata.ArtifactPublicationMs.HasValue)task.ArtifactPublicationMs=metadata.ArtifactPublicationMs;
                    if(!task.UploadWallMs.HasValue && request.UploadWallMs.HasValue && ValidDuration(request.UploadWallMs.Value))
                        task.UploadWallMs=request.UploadWallMs;
                    if(!task.UploadRetryCount.HasValue && ValidCount(request.UploadRetryCount))
                        task.UploadRetryCount=request.UploadRetryCount;
                    if(!task.UploadRetryWaitMs.HasValue && ValidDuration(request.UploadRetryWaitMs))
                        task.UploadRetryWaitMs=request.UploadRetryWaitMs;
                    task.ExecutionWorkerInstanceId=upload.WorkerInstanceId;
                    cancelled=task.State==BimTaskStates.Cancelled||task.State==BimTaskStates.CancelRequested;
                    if(!cancelled)
                    {
                        if (task.State != BimTaskStates.Completed) {Interlocked.Increment(ref _completed);RecordCompletionLocked(task,upload.WorkerInstanceId);}
                        task.State = BimTaskStates.Completed; task.CompletedAtUtc = metadata.CreatedAtUtc;
                        task.Payload = Array.Empty<byte>(); task.UploadId = null; _uploads.Remove(upload.UploadId);
                        ArtifactStore.SaveTaskState(task);
                        DispatchWaitersLocked();
                    }
                }
                if(cancelled)
                {
                    await _results.DeleteAsync(upload.TaskId,token).ConfigureAwait(false);
                    return ResponseErrors.Error<CompleteTaskResponseDto>("TASK_CANCELLED","Artifact publication raced with cancellation.");
                }
                return new CompleteTaskResponseDto { State = BimTaskStates.Completed, CompletedAtUtc = metadata.CreatedAtUtc };
            }
            catch (ResultStoreException error)
            {
                if(error.Code=="INTEGRITY_ERROR")FailPreparedArtifact(request.TransferId,error);
                return ResponseErrors.Error<CompleteTaskResponseDto>(error.Code, error.Message);
            }
        }
        private void FailPreparedArtifact(Guid transferId,ResultStoreException error)
        {
            lock(_gate)
            {
                if(_uploads.TryGetValue(transferId,out var upload)&&_tasks.TryGetValue(upload.TaskId,out var task)&&task.State!=BimTaskStates.Completed)
                {
                    task.State=BimTaskStates.Failed;task.CompletedAtUtc=_clock.UtcNow;
                    task.Error=new BimTaskErrorDto {Code=error.Code,Message=error.Message,IsRetryable=false};
                    ArtifactStore.SaveTaskState(task);
                    Interlocked.Increment(ref _failed);
                }
            }
        }
    }
}
