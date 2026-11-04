using System;
using System.Threading;
using System.Threading.Tasks;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed partial class BimBridgeRuntime
    {
        public async Task<BeginResultUploadResponseDto> BeginUploadAsync(BeginResultUploadRequestDto request, CancellationToken cancellationToken)
        {
            if (request == null || request.TotalLength < 0 || request.TotalChunks < 0)
                return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Upload descriptor is invalid.");
            if (request.TotalLength > _options.Results.MaxResultBytes)
                return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.ResultTooLarge, "Result exceeds the configured limit.");
            var expectedChunks = request.TotalLength == 0 ? 0 : (int)Math.Ceiling(request.TotalLength / (double)_options.Results.ChunkSizeBytes);
            if (request.TotalChunks != expectedChunks)
                return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.InvalidRequest, "TotalChunks does not match TotalLength and configured chunk size.");

            UploadRecord upload;
            lock (_gate)
            {
                if (!TryValidateAssignmentLocked(request.RegistrationId, request.TaskId, request.AssignmentId, out _, out var task, out var error))
                    return ResponseErrors.Error<BeginResultUploadResponseDto>(error.Code, error.Message, error.IsRetryable);
                if (task.State == BimTaskStates.CancelRequested)
                    return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.TaskCancelled, "Task cancellation has been requested.");
                if (task.State != BimTaskStates.Running)
                    return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Task is not running.");
                if (task.UploadId.HasValue)
                    return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Task already has an upload.");
                if (_uploads.Count >= _options.Results.MaxParallelUploads)
                    return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.TooManyUploads, "Too many result uploads are active.", true);

                upload = new UploadRecord
                {
                    UploadId = Guid.NewGuid(),
                    TaskId = task.TaskId,
                    AssignmentId = request.AssignmentId,
                    RegistrationId = request.RegistrationId,
                    ContentType = request.ContentType,
                    ContentEncoding = request.ContentEncoding,
                    TotalLength = request.TotalLength,
                    TotalChunks = request.TotalChunks,
                    ChunkSizeBytes = _options.Results.ChunkSizeBytes,
                    Sha256 = request.Sha256,
                    CreatedAtUtc = _clock.UtcNow,
                    FileName = request.FileName,
                    ResultKind = request.ResultKind
                };
                _uploads.Add(upload.UploadId, upload);
                task.UploadId = upload.UploadId;
                task.LeaseExpiresAtUtc = _clock.UtcNow.AddSeconds(_options.Tasks.AssignmentLeaseSeconds);
            }

            try
            {
                await _results.BeginUploadAsync(upload, cancellationToken).ConfigureAwait(false);
                return new BeginResultUploadResponseDto { UploadId = upload.UploadId, ChunkSizeBytes = _options.Results.ChunkSizeBytes };
            }
            catch (Exception exception)
            {
                lock (_gate)
                {
                    _uploads.Remove(upload.UploadId);
                    if (_tasks.TryGetValue(upload.TaskId, out var task) && task.UploadId == upload.UploadId)
                        task.UploadId = null;
                }
                _log.Error("Unable to begin result upload " + upload.UploadId, exception);
                return ResponseErrors.Error<BeginResultUploadResponseDto>(BimBridgeErrorCodes.InternalError, "Unable to initialize result storage.", true);
            }
        }

        public async Task<UploadResultChunkResponseDto> UploadChunkAsync(UploadResultChunkRequestDto request, CancellationToken cancellationToken)
        {
            if (request == null)
                return ResponseErrors.Error<UploadResultChunkResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");
            UploadRecord upload;
            lock (_gate)
            {
                if (!_uploads.TryGetValue(request.UploadId, out upload))
                    return ResponseErrors.Error<UploadResultChunkResponseDto>(BimBridgeErrorCodes.UploadNotFound, "Upload was not found.");
                if (!TryValidateAssignmentLocked(upload.RegistrationId, upload.TaskId, upload.AssignmentId, out _, out var task, out var error))
                    return ResponseErrors.Error<UploadResultChunkResponseDto>(error.Code, error.Message, error.IsRetryable);
                if (request.ChunkIndex < 0 || request.ChunkIndex >= upload.TotalChunks)
                    return ResponseErrors.Error<UploadResultChunkResponseDto>(BimBridgeErrorCodes.ChunkOutOfRange, "Chunk index is outside the declared range.");
                var expectedLength = request.ChunkIndex == upload.TotalChunks - 1
                    ? (int)(upload.TotalLength - (long)request.ChunkIndex * _options.Results.ChunkSizeBytes)
                    : _options.Results.ChunkSizeBytes;
                if ((request.Data?.Length ?? -1) != expectedLength)
                    return ResponseErrors.Error<UploadResultChunkResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Chunk length is invalid.");
                task.LeaseExpiresAtUtc = _clock.UtcNow.AddSeconds(_options.Tasks.AssignmentLeaseSeconds);
            }

            try
            {
                var written = await _results.WriteChunkAsync(upload, request.ChunkIndex, request.Data, cancellationToken).ConfigureAwait(false);
                int received;
                lock (_gate)
                {
                    if (written) upload.ReceivedChunks++;
                    received = upload.ReceivedChunks;
                }
                if (written) Interlocked.Add(ref _uploadedBytes, request.Data.Length);
                return new UploadResultChunkResponseDto { ChunkIndex = request.ChunkIndex, ReceivedChunks = received };
            }
            catch (ResultStoreException exception)
            {
                return ResponseErrors.Error<UploadResultChunkResponseDto>(exception.Code, exception.Message);
            }
            catch (Exception exception)
            {
                _log.Error("Unable to write result chunk for upload " + request.UploadId, exception);
                return ResponseErrors.Error<UploadResultChunkResponseDto>(BimBridgeErrorCodes.InternalError, "Unable to write result chunk.", true);
            }
        }

        public async Task<CompleteTaskResponseDto> CompleteAsync(CompleteTaskRequestDto request, CancellationToken cancellationToken)
        {
            if (request == null)
                return ResponseErrors.Error<CompleteTaskResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");
            UploadRecord upload;
            lock (_gate)
            {
                if (!TryValidateAssignmentLocked(request.RegistrationId, request.TaskId, request.AssignmentId, out _, out var task, out var error))
                    return ResponseErrors.Error<CompleteTaskResponseDto>(error.Code, error.Message, error.IsRetryable);
                if (!_uploads.TryGetValue(request.UploadId, out upload) || task.UploadId != upload.UploadId)
                    return ResponseErrors.Error<CompleteTaskResponseDto>(BimBridgeErrorCodes.UploadNotFound, "Upload was not found for this task.");
                if (task.State == BimTaskStates.CancelRequested && !_options.Cancellation.AcceptCompletedResultAfterCancellation)
                    return ResponseErrors.Error<CompleteTaskResponseDto>(BimBridgeErrorCodes.TaskCancelled, "Completed results are not accepted after cancellation.");
                task.LeaseExpiresAtUtc = _clock.UtcNow.AddSeconds(_options.Tasks.AssignmentLeaseSeconds);
            }

            try
            {
                await _results.CompleteUploadAsync(upload, _options.Results.ChunkSizeBytes, _clock.UtcNow, cancellationToken).ConfigureAwait(false);
            }
            catch (ResultStoreException exception)
            {
                return ResponseErrors.Error<CompleteTaskResponseDto>(exception.Code, exception.Message);
            }
            catch (Exception exception)
            {
                _log.Error("Unable to complete result upload " + request.UploadId, exception);
                return ResponseErrors.Error<CompleteTaskResponseDto>(BimBridgeErrorCodes.InternalError, "Unable to complete result upload.", true);
            }

            lock (_gate)
            {
                if (!TryValidateAssignmentLocked(request.RegistrationId, request.TaskId, request.AssignmentId, out _, out var task, out var error))
                {
                    _ = _results.DeleteAsync(request.TaskId, CancellationToken.None);
                    return ResponseErrors.Error<CompleteTaskResponseDto>(error.Code, error.Message, error.IsRetryable);
                }
                task.State = BimTaskStates.Completed;
                task.CompletedAtUtc = _clock.UtcNow;
                task.Payload = Array.Empty<byte>();
                task.UploadId = null;
                _uploads.Remove(upload.UploadId);
                RecordCompletionLocked(task);
                ReleaseWorkerLocked(task);
                Interlocked.Increment(ref _completed);
                DispatchWaitersLocked();
                return new CompleteTaskResponseDto { State = task.State, CompletedAtUtc = task.CompletedAtUtc.Value };
            }
        }

        public async Task<BimResultMetadataDto> GetResultMetadataAsync(Guid taskId, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (!_tasks.TryGetValue(taskId, out var task))
                    return ResponseErrors.Error<BimResultMetadataDto>(BimBridgeErrorCodes.TaskNotFound, "Task was not found.");
                if (task.State != BimTaskStates.Completed)
                    return ResponseErrors.Error<BimResultMetadataDto>(BimBridgeErrorCodes.InvalidRequest, "Task has no completed result.");
            }
            try
            {
                var metadata = await _results.GetMetadataAsync(taskId, cancellationToken).ConfigureAwait(false);
                if (metadata == null)
                    return ResponseErrors.Error<BimResultMetadataDto>(BimBridgeErrorCodes.TaskNotFound, "Result metadata was not found.");
                return MapMetadata(metadata);
            }
            catch (Exception exception)
            {
                _log.Error("Unable to read result metadata for task " + taskId, exception);
                return ResponseErrors.Error<BimResultMetadataDto>(BimBridgeErrorCodes.InternalError, "Unable to read result metadata.", true);
            }
        }

        public async Task<BimResultChunkDto> ReadResultChunkAsync(Guid taskId, int chunkIndex, CancellationToken cancellationToken)
        {
            lock(_gate)
            {
                if(!_tasks.TryGetValue(taskId,out var task))return ResponseErrors.Error<BimResultChunkDto>(BimBridgeErrorCodes.TaskNotFound,"Task was not found.");
                if(task.State!=BimTaskStates.Completed)return ResponseErrors.Error<BimResultChunkDto>(BimBridgeErrorCodes.InvalidRequest,"Task has no readable completed artifact.");
            }
            Interlocked.Increment(ref _activeDownloads);
            var serveStarted=System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                var metadata = await _results.GetMetadataAsync(taskId, cancellationToken).ConfigureAwait(false);
                if (metadata == null)
                    return ResponseErrors.Error<BimResultChunkDto>(BimBridgeErrorCodes.TaskNotFound, "Result was not found.");
                var data = await _results.ReadChunkAsync(taskId, chunkIndex, metadata.ChunkSizeBytes, cancellationToken).ConfigureAwait(false);
                if(data==null)return ResponseErrors.Error<BimResultChunkDto>(BimBridgeErrorCodes.ResultExpired,"Result retention expired.");
                Interlocked.Add(ref _downloadedBytes,data.Length);
                return new BimResultChunkDto
                {
                    TaskId = taskId,
                    ChunkIndex = chunkIndex,
                    Data = data,
                    IsLast = chunkIndex == metadata.TotalChunks - 1,
                    ManagerServeWallMs=(System.Diagnostics.Stopwatch.GetTimestamp()-serveStarted)*
                        1000d/System.Diagnostics.Stopwatch.Frequency
                };
            }
            catch (ResultStoreException exception)
            {
                return ResponseErrors.Error<BimResultChunkDto>(exception.Code, exception.Message);
            }
            catch (Exception exception)
            {
                _log.Error("Unable to read result chunk for task " + taskId, exception);
                return ResponseErrors.Error<BimResultChunkDto>(BimBridgeErrorCodes.InternalError, "Unable to read result chunk.", true);
            }
            finally {Interlocked.Decrement(ref _activeDownloads);}
        }

    }
}
