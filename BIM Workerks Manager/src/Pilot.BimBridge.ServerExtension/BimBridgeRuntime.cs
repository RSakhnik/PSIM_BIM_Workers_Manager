using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed partial class BimBridgeRuntime : IDisposable
    {
        private readonly object _gate = new object();
        private readonly BimBridgeOptions _options;
        private readonly IClock _clock;
        private readonly IBimResultStore _results;
        private readonly IBimBridgeLog _log;
        private readonly Dictionary<Guid, WorkerRecord> _workers = new Dictionary<Guid, WorkerRecord>();
        private readonly Dictionary<string, Guid> _workerInstances = new Dictionary<string, Guid>(StringComparer.Ordinal);
        private readonly Dictionary<Guid, TaskRecord> _tasks = new Dictionary<Guid, TaskRecord>();
        private readonly Dictionary<Guid, Guid> _clientRequests = new Dictionary<Guid, Guid>();
        private readonly Dictionary<Guid, UploadRecord> _uploads = new Dictionary<Guid, UploadRecord>();
        private readonly Dictionary<Guid, Waiter> _waiters = new Dictionary<Guid, Waiter>();
        private readonly HashSet<string> _retryableOperations;
        private readonly CancellationTokenSource _shutdown = new CancellationTokenSource();
        private readonly Task _leaseLoop;
        private readonly Task _cleanupLoop;
        private long _submitted;
        private long _completed;
        private long _failed;
        private long _cancelled;
        private long _retried;
        private long _uploadedBytes;

        public BimBridgeRuntime(BimBridgeOptions options, IClock clock, IBimResultStore results, IBimBridgeLog log, bool startMaintenance = true)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _clock = clock ?? throw new ArgumentNullException(nameof(clock));
            _results = results ?? throw new ArgumentNullException(nameof(results));
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _retryableOperations = new HashSet<string>(options.Tasks.RetryableOperationCodes ?? Array.Empty<string>(), StringComparer.Ordinal);
            if (results is FileSystemBimResultStore durableStore)
            {
                foreach (var recovered in durableStore.RecoverArtifacts())
                {
                    var upload = recovered.Item1;
                    var metadata = recovered.Item2;
                    _tasks[upload.TaskId] = new TaskRecord { TaskId = upload.TaskId, ClientRequestId = upload.ClientRequestId,
                        OperationCode = upload.OperationCode, CreatedAtUtc = upload.TaskCreatedAtUtc,
                        State = metadata == null ? ArtifactProtocol.ResultPrepared : BimTaskStates.Completed,
                        CompletedAtUtc = metadata?.CreatedAtUtc, UploadId = metadata == null ? (Guid?)upload.UploadId : null,
                        PreparedAtUtc=upload.CreatedAtUtc,
                        WorkerProcessingWallMs=upload.WorkerProcessingWallMs,ExecutionWorkerInstanceId=upload.WorkerInstanceId,
                        SpatialPerformance=upload.SpatialPerformance,Diagnostics=upload.Diagnostics,QueueWaitMs=upload.QueueWaitMs,
                        HashVerificationMs=metadata?.HashVerificationMs,ArtifactPublicationMs=metadata?.ArtifactPublicationMs };
                    _clientRequests[upload.ClientRequestId] = upload.TaskId;
                    if (metadata == null) _uploads[upload.UploadId] = upload;
                }
                foreach(var state in durableStore.RecoverTaskStates())
                {
                    var restored=state.ToTask();
                    if(restored.State==BimTaskStates.CancelRequested)
                    {
                        restored.State=BimTaskStates.Cancelled;restored.CompletedAtUtc=_clock.UtcNow;
                        durableStore.SaveTaskState(restored);
                    }
                    _tasks[state.TaskId]=restored;
                    _clientRequests[state.ClientRequestId]=state.TaskId;
                }
            }
            _leaseLoop = startMaintenance ? Task.Run(() => LeaseLoopAsync(_shutdown.Token)) : Task.CompletedTask;
            _cleanupLoop = startMaintenance ? Task.Run(() => CleanupLoopAsync(_shutdown.Token)) : Task.CompletedTask;
        }

        public RegisterWorkerResponseDto Register(RegisterWorkerRequestDto request)
        {
            var now = _clock.UtcNow;
            if (request == null || string.IsNullOrWhiteSpace(request.WorkerInstanceId))
                return ResponseErrors.Error<RegisterWorkerResponseDto>(BimBridgeErrorCodes.InvalidRequest, "WorkerInstanceId is required.");
            if (request.ProtocolVersion != BimBridgeProtocol.Version)
                return ResponseErrors.Error<RegisterWorkerResponseDto>(BimBridgeErrorCodes.UnsupportedProtocolVersion, "Unsupported protocol version.");

            lock (_gate)
            {
                var activeCount = _workers.Values.Count(worker => !worker.IsStale && worker.State != BimWorkerStates.Unregistered);
                var replacing = _workerInstances.TryGetValue(request.WorkerInstanceId, out var oldRegistration);
                if (!replacing && activeCount >= _options.Workers.MaxRegisteredWorkers)
                    return ResponseErrors.Error<RegisterWorkerResponseDto>(BimBridgeErrorCodes.WorkerLimitReached, "Worker limit has been reached.", true);

                if (replacing && _workers.TryGetValue(oldRegistration, out var oldWorker))
                {
                    oldWorker.IsStale = true;
                    oldWorker.State = BimWorkerStates.Unregistered;
                    if (oldWorker.CurrentTaskId.HasValue && _tasks.TryGetValue(oldWorker.CurrentTaskId.Value, out var oldTask))
                        AbandonLocked(oldTask, oldWorker, "Worker registered a new session.");
                }

                var worker = new WorkerRecord
                {
                    RegistrationId = Guid.NewGuid(),
                    WorkerInstanceId = request.WorkerInstanceId,
                    MachineName = request.MachineName,
                    ExtensionVersion = request.ExtensionVersion,
                    BimSdkVersion = request.BimSdkVersion,
                    Capabilities = ToSet(request.Capabilities),
                    Models = ToSet(request.AvailableModelKeys),
                    State = BimWorkerStates.Registered,
                    RegisteredAtUtc = now,
                    LastSeenAtUtc = now
                };
                _workers.Add(worker.RegistrationId, worker);
                _workerInstances[worker.WorkerInstanceId] = worker.RegistrationId;
                _log.Info("Worker registered: " + worker.RegistrationId + ", instance=" + worker.WorkerInstanceId + ", machine=" + worker.MachineName);
                return new RegisterWorkerResponseDto
                {
                    RegistrationId = worker.RegistrationId,
                    ServerTimeUtc = now,
                    HeartbeatIntervalSeconds = _options.Workers.HeartbeatIntervalSeconds,
                    AssignmentLeaseSeconds = _options.Tasks.AssignmentLeaseSeconds,
                    LongPollingTimeoutSeconds = _options.Workers.LongPollingTimeoutSeconds,
                    ResultChunkSizeBytes = _options.Results.ChunkSizeBytes
                };
            }
        }

        public void Unregister(UnregisterWorkerRequestDto request)
        {
            if (request == null) return;
            lock (_gate)
            {
                if (!_workers.TryGetValue(request.RegistrationId, out var worker)) return;
                worker.IsStale = true;
                worker.State = BimWorkerStates.Unregistered;
                worker.LastSeenAtUtc = _clock.UtcNow;
                if (worker.CurrentTaskId.HasValue && _tasks.TryGetValue(worker.CurrentTaskId.Value, out var task))
                    AbandonLocked(task, worker, "Worker unregistered.");
                CancelWaiterLocked(worker.RegistrationId);
                DispatchWaitersLocked();
            }
        }

        public BimTaskAcceptedDto Submit(SubmitBimTaskRequestDto request)
        {
            var acceptedStarted=System.Diagnostics.Stopwatch.GetTimestamp();
            var now = _clock.UtcNow;
            if (request == null || request.ClientRequestId == Guid.Empty || string.IsNullOrWhiteSpace(request.OperationCode))
                return ResponseErrors.Error<BimTaskAcceptedDto>(BimBridgeErrorCodes.InvalidRequest, "ClientRequestId and OperationCode are required.");
            if (request.ProtocolVersion != BimBridgeProtocol.Version)
                return ResponseErrors.Error<BimTaskAcceptedDto>(BimBridgeErrorCodes.UnsupportedProtocolVersion, "Unsupported protocol version.");
            if ((request.Payload?.Length ?? 0) > _options.Tasks.MaxPayloadBytes)
                return ResponseErrors.Error<BimTaskAcceptedDto>(BimBridgeErrorCodes.PayloadTooLarge, "Task payload exceeds the configured limit.");
            if (request.ExpiresAtUtc.HasValue && request.ExpiresAtUtc.Value <= now)
                return ResponseErrors.Error<BimTaskAcceptedDto>(BimBridgeErrorCodes.TaskExpired, "Task has already expired.");

            lock (_gate)
            {
                if (_clientRequests.TryGetValue(request.ClientRequestId, out var existingId) && _tasks.TryGetValue(existingId, out var existing))
                    return Accepted(existing);
                if (_tasks.Values.Count(task => task.State == BimTaskStates.Queued) >= _options.Tasks.QueueCapacity)
                    return ResponseErrors.Error<BimTaskAcceptedDto>(BimBridgeErrorCodes.QueueFull, "Task queue is full.", true);

                var task = new TaskRecord
                {
                    TaskId = Guid.NewGuid(),
                    ClientRequestId = request.ClientRequestId,
                    OperationCode = request.OperationCode,
                    RequiredCapabilities = request.RequiredCapabilities ?? Array.Empty<string>(),
                    TargetModelKey = request.TargetModelKey,
                    PreferredWorkerInstanceId = request.PreferredWorkerInstanceId,
                    Priority = request.Priority,
                    PayloadContentType = request.PayloadContentType,
                    PayloadEncoding = request.PayloadEncoding,
                    Payload = request.Payload ?? Array.Empty<byte>(),
                    CanRetry = request.CanRetry,
                    MaxAttempts = request.MaxAttempts > 0 ? request.MaxAttempts : _options.Tasks.DefaultMaxAttempts,
                    ExpiresAtUtc = request.ExpiresAtUtc ?? now.AddMinutes(_options.Tasks.DefaultTaskTtlMinutes),
                    State = BimTaskStates.Queued,
                    CreatedAtUtc = now,
                    QueuedAtTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                };
                _tasks.Add(task.TaskId, task);
                _clientRequests.Add(task.ClientRequestId, task.TaskId);
                Interlocked.Increment(ref _submitted);
                DispatchWaitersLocked();
                task.TaskAcceptanceWallMs=(System.Diagnostics.Stopwatch.GetTimestamp()-acceptedStarted)*
                    1000d/System.Diagnostics.Stopwatch.Frequency;
                return Accepted(task);
            }
        }

        public async Task<WaitForTaskResponseDto> WaitForTaskAsync(WaitForTaskRequestDto request, CancellationToken sessionCancellation)
        {
            if (request == null)
                return ResponseErrors.Error<WaitForTaskResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");

            Waiter waiter;
            lock (_gate)
            {
                if (!TryGetActiveWorkerLocked(request.RegistrationId, out var worker, out var error))
                    return ResponseErrors.Error<WaitForTaskResponseDto>(error.Code, error.Message, error.IsRetryable);
                if (worker.CurrentTaskId.HasValue)
                    return ResponseErrors.Error<WaitForTaskResponseDto>(BimBridgeErrorCodes.WorkerBusy, "Worker already has an active task.");
                if (_waiters.ContainsKey(worker.RegistrationId))
                    return ResponseErrors.Error<WaitForTaskResponseDto>(BimBridgeErrorCodes.WorkerAlreadyWaiting, "Worker already has an active long-poll request.");

                worker.LastSeenAtUtc = _clock.UtcNow;
                worker.State = BimWorkerStates.Waiting;
                var assignment = TryAssignLocked(worker);
                if (assignment != null)
                    return AssignmentResponse(assignment);

                waiter = new Waiter
                {
                    RegistrationId = worker.RegistrationId,
                    Completion = new TaskCompletionSource<WaitForTaskResponseDto>(TaskCreationOptions.RunContinuationsAsynchronously)
                };
                _waiters.Add(worker.RegistrationId, waiter);
            }

            var requestedTimeout = request.WaitTimeoutSeconds > 0 ? request.WaitTimeoutSeconds : _options.Workers.LongPollingTimeoutSeconds;
            var timeout = Math.Min(requestedTimeout, _options.Workers.LongPollingTimeoutSeconds);
            using (var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(sessionCancellation, _shutdown.Token))
            {
                var delay = Task.Delay(TimeSpan.FromSeconds(timeout), timeoutSource.Token);
                var winner = await Task.WhenAny(waiter.Completion.Task, delay).ConfigureAwait(false);
                if (winner == waiter.Completion.Task)
                {
                    timeoutSource.Cancel();
                    return await waiter.Completion.Task.ConfigureAwait(false);
                }
            }

            lock (_gate)
            {
                if (waiter.Completion.Task.IsCompleted)
                    return waiter.Completion.Task.GetAwaiter().GetResult();
                _waiters.Remove(waiter.RegistrationId);
                if (_workers.TryGetValue(waiter.RegistrationId, out var worker) && !worker.CurrentTaskId.HasValue && !worker.IsStale)
                {
                    worker.State = BimWorkerStates.Ready;
                    worker.LastSeenAtUtc = _clock.UtcNow;
                }
                return new WaitForTaskResponseDto { HasTask = false, ServerTimeUtc = _clock.UtcNow };
            }
        }

        public WorkerHeartbeatResponseDto Heartbeat(WorkerHeartbeatRequestDto request)
        {
            var now = _clock.UtcNow;
            if (request == null)
                return ResponseErrors.Error<WorkerHeartbeatResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");
            lock (_gate)
            {
                if (!TryGetActiveWorkerLocked(request.RegistrationId, out var worker, out var error))
                {
                    var response = ResponseErrors.Error<WorkerHeartbeatResponseDto>(error.Code, error.Message, error.IsRetryable);
                    response.ServerTimeUtc = now;
                    response.RegistrationIsValid = false;
                    return response;
                }

                worker.LastSeenAtUtc = now;
                worker.Capabilities = request.Capabilities == null ? worker.Capabilities : ToSet(request.Capabilities);
                worker.Models = request.AvailableModelKeys == null ? worker.Models : ToSet(request.AvailableModelKeys);
                worker.StatusMessage = Limit(request.StatusMessage, 1024);

                TaskRecord task = null;
                if (worker.CurrentTaskId.HasValue)
                {
                    _tasks.TryGetValue(worker.CurrentTaskId.Value, out task);
                    if (task == null || request.CurrentTaskId != task.TaskId || request.AssignmentId != task.AssignmentId)
                        return HeartbeatError(now, BimBridgeErrorCodes.AssignmentMismatch, "Heartbeat assignment does not match the active task.");
                    if (!task.LeaseExpiresAtUtc.HasValue || task.LeaseExpiresAtUtc.Value <= now)
                        return HeartbeatError(now, BimBridgeErrorCodes.LeaseExpired, "Assignment lease has expired.");
                    if (task.State == BimTaskStates.Running || task.State == BimTaskStates.CancelRequested)
                        task.LeaseExpiresAtUtc = now.AddSeconds(_options.Tasks.AssignmentLeaseSeconds);
                    worker.State = BimWorkerStates.Busy;
                }
                else
                {
                    worker.State = string.Equals(request.State, BimWorkerStates.Degraded, StringComparison.Ordinal)
                        ? BimWorkerStates.Degraded
                        : (_waiters.ContainsKey(worker.RegistrationId) ? BimWorkerStates.Waiting : BimWorkerStates.Ready);
                }

                return new WorkerHeartbeatResponseDto
                {
                    ServerTimeUtc = now,
                    RegistrationIsValid = true,
                    CancellationRequested = task != null && task.State == BimTaskStates.CancelRequested,
                    LeaseExpiresAtUtc = task?.LeaseExpiresAtUtc
                };
            }
        }

        public StartTaskResponseDto StartTask(StartTaskRequestDto request)
        {
            if (request == null)
                return ResponseErrors.Error<StartTaskResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");
            lock (_gate)
            {
                if (!TryValidateAssignmentLocked(request.RegistrationId, request.TaskId, request.AssignmentId, out var worker, out var task, out var error))
                    return ResponseErrors.Error<StartTaskResponseDto>(error.Code, error.Message, error.IsRetryable);
                if (task.State == BimTaskStates.CancelRequested || task.State == BimTaskStates.Cancelled)
                    return ResponseErrors.Error<StartTaskResponseDto>(BimBridgeErrorCodes.TaskCancelled, "Task has been cancelled.");
                if (task.State != BimTaskStates.Assigned)
                    return ResponseErrors.Error<StartTaskResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Task is not in ASSIGNED state.");
                task.State = BimTaskStates.Running;
                task.StartedAtUtc = _clock.UtcNow;
                task.LeaseExpiresAtUtc = _clock.UtcNow.AddSeconds(_options.Tasks.AssignmentLeaseSeconds);
                worker.State = BimWorkerStates.Busy;
                return new StartTaskResponseDto { State = task.State, LeaseExpiresAtUtc = task.LeaseExpiresAtUtc.Value };
            }
        }

        public BimTaskStatusDto GetTaskStatus(Guid taskId)
        {
            lock (_gate)
            {
                if (!_tasks.TryGetValue(taskId, out var task))
                    return ResponseErrors.Error<BimTaskStatusDto>(BimBridgeErrorCodes.TaskNotFound, "Task was not found.");
                return new BimTaskStatusDto
                {
                    TaskId = task.TaskId,
                    State = task.State,
                    AttemptNumber = task.AttemptNumber,
                    AssignmentId = task.AssignmentId,
                    CreatedAtUtc = task.CreatedAtUtc,
                    AssignedAtUtc = task.AssignedAtUtc,
                    StartedAtUtc = task.StartedAtUtc,
                    CompletedAtUtc = task.CompletedAtUtc,
                    PreparedAtUtc = task.PreparedAtUtc,
                    TaskError = task.Error,
                    WorkerInstanceId=task.ExecutionWorkerInstanceId,
                    ProcessingWallMs=task.WorkerProcessingWallMs,
                    SpatialPerformance=task.SpatialPerformance,
                    Diagnostics=task.Diagnostics,
                    QueueWaitMs=task.QueueWaitMs,
                    AttemptQueueWaitMs=task.AttemptQueueWaitMs,
                    PriorAttempts=task.PriorAttempts.ToArray(),
                    TaskAcceptanceWallMs=task.TaskAcceptanceWallMs,
                    AssignmentWallMs=task.AssignmentWallMs,
                    ResultPreparationWallMs=task.ResultPreparationWallMs,
                    HashVerificationMs=task.HashVerificationMs,
                    ArtifactPublicationMs=task.ArtifactPublicationMs,
                    UploadWallMs=task.UploadWallMs,
                    UploadRetryCount=task.UploadRetryCount,
                    UploadRetryWaitMs=task.UploadRetryWaitMs
                };
            }
        }

        public CancelBimTaskResponseDto Cancel(Guid taskId)
        {
            lock (_gate)
            {
                if (!_tasks.TryGetValue(taskId, out var task))
                    return ResponseErrors.Error<CancelBimTaskResponseDto>(BimBridgeErrorCodes.TaskNotFound, "Task was not found.");
                if (IsTerminal(task.State))
                    return new CancelBimTaskResponseDto { TaskId = taskId, State = task.State };

                if (task.State == BimTaskStates.Running)
                {
                    task.State = BimTaskStates.CancelRequested;
                    if(_results is FileSystemBimResultStore cancelStore)cancelStore.SaveTaskState(task);
                    return new CancelBimTaskResponseDto { TaskId = taskId, State = task.State, CancellationRequested = true };
                }

                task.State = BimTaskStates.Cancelled;
                task.CompletedAtUtc = _clock.UtcNow;
                if(_results is FileSystemBimResultStore cancelledStore)cancelledStore.SaveTaskState(task);
                ReleaseWorkerLocked(task);
                Interlocked.Increment(ref _cancelled);
                DispatchWaitersLocked();
                return new CancelBimTaskResponseDto { TaskId = taskId, State = task.State };
            }
        }

        public FailTaskResponseDto Fail(FailTaskRequestDto request)
        {
            if (request == null)
                return ResponseErrors.Error<FailTaskResponseDto>(BimBridgeErrorCodes.InvalidRequest, "Request is required.");
            lock (_gate)
            {
                if (!TryValidateAssignmentLocked(request.RegistrationId, request.TaskId, request.AssignmentId, out _, out var task, out var error))
                    return ResponseErrors.Error<FailTaskResponseDto>(error.Code, error.Message, error.IsRetryable);

                if (request.CancellationAcknowledged || task.State == BimTaskStates.CancelRequested)
                {
                    task.State = BimTaskStates.Cancelled;
                    task.CompletedAtUtc = _clock.UtcNow;
                    task.Error = request.Error;
                    if(_results is FileSystemBimResultStore cancelledStore)cancelledStore.SaveTaskState(task);
                    ReleaseWorkerLocked(task);
                    Interlocked.Increment(ref _cancelled);
                    DispatchWaitersLocked();
                    return new FailTaskResponseDto { State = task.State };
                }

                task.Error = request.Error ?? new BimTaskErrorDto { Code = BimBridgeErrorCodes.InternalError, Message = "Worker reported an unspecified failure." };
                var retry = task.Error.IsRetryable && CanRetryLocked(task);
                ReleaseWorkerLocked(task);
                if (retry)
                {
                    RecordPreviousAttempt(task,BimTaskStates.Failed,_clock.UtcNow);
                    ResetForRetryLocked(task);
                    Interlocked.Increment(ref _retried);
                }
                else
                {
                    task.State = BimTaskStates.Failed;
                    task.CompletedAtUtc = _clock.UtcNow;
                    if(_results is FileSystemBimResultStore)TryPersistTelemetryState(task);
                    Interlocked.Increment(ref _failed);
                }
                DispatchWaitersLocked();
                return new FailTaskResponseDto { State = task.State, Requeued = retry };
            }
        }

        public async Task<BimBridgeHealthDto> GetHealthAsync(CancellationToken cancellationToken)
        {
            int registered, ready, busy, offline, queued, running, uploads;
            lock (_gate)
            {
                registered = _workers.Values.Count(worker => !worker.IsStale && worker.State != BimWorkerStates.Unregistered);
                ready = _workers.Values.Count(worker => worker.State == BimWorkerStates.Ready || worker.State == BimWorkerStates.Waiting);
                busy = _workers.Values.Count(worker => worker.State == BimWorkerStates.Busy);
                offline = _workers.Values.Count(worker => worker.State == BimWorkerStates.Offline);
                queued = _tasks.Values.Count(task => task.State == BimTaskStates.Queued);
                running = _tasks.Values.Count(task => task.State == BimTaskStates.Running || task.State == BimTaskStates.CancelRequested);
                uploads = _uploads.Count;
            }

            try
            {
                var storedBytes = await _results.GetStoredBytesAsync(cancellationToken).ConfigureAwait(false);
                var state = ready == 0 || queued >= _options.Tasks.QueueCapacity * 9 / 10 ? "DEGRADED" : "READY";
                return new BimBridgeHealthDto
                {
                    State = state,
                    ProtocolVersion = BimBridgeProtocol.Version,
                    SupportedProtocolVersions = BimBridgeProtocol.SupportedVersions,
                    RegisteredWorkers = registered,
                    ReadyWorkers = ready,
                    BusyWorkers = busy,
                    OfflineWorkers = offline,
                    QueuedTasks = queued,
                    RunningTasks = running,
                    PendingResultUploads = uploads,
                    Capabilities = new[] { ArtifactProtocol.ResumableUpload, ArtifactProtocol.ChunkDownload },
                    StoredResultBytes = storedBytes,
                    Message = ready == 0 ? "No ready workers are registered." : "BIM Bridge is operational."
                };
            }
            catch (Exception exception)
            {
                _log.Error("Health check failed.", exception);
                var response = ResponseErrors.Error<BimBridgeHealthDto>(BimBridgeErrorCodes.InternalError, "Result storage is unavailable.", true);
                response.State = "DEGRADED";
                response.ProtocolVersion = BimBridgeProtocol.Version;
                response.SupportedProtocolVersions = BimBridgeProtocol.SupportedVersions;
                return response;
            }
        }

        public List<WorkerStatusDto> GetWorkers()
        {
            lock (_gate)
            {
                return _workers.Values.Select(worker => new WorkerStatusDto
                {
                    RegistrationId = worker.RegistrationId,
                    WorkerInstanceId = worker.WorkerInstanceId,
                    MachineName = worker.MachineName,
                    State = worker.State,
                    RegisteredAtUtc = worker.RegisteredAtUtc,
                    LastSeenAtUtc = worker.LastSeenAtUtc,
                    CurrentTaskId = worker.CurrentTaskId,
                    Capabilities = worker.Capabilities.ToArray(),
                    AvailableModelKeys = worker.Models.ToArray(),
                    StatusMessage = worker.StatusMessage
                }).ToList();
            }
        }

        public BimBridgeStatisticsDto GetStatistics()
        {
            lock (_gate)
            {
                var result = new BimBridgeStatisticsDto
                {
                    SubmittedTasks = Interlocked.Read(ref _submitted),
                    CompletedTasks = Interlocked.Read(ref _completed),
                    FailedTasks = Interlocked.Read(ref _failed),
                    CancelledTasks = Interlocked.Read(ref _cancelled),
                    RetriedTasks = Interlocked.Read(ref _retried),
                    UploadedBytes = Interlocked.Read(ref _uploadedBytes),
                    ActiveLongPolls = _waiters.Count
                };
                AddParallelStatistics(result);
                return result;
            }
        }

        internal void RunLeaseCheck()
        {
            lock (_gate)
            {
                var now = _clock.UtcNow;
                foreach (var worker in _workers.Values.Where(value => !value.IsStale).ToList())
                {
                    if (worker.State != BimWorkerStates.Unregistered &&
                        now - worker.LastSeenAtUtc > TimeSpan.FromSeconds(_options.Workers.WorkerOfflineAfterSeconds))
                    {
                        worker.State = BimWorkerStates.Offline;
                        CancelWaiterLocked(worker.RegistrationId);
                        if (worker.CurrentTaskId.HasValue && _tasks.TryGetValue(worker.CurrentTaskId.Value, out var task))
                            AbandonLocked(task, worker, "Worker heartbeat expired.");
                    }
                }

                foreach (var task in _tasks.Values.ToList())
                {
                    if (task.State == BimTaskStates.Queued && task.ExpiresAtUtc.HasValue && task.ExpiresAtUtc <= now)
                    {
                        task.State = BimTaskStates.Expired;
                        task.CompletedAtUtc = now;
                    }
                    else if ((task.State == BimTaskStates.Assigned || task.State == BimTaskStates.Running || task.State == BimTaskStates.CancelRequested) &&
                             task.LeaseExpiresAtUtc.HasValue && task.LeaseExpiresAtUtc <= now)
                    {
                        _workers.TryGetValue(task.RegistrationId ?? Guid.Empty, out var worker);
                        AbandonLocked(task, worker, "Assignment lease expired.");
                    }
                }
                DispatchWaitersLocked();
            }
        }

        internal async Task RunCleanupAsync(CancellationToken cancellationToken)
        {
            lock(_gate) if(_results is FileSystemBimResultStore durableStore)
            {
                durableStore.CleanupReservations(_clock.UtcNow,TimeSpan.FromMinutes(_options.Results.ReservationTtlMinutes));
                durableStore.CleanupTaskStates(_clock.UtcNow,TimeSpan.FromMinutes(_options.Results.ReservationTtlMinutes));
            }
            List<Guid> resultTasks;
            List<Guid> staleUploads;
            lock (_gate)
            {
                var now = _clock.UtcNow;
                resultTasks = _tasks.Values.Where(task => ShouldDeleteTask(task, now)).Select(task => task.TaskId).ToList();
                staleUploads = _uploads.Values
                    .Where(upload => now - upload.CreatedAtUtc > TimeSpan.FromMinutes(_options.Results.IncompleteUploadTtlMinutes))
                    .Select(upload => upload.UploadId)
                    .ToList();

                foreach (var taskId in resultTasks)
                {
                    if (_tasks.TryGetValue(taskId, out var task))
                        _clientRequests.Remove(task.ClientRequestId);
                    _tasks.Remove(taskId);
                }
                foreach (var uploadId in staleUploads)
                {
                    if (_uploads.TryGetValue(uploadId, out var upload) && _tasks.TryGetValue(upload.TaskId, out var task))
                    {
                        task.UploadId = null;
                        if (task.State == ArtifactProtocol.ResultPrepared || task.State == ArtifactProtocol.ResultUploading)
                        { task.State = BimTaskStates.Expired; task.CompletedAtUtc = now; }
                    }
                    _uploads.Remove(uploadId);
                }

                var staleWorkers = _workers.Values.Where(worker =>
                    (worker.State == BimWorkerStates.Offline || worker.State == BimWorkerStates.Unregistered || worker.IsStale) &&
                    now - worker.LastSeenAtUtc > TimeSpan.FromMinutes(_options.Maintenance.WorkerRegistrationTtlMinutes)).ToList();
                foreach (var worker in staleWorkers)
                {
                    _workers.Remove(worker.RegistrationId);
                    if (_workerInstances.TryGetValue(worker.WorkerInstanceId, out var current) && current == worker.RegistrationId)
                        _workerInstances.Remove(worker.WorkerInstanceId);
                }
            }

            foreach (var taskId in resultTasks)
                await _results.DeleteAsync(taskId, cancellationToken).ConfigureAwait(false);
            foreach (var uploadId in staleUploads)
                await _results.DeleteUploadAsync(uploadId, cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _shutdown.Cancel();
            lock (_gate)
            {
                foreach (var waiter in _waiters.Values)
                    waiter.Completion.TrySetCanceled();
                _waiters.Clear();
            }
            _shutdown.Dispose();
        }

        private async Task LeaseLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_options.Maintenance.LeaseCheckIntervalSeconds), cancellationToken).ConfigureAwait(false);
                    RunLeaseCheck();
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
                catch (Exception exception) { _log.Error("Lease maintenance iteration failed.", exception); }
            }
        }

        private async Task CleanupLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_options.Maintenance.CleanupIntervalSeconds), cancellationToken).ConfigureAwait(false);
                    await RunCleanupAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
                catch (Exception exception) { _log.Error("Cleanup maintenance iteration failed.", exception); }
            }
        }

        private BimTaskAssignmentDto TryAssignLocked(WorkerRecord worker)
        {
            if (worker.IsStale || worker.CurrentTaskId.HasValue ||
                (worker.State != BimWorkerStates.Waiting && worker.State != BimWorkerStates.Ready))
                return null;
            var assignmentStarted=System.Diagnostics.Stopwatch.GetTimestamp();
            var now = _clock.UtcNow;
            var task = _tasks.Values
                .Where(candidate => candidate.State == BimTaskStates.Queued &&
                                    (!candidate.ExpiresAtUtc.HasValue || candidate.ExpiresAtUtc > now) &&
                                    candidate.RequiredCapabilities.All(worker.Capabilities.Contains) &&
                                    (string.IsNullOrWhiteSpace(candidate.TargetModelKey) || worker.Models.Contains(candidate.TargetModelKey)))
                .OrderByDescending(candidate => string.Equals(candidate.PreferredWorkerInstanceId, worker.WorkerInstanceId, StringComparison.Ordinal))
                .ThenByDescending(candidate => candidate.Priority)
                .ThenBy(candidate => candidate.CreatedAtUtc)
                .FirstOrDefault();
            if (task == null) return null;

            task.State = BimTaskStates.Assigned;
            if(task.QueuedAtTicks!=0)
            {
                var elapsed=System.Diagnostics.Stopwatch.GetTimestamp()-task.QueuedAtTicks;
                task.AttemptQueueWaitMs=elapsed*1000d/System.Diagnostics.Stopwatch.Frequency;
                task.QueueWaitMs=(task.QueueWaitMs??0)+task.AttemptQueueWaitMs.Value;
                task.QueuedAtTicks=0;
            }
            task.ExecutionWorkerInstanceId=worker.WorkerInstanceId;
            task.PreparedAtUtc=null;
            _assignmentsByWorker.TryGetValue(worker.WorkerInstanceId,out var assignments);
            _assignmentsByWorker[worker.WorkerInstanceId]=assignments+1;
            task.AttemptNumber++;
            task.Error=null;
            task.RegistrationId = worker.RegistrationId;
            task.AssignmentId = Guid.NewGuid();
            task.AssignedAtUtc = now;
            task.StartedAtUtc = null;
            task.LeaseExpiresAtUtc = now.AddSeconds(_options.Tasks.StartTaskTimeoutSeconds);
            worker.CurrentTaskId = task.TaskId;
            worker.State = BimWorkerStates.Busy;
            task.AssignmentWallMs=(System.Diagnostics.Stopwatch.GetTimestamp()-assignmentStarted)*
                1000d/System.Diagnostics.Stopwatch.Frequency;
            return new BimTaskAssignmentDto
            {
                TaskId = task.TaskId,
                AssignmentId = task.AssignmentId.Value,
                AttemptNumber = task.AttemptNumber,
                LeaseExpiresAtUtc = task.LeaseExpiresAtUtc.Value,
                OperationCode = task.OperationCode,
                TargetModelKey = task.TargetModelKey,
                PayloadContentType = task.PayloadContentType,
                PayloadEncoding = task.PayloadEncoding,
                Payload = task.Payload
            };
        }

        private void DispatchWaitersLocked()
        {
            foreach (var waiter in _waiters.Values.ToList())
            {
                if (!_workers.TryGetValue(waiter.RegistrationId, out var worker)) continue;
                var assignment = TryAssignLocked(worker);
                if (assignment == null) continue;
                _waiters.Remove(waiter.RegistrationId);
                waiter.Completion.TrySetResult(AssignmentResponse(assignment));
            }
        }

        private bool TryValidateAssignmentLocked(
            Guid registrationId,
            Guid taskId,
            Guid assignmentId,
            out WorkerRecord worker,
            out TaskRecord task,
            out BimBridgeErrorDto error)
        {
            task = null;
            if (!TryGetActiveWorkerLocked(registrationId, out worker, out error))
                return false;
            if (!_tasks.TryGetValue(taskId, out task))
            {
                error = ResponseErrors.Create(BimBridgeErrorCodes.TaskNotFound, "Task was not found.");
                return false;
            }
            if (task.AssignmentId != assignmentId || task.RegistrationId != registrationId)
            {
                error = ResponseErrors.Create(BimBridgeErrorCodes.LeaseLost, "Assignment is no longer current.");
                return false;
            }
            if (!task.LeaseExpiresAtUtc.HasValue || task.LeaseExpiresAtUtc <= _clock.UtcNow)
            {
                error = ResponseErrors.Create(BimBridgeErrorCodes.LeaseExpired, "Assignment lease has expired.", true);
                return false;
            }
            error = null;
            return true;
        }

        private bool TryGetActiveWorkerLocked(Guid registrationId, out WorkerRecord worker, out BimBridgeErrorDto error)
        {
            if (!_workers.TryGetValue(registrationId, out worker) || worker.IsStale || worker.State == BimWorkerStates.Unregistered)
            {
                error = ResponseErrors.Create(BimBridgeErrorCodes.WorkerNotRegistered, "Worker registration is not valid.");
                return false;
            }
            if (worker.State == BimWorkerStates.Offline)
            {
                error = ResponseErrors.Create(BimBridgeErrorCodes.WorkerOffline, "Worker is offline.", true);
                return false;
            }
            error = null;
            return true;
        }

        private void AbandonLocked(TaskRecord task, WorkerRecord worker, string reason)
        {
            if (task == null || IsTerminal(task.State)) return;
            if (task.State == ArtifactProtocol.ResultPrepared || task.State == ArtifactProtocol.ResultUploading)
            { if (worker != null) worker.CurrentTaskId = null; return; }
            if (task.State == BimTaskStates.CancelRequested)
            {
                task.State = BimTaskStates.Cancelled;
                task.CompletedAtUtc = _clock.UtcNow;
                if(_results is FileSystemBimResultStore cancelledStore)cancelledStore.SaveTaskState(task);
                Interlocked.Increment(ref _cancelled);
            }
            else
            {
                task.State = BimTaskStates.Abandoned;
                task.Error = new BimTaskErrorDto { Code = BimBridgeErrorCodes.LeaseExpired, Message = reason, IsRetryable = true };
                if (CanRetryLocked(task))
                {
                    RecordPreviousAttempt(task,BimTaskStates.Abandoned,_clock.UtcNow);
                    ResetForRetryLocked(task);
                    Interlocked.Increment(ref _retried);
                }
                else
                {
                    task.State = BimTaskStates.Failed;
                    task.CompletedAtUtc = _clock.UtcNow;
                    if(_results is FileSystemBimResultStore)TryPersistTelemetryState(task);
                    Interlocked.Increment(ref _failed);
                }
            }
            if (worker != null)
            {
                worker.CurrentTaskId = null;
                if (worker.State != BimWorkerStates.Offline && !worker.IsStale)
                    worker.State = BimWorkerStates.Degraded;
            }
        }

        private bool CanRetryLocked(TaskRecord task)
        {
            return task.CanRetry && task.AttemptNumber < task.MaxAttempts &&
                (_retryableOperations.Contains(task.OperationCode) || IsPinnedReadOnlyExport(task));
        }

        private static bool IsPinnedReadOnlyExport(TaskRecord task)
        {
            if(task.OperationCode!=ArtifactProtocol.ExportSpatial && task.OperationCode!=ArtifactProtocol.ExportElements)return false;
            if(!string.IsNullOrEmpty(task.PayloadEncoding)&&task.PayloadEncoding!="identity"&&task.PayloadEncoding!="utf-8"&&task.PayloadEncoding!="utf8")return false;
            try
            {
                var payload=Newtonsoft.Json.Linq.JObject.Parse(System.Text.Encoding.UTF8.GetString(task.Payload??Array.Empty<byte>()));
                var version=payload["ModelVersionUtc"];
                return version!=null&&DateTimeOffset.TryParse(version.ToString(),System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.RoundtripKind,out _);
            }
            catch(Newtonsoft.Json.JsonException){return false;}
        }

        private static void ResetForRetryLocked(TaskRecord task)
        {
            task.State = BimTaskStates.Queued;
            task.QueuedAtTicks=System.Diagnostics.Stopwatch.GetTimestamp();
            task.AttemptQueueWaitMs=null;
            task.AssignmentWallMs=null;
            task.ResultPreparationWallMs=null;
            task.UploadRetryCount=null;
            task.UploadRetryWaitMs=null;
            task.ExecutionWorkerInstanceId=null;
            task.WorkerProcessingWallMs=null;
            task.SpatialPerformance=null;
            task.Diagnostics=null;
            task.RegistrationId = null;
            task.AssignmentId = null;
            task.LeaseExpiresAtUtc = null;
            task.AssignedAtUtc = null;
            task.StartedAtUtc = null;
            task.UploadId = null;
        }

        private static void RecordPreviousAttempt(TaskRecord task,string state,DateTime endedAtUtc)
        {
            if(!task.AssignmentId.HasValue)return;
            task.PriorAttempts.Add(new TaskAttemptTelemetryDto {
                AttemptNumber=task.AttemptNumber,AssignmentId=task.AssignmentId.Value,
                WorkerInstanceId=task.ExecutionWorkerInstanceId,AssignedAtUtc=task.AssignedAtUtc,
                StartedAtUtc=task.StartedAtUtc,EndedAtUtc=endedAtUtc,QueueWaitMs=task.AttemptQueueWaitMs,
                AssignmentWallMs=task.AssignmentWallMs,
                ResultPreparationWallMs=task.ResultPreparationWallMs,
                UploadRetryCount=task.UploadRetryCount,UploadRetryWaitMs=task.UploadRetryWaitMs,
                TaskError=task.Error,State=state,ProcessingWallMs=task.WorkerProcessingWallMs,
                SpatialPerformance=task.SpatialPerformance,Diagnostics=task.Diagnostics
            });
        }

        private void TryPersistTelemetryState(TaskRecord task)
        {
            if(!(_results is FileSystemBimResultStore store))return;
            try { store.SaveTaskState(task); }
            catch(Exception error) { _log.Warning("Task telemetry checkpoint failed for " + task.TaskId + ": " + error.Message); }
        }

        private void ReleaseWorkerLocked(TaskRecord task)
        {
            if (task.RegistrationId.HasValue && _workers.TryGetValue(task.RegistrationId.Value, out var worker))
            {
                worker.CurrentTaskId = null;
                if (!worker.IsStale && worker.State != BimWorkerStates.Offline)
                    worker.State = BimWorkerStates.Ready;
            }
            task.RegistrationId = null;
            task.LeaseExpiresAtUtc = null;
        }

        private void CancelWaiterLocked(Guid registrationId)
        {
            if (_waiters.TryGetValue(registrationId, out var waiter))
            {
                _waiters.Remove(registrationId);
                waiter.Completion.TrySetResult(new WaitForTaskResponseDto { HasTask = false, ServerTimeUtc = _clock.UtcNow });
            }
        }

        private bool ShouldDeleteTask(TaskRecord task, DateTime now)
        {
            if (!task.CompletedAtUtc.HasValue) return false;
            var age = now - task.CompletedAtUtc.Value;
            if (task.State == BimTaskStates.Completed)
                return age > TimeSpan.FromMinutes(_options.Results.CompletedResultTtlMinutes);
            if (task.State == BimTaskStates.Cancelled)
                return age > TimeSpan.FromMinutes(_options.Maintenance.CancelledTaskTtlMinutes);
            return IsTerminal(task.State) && age > TimeSpan.FromMinutes(_options.Maintenance.FailedTaskTtlMinutes);
        }

        private static HashSet<string> ToSet(IEnumerable<string> values)
        {
            return new HashSet<string>((values ?? Array.Empty<string>()).Where(value => !string.IsNullOrWhiteSpace(value)), StringComparer.Ordinal);
        }

        private static bool IsTerminal(string state)
        {
            return state == BimTaskStates.Completed || state == BimTaskStates.Failed ||
                   state == BimTaskStates.Cancelled || state == BimTaskStates.Expired;
        }

        private static string Limit(string value, int length)
        {
            if (string.IsNullOrEmpty(value) || value.Length <= length) return value;
            return value.Substring(0, length);
        }

        private static BimTaskAcceptedDto Accepted(TaskRecord task)
        {
            return new BimTaskAcceptedDto { TaskId = task.TaskId, State = task.State, CreatedAtUtc = task.CreatedAtUtc };
        }

        private WaitForTaskResponseDto AssignmentResponse(BimTaskAssignmentDto assignment)
        {
            return new WaitForTaskResponseDto { HasTask = true, Assignment = assignment, ServerTimeUtc = _clock.UtcNow };
        }

        private static WorkerHeartbeatResponseDto HeartbeatError(DateTime now, string code, string message)
        {
            var response = ResponseErrors.Error<WorkerHeartbeatResponseDto>(code, message);
            response.ServerTimeUtc = now;
            response.RegistrationIsValid = true;
            return response;
        }

        private BimResultMetadataDto MapMetadata(StoredResultMetadata metadata)
        {
            return new BimResultMetadataDto
            {
                TaskId = metadata.TaskId,
                ContentType = metadata.ContentType,
                ContentEncoding = metadata.ContentEncoding,
                TotalLength = metadata.TotalLength,
                TotalChunks = metadata.TotalChunks,
                ChunkSizeBytes = metadata.ChunkSizeBytes,
                Sha256 = metadata.Sha256,
                CreatedAtUtc = metadata.CreatedAtUtc,
                FileName = metadata.FileName,
                ResultKind = metadata.ResultKind,
                ExpiresAtUtc = metadata.CreatedAtUtc.AddMinutes(_options.Results.CompletedResultTtlMinutes)
            };
        }
    }
}
