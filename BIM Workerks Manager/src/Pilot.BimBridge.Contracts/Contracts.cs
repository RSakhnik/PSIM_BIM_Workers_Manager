using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Pilot.BimBridge.Contracts
{
    public static class BimBridgeProtocol
    {
        public const string Version = "1.0";
        public static readonly string[] SupportedVersions = { Version };
    }

    public static class BimWorkerStates
    {
        public const string Registered = "REGISTERED";
        public const string Ready = "READY";
        public const string Waiting = "WAITING";
        public const string Busy = "BUSY";
        public const string Degraded = "DEGRADED";
        public const string Offline = "OFFLINE";
        public const string Unregistered = "UNREGISTERED";
    }

    public static class BimTaskStates
    {
        public const string Queued = "QUEUED";
        public const string Assigned = "ASSIGNED";
        public const string Running = "RUNNING";
        public const string CancelRequested = "CANCEL_REQUESTED";
        public const string Completed = "COMPLETED";
        public const string Failed = "FAILED";
        public const string Cancelled = "CANCELLED";
        public const string Abandoned = "ABANDONED";
        public const string Expired = "EXPIRED";
    }

    public static class BimBridgeErrorCodes
    {
        public const string InvalidRequest = "INVALID_REQUEST";
        public const string UnsupportedProtocolVersion = "UNSUPPORTED_PROTOCOL_VERSION";
        public const string WorkerNotRegistered = "WORKER_NOT_REGISTERED";
        public const string WorkerOffline = "WORKER_OFFLINE";
        public const string WorkerAlreadyWaiting = "WORKER_ALREADY_WAITING";
        public const string WorkerBusy = "WORKER_BUSY";
        public const string WorkerLimitReached = "WORKER_LIMIT_REACHED";
        public const string CapabilityNotAvailable = "CAPABILITY_NOT_AVAILABLE";
        public const string ModelNotAvailable = "MODEL_NOT_AVAILABLE";
        public const string QueueFull = "QUEUE_FULL";
        public const string TaskNotFound = "TASK_NOT_FOUND";
        public const string TaskAlreadyCompleted = "TASK_ALREADY_COMPLETED";
        public const string TaskCancelled = "TASK_CANCELLED";
        public const string TaskExpired = "TASK_EXPIRED";
        public const string AssignmentNotFound = "ASSIGNMENT_NOT_FOUND";
        public const string AssignmentMismatch = "ASSIGNMENT_MISMATCH";
        public const string LeaseExpired = "LEASE_EXPIRED";
        public const string LeaseLost = "LEASE_LOST";
        public const string UploadNotFound = "UPLOAD_NOT_FOUND";
        public const string UploadIncomplete = "UPLOAD_INCOMPLETE";
        public const string ChunkAlreadyReceived = "CHUNK_ALREADY_RECEIVED";
        public const string ChunkOutOfRange = "CHUNK_OUT_OF_RANGE";
        public const string PayloadTooLarge = "PAYLOAD_TOO_LARGE";
        public const string ResultTooLarge = "RESULT_TOO_LARGE";
        public const string TooManyUploads = "TOO_MANY_UPLOADS";
        public const string InternalError = "INTERNAL_ERROR";
    }

    public sealed class BimBridgeErrorDto
    {
        public string Code { get; set; }
        public string Message { get; set; }
        public bool IsRetryable { get; set; }
    }

    public abstract class BimBridgeResponseDto
    {
        public BimBridgeErrorDto Error { get; set; }
        public bool IsSuccess => Error == null;
    }

    public interface IBimBridgeTaskApi
    {
        Task<BimTaskAcceptedDto> SubmitTaskAsync(SubmitBimTaskRequestDto request);
        Task<BimTaskStatusDto> GetTaskStatusAsync(Guid taskId);
        Task<BimResultMetadataDto> GetResultMetadataAsync(Guid taskId);
        Task<BimResultChunkDto> ReadResultChunkAsync(Guid taskId, int chunkIndex);
        Task<CancelBimTaskResponseDto> CancelTaskAsync(Guid taskId);
    }

    public interface IBimBridgeWorkerApi
    {
        Task<RegisterWorkerResponseDto> RegisterWorkerAsync(RegisterWorkerRequestDto request);
        Task UnregisterWorkerAsync(UnregisterWorkerRequestDto request);
        Task<WaitForTaskResponseDto> WaitForTaskAsync(WaitForTaskRequestDto request);
        Task<StartTaskResponseDto> StartTaskAsync(StartTaskRequestDto request);
        Task<WorkerHeartbeatResponseDto> SendHeartbeatAsync(WorkerHeartbeatRequestDto request);
        Task<BeginResultUploadResponseDto> BeginResultUploadAsync(BeginResultUploadRequestDto request);
        Task<UploadResultChunkResponseDto> UploadResultChunkAsync(UploadResultChunkRequestDto request);
        Task<CompleteTaskResponseDto> CompleteTaskAsync(CompleteTaskRequestDto request);
        Task<FailTaskResponseDto> FailTaskAsync(FailTaskRequestDto request);
    }

    public interface IBimBridgeDiagnosticsApi
    {
        Task<BimBridgeHealthDto> GetHealthAsync();
        Task<List<WorkerStatusDto>> GetWorkersAsync();
        Task<BimBridgeStatisticsDto> GetStatisticsAsync();
    }

    // The Pilot custom API transport requires a callback contract. BIM Bridge does
    // not invoke it: workers still receive all work through outbound long polling.
    public interface IBimBridgeClientCallback
    {
        void OnNotification(BimBridgeNotificationDto notification);
    }

    public sealed class BimBridgeNotificationDto
    {
        public string Code { get; set; }
        public string Message { get; set; }
    }

    public sealed class RegisterWorkerRequestDto
    {
        public string ProtocolVersion { get; set; }
        public string WorkerInstanceId { get; set; }
        public string MachineName { get; set; }
        public string ExtensionVersion { get; set; }
        public string BimSdkVersion { get; set; }
        public string[] Capabilities { get; set; }
        public string[] AvailableModelKeys { get; set; }
        public int MaxParallelTasks { get; set; }
    }

    public sealed class RegisterWorkerResponseDto : BimBridgeResponseDto
    {
        public Guid RegistrationId { get; set; }
        public DateTime ServerTimeUtc { get; set; }
        public int HeartbeatIntervalSeconds { get; set; }
        public int AssignmentLeaseSeconds { get; set; }
        public int LongPollingTimeoutSeconds { get; set; }
        public int ResultChunkSizeBytes { get; set; }
    }

    public sealed class UnregisterWorkerRequestDto
    {
        public Guid RegistrationId { get; set; }
    }

    public sealed class WorkerHeartbeatRequestDto
    {
        public Guid RegistrationId { get; set; }
        public string State { get; set; }
        public Guid? CurrentTaskId { get; set; }
        public Guid? AssignmentId { get; set; }
        public string[] AvailableModelKeys { get; set; }
        public string[] Capabilities { get; set; }
        public string StatusMessage { get; set; }
        public DateTime ClientTimeUtc { get; set; }
    }

    public sealed class WorkerHeartbeatResponseDto : BimBridgeResponseDto
    {
        public DateTime ServerTimeUtc { get; set; }
        public bool RegistrationIsValid { get; set; }
        public bool CancellationRequested { get; set; }
        public DateTime? LeaseExpiresAtUtc { get; set; }
    }

    public sealed class WaitForTaskRequestDto
    {
        public Guid RegistrationId { get; set; }
        public int WaitTimeoutSeconds { get; set; }
    }

    public sealed class WaitForTaskResponseDto : BimBridgeResponseDto
    {
        public bool HasTask { get; set; }
        public BimTaskAssignmentDto Assignment { get; set; }
        public DateTime ServerTimeUtc { get; set; }
    }

    public sealed class SubmitBimTaskRequestDto
    {
        public Guid ClientRequestId { get; set; }
        public string ProtocolVersion { get; set; }
        public string OperationCode { get; set; }
        public string[] RequiredCapabilities { get; set; }
        public string TargetModelKey { get; set; }
        public string PreferredWorkerInstanceId { get; set; }
        public int Priority { get; set; }
        public string PayloadContentType { get; set; }
        public string PayloadEncoding { get; set; }
        public byte[] Payload { get; set; }
        public bool CanRetry { get; set; }
        public int MaxAttempts { get; set; }
        public DateTime? ExpiresAtUtc { get; set; }
    }

    public sealed class BimTaskAcceptedDto : BimBridgeResponseDto
    {
        public Guid TaskId { get; set; }
        public string State { get; set; }
        public DateTime CreatedAtUtc { get; set; }
    }

    public sealed class BimTaskAssignmentDto
    {
        public Guid TaskId { get; set; }
        public Guid AssignmentId { get; set; }
        public int AttemptNumber { get; set; }
        public DateTime LeaseExpiresAtUtc { get; set; }
        public string OperationCode { get; set; }
        public string TargetModelKey { get; set; }
        public string PayloadContentType { get; set; }
        public string PayloadEncoding { get; set; }
        public byte[] Payload { get; set; }
    }

    public sealed class StartTaskRequestDto
    {
        public Guid RegistrationId { get; set; }
        public Guid TaskId { get; set; }
        public Guid AssignmentId { get; set; }
    }

    public sealed class StartTaskResponseDto : BimBridgeResponseDto
    {
        public string State { get; set; }
        public DateTime LeaseExpiresAtUtc { get; set; }
    }

    public sealed class BeginResultUploadRequestDto
    {
        public Guid RegistrationId { get; set; }
        public Guid TaskId { get; set; }
        public Guid AssignmentId { get; set; }
        public string ContentType { get; set; }
        public string ContentEncoding { get; set; }
        public long TotalLength { get; set; }
        public int TotalChunks { get; set; }
        public string Sha256 { get; set; }
    }

    public sealed class BeginResultUploadResponseDto : BimBridgeResponseDto
    {
        public Guid UploadId { get; set; }
        public int ChunkSizeBytes { get; set; }
    }

    public sealed class UploadResultChunkRequestDto
    {
        public Guid UploadId { get; set; }
        public int ChunkIndex { get; set; }
        public byte[] Data { get; set; }
    }

    public sealed class UploadResultChunkResponseDto : BimBridgeResponseDto
    {
        public int ChunkIndex { get; set; }
        public int ReceivedChunks { get; set; }
    }

    public sealed class CompleteTaskRequestDto
    {
        public Guid RegistrationId { get; set; }
        public Guid TaskId { get; set; }
        public Guid AssignmentId { get; set; }
        public Guid UploadId { get; set; }
    }

    public sealed class CompleteTaskResponseDto : BimBridgeResponseDto
    {
        public string State { get; set; }
        public DateTime CompletedAtUtc { get; set; }
    }

    public sealed class BimTaskErrorDto
    {
        public string Code { get; set; }
        public string Message { get; set; }
        public bool IsRetryable { get; set; }
    }

    public sealed class FailTaskRequestDto
    {
        public Guid RegistrationId { get; set; }
        public Guid TaskId { get; set; }
        public Guid AssignmentId { get; set; }
        public BimTaskErrorDto Error { get; set; }
        public bool CancellationAcknowledged { get; set; }
        public string DiagnosticDetails { get; set; }
    }

    public sealed class FailTaskResponseDto : BimBridgeResponseDto
    {
        public string State { get; set; }
        public bool Requeued { get; set; }
    }

    public sealed class BimTaskStatusDto : BimBridgeResponseDto
    {
        public Guid TaskId { get; set; }
        public string State { get; set; }
        public int AttemptNumber { get; set; }
        public DateTime CreatedAtUtc { get; set; }
        public DateTime? AssignedAtUtc { get; set; }
        public DateTime? StartedAtUtc { get; set; }
        public DateTime? CompletedAtUtc { get; set; }
        public BimTaskErrorDto TaskError { get; set; }
    }

    public sealed class BimResultMetadataDto : BimBridgeResponseDto
    {
        public Guid TaskId { get; set; }
        public string ContentType { get; set; }
        public string ContentEncoding { get; set; }
        public long TotalLength { get; set; }
        public int TotalChunks { get; set; }
        public int ChunkSizeBytes { get; set; }
        public string Sha256 { get; set; }
        public DateTime CreatedAtUtc { get; set; }
    }

    public sealed class BimResultChunkDto : BimBridgeResponseDto
    {
        public Guid TaskId { get; set; }
        public int ChunkIndex { get; set; }
        public byte[] Data { get; set; }
        public bool IsLast { get; set; }
    }

    public sealed class CancelBimTaskResponseDto : BimBridgeResponseDto
    {
        public Guid TaskId { get; set; }
        public string State { get; set; }
        public bool CancellationRequested { get; set; }
    }

    public sealed class WorkerStatusDto
    {
        public Guid RegistrationId { get; set; }
        public string WorkerInstanceId { get; set; }
        public string MachineName { get; set; }
        public string State { get; set; }
        public DateTime RegisteredAtUtc { get; set; }
        public DateTime LastSeenAtUtc { get; set; }
        public Guid? CurrentTaskId { get; set; }
        public string[] Capabilities { get; set; }
        public string[] AvailableModelKeys { get; set; }
        public string StatusMessage { get; set; }
    }

    public sealed class BimBridgeHealthDto : BimBridgeResponseDto
    {
        public string State { get; set; }
        public string ProtocolVersion { get; set; }
        public string[] SupportedProtocolVersions { get; set; }
        public int RegisteredWorkers { get; set; }
        public int ReadyWorkers { get; set; }
        public int BusyWorkers { get; set; }
        public int OfflineWorkers { get; set; }
        public int QueuedTasks { get; set; }
        public int RunningTasks { get; set; }
        public int PendingResultUploads { get; set; }
        public long StoredResultBytes { get; set; }
        public string Message { get; set; }
    }

    public sealed class BimBridgeStatisticsDto : BimBridgeResponseDto
    {
        public long SubmittedTasks { get; set; }
        public long CompletedTasks { get; set; }
        public long FailedTasks { get; set; }
        public long CancelledTasks { get; set; }
        public long RetriedTasks { get; set; }
        public long UploadedBytes { get; set; }
        public int ActiveLongPolls { get; set; }
    }
}
