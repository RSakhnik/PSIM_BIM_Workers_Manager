using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProtoBuf;

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
        public const string UnsupportedPilotVersion = "UNSUPPORTED_PILOT_VERSION";
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
        public const string ResultExpired = "RESULT_EXPIRED";
        public const string FileReferenceInvalid = "FILE_REFERENCE_INVALID";
        public const string FileReferenceAmbiguous = "FILE_REFERENCE_AMBIGUOUS";
        public const string FileNotFound = "FILE_NOT_FOUND";
        public const string FileVersionNotFound = "FILE_VERSION_NOT_FOUND";
        public const string FileMountFailed = "FILE_MOUNT_FAILED";
        public const string FileUnmountFailed = "FILE_UNMOUNT_FAILED";
        public const string FileReadFailed = "FILE_READ_FAILED";
        public const string FileCopyFailed = "FILE_COPY_FAILED";
        public const string FileTooLarge = "FILE_TOO_LARGE";
        public const string UnsupportedFileType = "UNSUPPORTED_FILE_TYPE";
        public const string ExcelOpenFailed = "EXCEL_OPEN_FAILED";
        public const string ExcelSheetNotFound = "EXCEL_SHEET_NOT_FOUND";
        public const string ExcelContentLimitExceeded = "EXCEL_CONTENT_LIMIT_EXCEEDED";
        public const string TempFileIoError = "TEMP_FILE_IO_ERROR";
        public const string ResourceLimit = "RESOURCE_LIMIT";
        public const string InternalError = "INTERNAL_ERROR";
    }

    [ProtoContract]
    public sealed class BimBridgeErrorDto
    {
        [ProtoMember(1)]
        public string Code { get; set; }
        [ProtoMember(2)]
        public string Message { get; set; }
        [ProtoMember(3)]
        public bool IsRetryable { get; set; }
    }

    [ProtoContract]
    [ProtoInclude(100, typeof(RegisterWorkerResponseDto))]
    [ProtoInclude(101, typeof(WorkerHeartbeatResponseDto))]
    [ProtoInclude(102, typeof(WaitForTaskResponseDto))]
    [ProtoInclude(103, typeof(BimTaskAcceptedDto))]
    [ProtoInclude(104, typeof(StartTaskResponseDto))]
    [ProtoInclude(105, typeof(BeginResultUploadResponseDto))]
    [ProtoInclude(106, typeof(UploadResultChunkResponseDto))]
    [ProtoInclude(107, typeof(CompleteTaskResponseDto))]
    [ProtoInclude(108, typeof(FailTaskResponseDto))]
    [ProtoInclude(109, typeof(BimTaskStatusDto))]
    [ProtoInclude(110, typeof(BimResultMetadataDto))]
    [ProtoInclude(111, typeof(BimResultChunkDto))]
    [ProtoInclude(112, typeof(CancelBimTaskResponseDto))]
    [ProtoInclude(113, typeof(BimBridgeHealthDto))]
    [ProtoInclude(114, typeof(BimBridgeStatisticsDto))]
    [ProtoInclude(115, typeof(ArtifactUploadStatusDto))]
    [ProtoInclude(116, typeof(ArtifactChunkResponseDto))]
    public abstract class BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public BimBridgeErrorDto Error { get; set; }
        [ProtoIgnore]
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

    [ProtoContract]
    public sealed class BimBridgeNotificationDto
    {
        [ProtoMember(1)]
        public string Code { get; set; }
        [ProtoMember(2)]
        public string Message { get; set; }
    }

    [ProtoContract]
    public sealed class RegisterWorkerRequestDto
    {
        [ProtoMember(1)]
        public string ProtocolVersion { get; set; }
        [ProtoMember(2)]
        public string WorkerInstanceId { get; set; }
        [ProtoMember(3)]
        public string MachineName { get; set; }
        [ProtoMember(4)]
        public string ExtensionVersion { get; set; }
        [ProtoMember(5)]
        public string BimSdkVersion { get; set; }
        [ProtoMember(6)]
        public string[] Capabilities { get; set; }
        [ProtoMember(7)]
        public string[] AvailableModelKeys { get; set; }
        [ProtoMember(8)]
        public int MaxParallelTasks { get; set; }
    }

    [ProtoContract]
    public sealed class RegisterWorkerResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public DateTime ServerTimeUtc { get; set; }
        [ProtoMember(3)]
        public int HeartbeatIntervalSeconds { get; set; }
        [ProtoMember(4)]
        public int AssignmentLeaseSeconds { get; set; }
        [ProtoMember(5)]
        public int LongPollingTimeoutSeconds { get; set; }
        [ProtoMember(6)]
        public int ResultChunkSizeBytes { get; set; }
    }

    [ProtoContract]
    public sealed class UnregisterWorkerRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
    }

    [ProtoContract]
    public sealed class WorkerHeartbeatRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public string State { get; set; }
        [ProtoMember(3)]
        public Guid? CurrentTaskId { get; set; }
        [ProtoMember(4)]
        public Guid? AssignmentId { get; set; }
        [ProtoMember(5)]
        public string[] AvailableModelKeys { get; set; }
        [ProtoMember(6)]
        public string[] Capabilities { get; set; }
        [ProtoMember(7)]
        public string StatusMessage { get; set; }
        [ProtoMember(8)]
        public DateTime ClientTimeUtc { get; set; }
    }

    [ProtoContract]
    public sealed class WorkerHeartbeatResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public DateTime ServerTimeUtc { get; set; }
        [ProtoMember(2)]
        public bool RegistrationIsValid { get; set; }
        [ProtoMember(3)]
        public bool CancellationRequested { get; set; }
        [ProtoMember(4)]
        public DateTime? LeaseExpiresAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class WaitForTaskRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public int WaitTimeoutSeconds { get; set; }
    }

    [ProtoContract]
    public sealed class WaitForTaskResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public bool HasTask { get; set; }
        [ProtoMember(2)]
        public BimTaskAssignmentDto Assignment { get; set; }
        [ProtoMember(3)]
        public DateTime ServerTimeUtc { get; set; }
    }

    [ProtoContract]
    public sealed class SubmitBimTaskRequestDto
    {
        [ProtoMember(1)]
        public Guid ClientRequestId { get; set; }
        [ProtoMember(2)]
        public string ProtocolVersion { get; set; }
        [ProtoMember(3)]
        public string OperationCode { get; set; }
        [ProtoMember(4)]
        public string[] RequiredCapabilities { get; set; }
        [ProtoMember(5)]
        public string TargetModelKey { get; set; }
        [ProtoMember(6)]
        public string PreferredWorkerInstanceId { get; set; }
        [ProtoMember(7)]
        public int Priority { get; set; }
        [ProtoMember(8)]
        public string PayloadContentType { get; set; }
        [ProtoMember(9)]
        public string PayloadEncoding { get; set; }
        [ProtoMember(10)]
        public byte[] Payload { get; set; }
        [ProtoMember(11)]
        public bool CanRetry { get; set; }
        [ProtoMember(12)]
        public int MaxAttempts { get; set; }
        [ProtoMember(13)]
        public DateTime? ExpiresAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class BimTaskAcceptedDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public string State { get; set; }
        [ProtoMember(3)]
        public DateTime CreatedAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class BimTaskAssignmentDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public Guid AssignmentId { get; set; }
        [ProtoMember(3)]
        public int AttemptNumber { get; set; }
        [ProtoMember(4)]
        public DateTime LeaseExpiresAtUtc { get; set; }
        [ProtoMember(5)]
        public string OperationCode { get; set; }
        [ProtoMember(6)]
        public string TargetModelKey { get; set; }
        [ProtoMember(7)]
        public string PayloadContentType { get; set; }
        [ProtoMember(8)]
        public string PayloadEncoding { get; set; }
        [ProtoMember(9)]
        public byte[] Payload { get; set; }
    }

    [ProtoContract]
    public sealed class StartTaskRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public Guid TaskId { get; set; }
        [ProtoMember(3)]
        public Guid AssignmentId { get; set; }
    }

    [ProtoContract]
    public sealed class StartTaskResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public string State { get; set; }
        [ProtoMember(2)]
        public DateTime LeaseExpiresAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class BeginResultUploadRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public Guid TaskId { get; set; }
        [ProtoMember(3)]
        public Guid AssignmentId { get; set; }
        [ProtoMember(4)]
        public string ContentType { get; set; }
        [ProtoMember(5)]
        public string ContentEncoding { get; set; }
        [ProtoMember(6)]
        public long TotalLength { get; set; }
        [ProtoMember(7)]
        public int TotalChunks { get; set; }
        [ProtoMember(8)]
        public string Sha256 { get; set; }
        [ProtoMember(9)]
        public string FileName { get; set; }
        [ProtoMember(10)]
        public string ResultKind { get; set; }
    }

    [ProtoContract]
    public sealed class BeginResultUploadResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid UploadId { get; set; }
        [ProtoMember(2)]
        public int ChunkSizeBytes { get; set; }
    }

    [ProtoContract]
    public sealed class UploadResultChunkRequestDto
    {
        [ProtoMember(1)]
        public Guid UploadId { get; set; }
        [ProtoMember(2)]
        public int ChunkIndex { get; set; }
        [ProtoMember(3)]
        public byte[] Data { get; set; }
    }

    [ProtoContract]
    public sealed class UploadResultChunkResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public int ChunkIndex { get; set; }
        [ProtoMember(2)]
        public int ReceivedChunks { get; set; }
    }

    [ProtoContract]
    public sealed class CompleteTaskRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public Guid TaskId { get; set; }
        [ProtoMember(3)]
        public Guid AssignmentId { get; set; }
        [ProtoMember(4)]
        public Guid UploadId { get; set; }
    }

    [ProtoContract]
    public sealed class CompleteTaskResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public string State { get; set; }
        [ProtoMember(2)]
        public DateTime CompletedAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class BimTaskErrorDto
    {
        [ProtoMember(1)]
        public string Code { get; set; }
        [ProtoMember(2)]
        public string Message { get; set; }
        [ProtoMember(3)]
        public bool IsRetryable { get; set; }
    }

    [ProtoContract]
    public sealed class FailTaskRequestDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public Guid TaskId { get; set; }
        [ProtoMember(3)]
        public Guid AssignmentId { get; set; }
        [ProtoMember(4)]
        public BimTaskErrorDto Error { get; set; }
        [ProtoMember(5)]
        public bool CancellationAcknowledged { get; set; }
        [ProtoMember(6)]
        public string DiagnosticDetails { get; set; }
    }

    [ProtoContract]
    public sealed class FailTaskResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public string State { get; set; }
        [ProtoMember(2)]
        public bool Requeued { get; set; }
    }

    [ProtoContract]
    public sealed class BimTaskStatusDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public string State { get; set; }
        [ProtoMember(3)]
        public int AttemptNumber { get; set; }
        [ProtoMember(4)]
        public DateTime CreatedAtUtc { get; set; }
        [ProtoMember(5)]
        public DateTime? AssignedAtUtc { get; set; }
        [ProtoMember(6)]
        public DateTime? StartedAtUtc { get; set; }
        [ProtoMember(7)]
        public DateTime? CompletedAtUtc { get; set; }
        [ProtoMember(8)]
        public BimTaskErrorDto TaskError { get; set; }
        [ProtoMember(9)] public string WorkerInstanceId {get;set;}
        [ProtoMember(10)] public double? ProcessingWallMs {get;set;}
    }

    [ProtoContract]
    public sealed class BimResultMetadataDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public string ContentType { get; set; }
        [ProtoMember(3)]
        public string ContentEncoding { get; set; }
        [ProtoMember(4)]
        public long TotalLength { get; set; }
        [ProtoMember(5)]
        public int TotalChunks { get; set; }
        [ProtoMember(6)]
        public int ChunkSizeBytes { get; set; }
        [ProtoMember(7)]
        public string Sha256 { get; set; }
        [ProtoMember(8)]
        public DateTime CreatedAtUtc { get; set; }
        [ProtoMember(9)]
        public string FileName { get; set; }
        [ProtoMember(10)]
        public string ResultKind { get; set; }
        [ProtoMember(11)]
        public DateTime? ExpiresAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class BimResultChunkDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public int ChunkIndex { get; set; }
        [ProtoMember(3)]
        public byte[] Data { get; set; }
        [ProtoMember(4)]
        public bool IsLast { get; set; }
    }

    [ProtoContract]
    public sealed class CancelBimTaskResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public Guid TaskId { get; set; }
        [ProtoMember(2)]
        public string State { get; set; }
        [ProtoMember(3)]
        public bool CancellationRequested { get; set; }
    }

    [ProtoContract]
    public sealed class WorkerStatusDto
    {
        [ProtoMember(1)]
        public Guid RegistrationId { get; set; }
        [ProtoMember(2)]
        public string WorkerInstanceId { get; set; }
        [ProtoMember(3)]
        public string MachineName { get; set; }
        [ProtoMember(4)]
        public string State { get; set; }
        [ProtoMember(5)]
        public DateTime RegisteredAtUtc { get; set; }
        [ProtoMember(6)]
        public DateTime LastSeenAtUtc { get; set; }
        [ProtoMember(7)]
        public Guid? CurrentTaskId { get; set; }
        [ProtoMember(8)]
        public string[] Capabilities { get; set; }
        [ProtoMember(9)]
        public string[] AvailableModelKeys { get; set; }
        [ProtoMember(10)]
        public string StatusMessage { get; set; }
    }

    [ProtoContract]
    public sealed class BimBridgeHealthDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public string State { get; set; }
        [ProtoMember(2)]
        public string ProtocolVersion { get; set; }
        [ProtoMember(3)]
        public string[] SupportedProtocolVersions { get; set; }
        [ProtoMember(4)]
        public int RegisteredWorkers { get; set; }
        [ProtoMember(5)]
        public int ReadyWorkers { get; set; }
        [ProtoMember(6)]
        public int BusyWorkers { get; set; }
        [ProtoMember(7)]
        public int OfflineWorkers { get; set; }
        [ProtoMember(8)]
        public int QueuedTasks { get; set; }
        [ProtoMember(9)]
        public int RunningTasks { get; set; }
        [ProtoMember(10)]
        public int PendingResultUploads { get; set; }
        [ProtoMember(11)]
        public long StoredResultBytes { get; set; }
        [ProtoMember(12)]
        public string Message { get; set; }
        [ProtoMember(14)] public string[] Capabilities { get; set; }
    }

    [ProtoContract]
    public sealed class BimBridgeStatisticsDto : BimBridgeResponseDto
    {
        [ProtoMember(1)]
        public long SubmittedTasks { get; set; }
        [ProtoMember(2)]
        public long CompletedTasks { get; set; }
        [ProtoMember(3)]
        public long FailedTasks { get; set; }
        [ProtoMember(4)]
        public long CancelledTasks { get; set; }
        [ProtoMember(5)]
        public long RetriedTasks { get; set; }
        [ProtoMember(6)]
        public long UploadedBytes { get; set; }
        [ProtoMember(7)]
        public int ActiveLongPolls { get; set; }
        [ProtoMember(8)] public int QueuedTasks {get;set;}
        [ProtoMember(9)] public int RunningTasks {get;set;}
        [ProtoMember(10)] public int WaitingWorkers {get;set;}
        [ProtoMember(11)] public int BusyWorkers {get;set;}
        [ProtoMember(12)] public long AssignmentsTotal {get;set;}
        [ProtoMember(13)] public System.Collections.Generic.Dictionary<string,long> AssignmentsByWorker {get;set;}
        [ProtoMember(14)] public System.Collections.Generic.Dictionary<string,long> CompletedTasksByWorker {get;set;}
        [ProtoMember(15)] public int ActiveUploads {get;set;}
        [ProtoMember(16)] public int PreparedArtifacts {get;set;}
        [ProtoMember(17)] public int ActiveDownloads {get;set;}
        [ProtoMember(18)] public long DownloadedBytes {get;set;}
        [ProtoMember(19)] public double UploadBytesPerSecond {get;set;}
        [ProtoMember(20)] public double DownloadBytesPerSecond {get;set;}
        [ProtoMember(21)] public double ManagerShaDurationMs {get;set;}
        [ProtoMember(22)] public System.Collections.Generic.Dictionary<string,double> AverageProcessingWallMsByWorker {get;set;}
    }
}
