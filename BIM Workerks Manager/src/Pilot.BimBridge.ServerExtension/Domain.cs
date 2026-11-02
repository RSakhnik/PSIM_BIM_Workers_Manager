using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class WorkerRecord
    {
        public Guid RegistrationId;
        public string WorkerInstanceId;
        public string MachineName;
        public string ExtensionVersion;
        public string BimSdkVersion;
        public HashSet<string> Capabilities = new HashSet<string>(StringComparer.Ordinal);
        public HashSet<string> Models = new HashSet<string>(StringComparer.Ordinal);
        public string State;
        public string StatusMessage;
        public DateTime RegisteredAtUtc;
        public DateTime LastSeenAtUtc;
        public Guid? CurrentTaskId;
        public bool IsStale;
    }

    internal sealed class TaskRecord
    {
        public Guid TaskId;
        public Guid ClientRequestId;
        public string OperationCode;
        public string[] RequiredCapabilities;
        public string TargetModelKey;
        public string PreferredWorkerInstanceId;
        public string ExecutionWorkerInstanceId;
        public DateTime? PreparedAtUtc;
        public double? WorkerProcessingWallMs;
        public int Priority;
        public string PayloadContentType;
        public string PayloadEncoding;
        public byte[] Payload;
        public bool CanRetry;
        public int MaxAttempts;
        public DateTime? ExpiresAtUtc;
        public string State;
        public int AttemptNumber;
        public Guid? RegistrationId;
        public Guid? AssignmentId;
        public DateTime? LeaseExpiresAtUtc;
        public DateTime CreatedAtUtc;
        public DateTime? AssignedAtUtc;
        public DateTime? StartedAtUtc;
        public DateTime? CompletedAtUtc;
        public BimTaskErrorDto Error;
        public Guid? UploadId;
    }

    internal sealed class UploadRecord
    {
        public Guid UploadId;
        public Guid TaskId;
        public Guid AssignmentId;
        public Guid RegistrationId;
        public string ContentType;
        public string ContentEncoding;
        public long TotalLength;
        public int TotalChunks;
        public string Sha256;
        public DateTime CreatedAtUtc;
        public int ReceivedChunks;
        public double? WorkerProcessingWallMs;
        public int ChunkSizeBytes;
        public string WorkerInstanceId;
        public string FileName;
        public string ResultKind;
        public Guid ClientRequestId;
        public string OperationCode;
        public DateTime TaskCreatedAtUtc;
    }

    internal sealed class Waiter
    {
        public Guid RegistrationId;
        public TaskCompletionSource<WaitForTaskResponseDto> Completion;
    }

    internal static class ResponseErrors
    {
        public static BimBridgeErrorDto Create(string code, string message, bool retryable = false)
        {
            return new BimBridgeErrorDto { Code = code, Message = message, IsRetryable = retryable };
        }

        public static T Error<T>(string code, string message, bool retryable = false)
            where T : BimBridgeResponseDto, new()
        {
            return new T { Error = Create(code, message, retryable) };
        }
    }
}
