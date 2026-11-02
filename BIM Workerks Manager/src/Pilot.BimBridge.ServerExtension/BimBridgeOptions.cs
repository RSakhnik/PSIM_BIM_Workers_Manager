using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class BimBridgeOptions
    {
        public ProtocolOptions Protocol { get; set; } = new ProtocolOptions();
        public WorkerOptions Workers { get; set; } = new WorkerOptions();
        public TaskOptions Tasks { get; set; } = new TaskOptions();
        public ResultOptions Results { get; set; } = new ResultOptions();
        public MaintenanceOptions Maintenance { get; set; } = new MaintenanceOptions();
        public CancellationOptions Cancellation { get; set; } = new CancellationOptions();
        public LoggingOptions Logging { get; set; } = new LoggingOptions();
        public RestApiOptions RestApi { get; set; } = new RestApiOptions();

        public static BimBridgeOptions Load(string path)
        {
            if (!File.Exists(path))
                throw new FileNotFoundException("BIM Bridge configuration file was not found.", path);

            var options = JsonConvert.DeserializeObject<BimBridgeOptions>(File.ReadAllText(path));
            if (options == null)
                throw new InvalidDataException("BIM Bridge configuration is empty.");

            options.Validate();
            return options;
        }

        public void Validate()
        {
            RestApi.Validate();
            if (Protocol.Version != BimBridgeProtocol.Version)
                throw new InvalidDataException("Protocol.Version must be " + BimBridgeProtocol.Version + ".");
            if (Workers.MaxRegisteredWorkers <= 0 || Workers.MaxParallelTasksPerWorker <= 0)
                throw new InvalidDataException("Worker limits must be positive.");
            if (Workers.HeartbeatIntervalSeconds <= 0 ||
                Workers.WorkerOfflineAfterSeconds <= Workers.HeartbeatIntervalSeconds ||
                Workers.LongPollingTimeoutSeconds <= 0)
                throw new InvalidDataException("Worker timing settings are invalid.");
            if (Tasks.QueueCapacity <= 0 || Tasks.AssignmentLeaseSeconds <= 0 ||
                Tasks.StartTaskTimeoutSeconds <= 0 || Tasks.MaxPayloadBytes <= 0)
                throw new InvalidDataException("Task settings are invalid.");
            if (Results.ChunkSizeBytes <= 0 || Results.MaxResultBytes <= 0 ||
                Results.MaxParallelUploads <= 0 || string.IsNullOrWhiteSpace(Results.Directory))
                throw new InvalidDataException("Result settings are invalid.");
            if (Results.ChunkSizeBytes > Results.MaxUploadChunkBytes || Results.MaxUploadChunkBytes <= 0 ||
                Results.MaxArtifactBytes <= 0 || Results.ArtifactDiskQuotaBytes < Results.MaxArtifactBytes ||
                Results.IncompleteUploadTtlMinutes <= 0 || Results.CompletedResultTtlMinutes <= 0 || Results.ReservationTtlMinutes <= 0)
                throw new InvalidDataException("Artifact resource limits or retention settings are invalid.");
            if (!Path.IsPathRooted(Results.Directory))
                throw new InvalidDataException("Results.Directory must be an absolute path outside the server binaries.");
            if (Maintenance.LeaseCheckIntervalSeconds <= 0 || Maintenance.CleanupIntervalSeconds <= 0)
                throw new InvalidDataException("Maintenance intervals must be positive.");
        }
    }

    // Compatibility endpoint for existing token-authenticated PSIMpy notebooks.
    // Disabled unless explicitly enabled by the existing deployment configuration.
    internal sealed class RestApiOptions
    {
        public bool Enabled { get; set; }
        public string Prefix { get; set; } = "http://127.0.0.1:5099/bim-bridge/";
        public string Token { get; set; }
        public string TokenEnvironmentVariable { get; set; }
        public string AllowedOrigin { get; set; }
        public int MaxRequestBytes { get; set; } = 5 * 1024 * 1024;
        public string ResolveToken()
        {
            var environment=string.IsNullOrWhiteSpace(TokenEnvironmentVariable)?null:Environment.GetEnvironmentVariable(TokenEnvironmentVariable);
            return string.IsNullOrWhiteSpace(environment)?Token:environment;
        }
        public void Validate()
        {
            if (!Enabled) return;
            if (!Uri.TryCreate(Prefix, UriKind.Absolute, out var uri) ||
                (uri.Scheme != "http" && uri.Scheme != "https") || !Prefix.EndsWith("/") ||
                !string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment))
                throw new InvalidDataException("RestApi.Prefix must be an absolute HTTP(S) prefix ending with '/'.");
            if (string.IsNullOrWhiteSpace(ResolveToken()) || MaxRequestBytes <= 0)
                throw new InvalidDataException("Enabled RestApi requires a token and a positive request limit.");
        }
    }

    internal sealed class ProtocolOptions
    {
        public string Version { get; set; } = BimBridgeProtocol.Version;
    }

    internal sealed class WorkerOptions
    {
        public int MaxRegisteredWorkers { get; set; } = 20;
        public int MaxParallelTasksPerWorker { get; set; } = 1;
        public int HeartbeatIntervalSeconds { get; set; } = 15;
        public int WorkerOfflineAfterSeconds { get; set; } = 45;
        public int LongPollingTimeoutSeconds { get; set; } = 25;
    }

    internal sealed class TaskOptions
    {
        public int QueueCapacity { get; set; } = 1000;
        public int AssignmentLeaseSeconds { get; set; } = 60;
        public int StartTaskTimeoutSeconds { get; set; } = 30;
        public int DefaultMaxAttempts { get; set; } = 2;
        public int DefaultTaskTtlMinutes { get; set; } = 60;
        public int MaxPayloadBytes { get; set; } = 5 * 1024 * 1024;
        public string[] RetryableOperationCodes { get; set; } = Array.Empty<string>();
    }

    internal sealed class ResultOptions
    {
        public string Directory { get; set; }
        public int ChunkSizeBytes { get; set; } = 1024 * 1024;
        public long MaxResultBytes { get; set; } = 256L * 1024 * 1024;
        public long MaxArtifactBytes { get; set; } = 8L * 1024 * 1024 * 1024;
        public long ArtifactDiskQuotaBytes { get; set; } = 32L * 1024 * 1024 * 1024;
        public int MaxUploadChunkBytes { get; set; } = 4 * 1024 * 1024;
        public int MaxParallelUploads { get; set; } = 8;
        public int CompletedResultTtlMinutes { get; set; } = 60;
        public int IncompleteUploadTtlMinutes { get; set; } = 30;
        public int ReservationTtlMinutes { get; set; } = 1440;
    }

    internal sealed class MaintenanceOptions
    {
        public int LeaseCheckIntervalSeconds { get; set; } = 10;
        public int CleanupIntervalSeconds { get; set; } = 60;
        public int FailedTaskTtlMinutes { get; set; } = 60;
        public int CancelledTaskTtlMinutes { get; set; } = 30;
        public int WorkerRegistrationTtlMinutes { get; set; } = 10;
    }

    internal sealed class CancellationOptions
    {
        public bool AcceptCompletedResultAfterCancellation { get; set; }
    }

    internal sealed class LoggingOptions
    {
        public bool LogPayloads { get; set; }
        public bool LogResults { get; set; }
    }

    internal interface IClock
    {
        DateTime UtcNow { get; }
    }

    internal sealed class SystemClock : IClock
    {
        public DateTime UtcNow => DateTime.UtcNow;
    }

    internal sealed class MutableClock : IClock
    {
        public MutableClock(DateTime value) { UtcNow = value; }
        public DateTime UtcNow { get; private set; }
        public void Advance(TimeSpan duration) { UtcNow = UtcNow.Add(duration); }
    }

    internal interface IBimBridgeLog
    {
        void Info(string message);
        void Warning(string message);
        void Error(string message, Exception exception);
    }

    internal sealed class TraceBimBridgeLog : IBimBridgeLog
    {
        private readonly System.Diagnostics.TraceSource _source = new System.Diagnostics.TraceSource("Pilot.BimBridge");
        public void Info(string message) { _source.TraceEvent(System.Diagnostics.TraceEventType.Information, 0, message); }
        public void Warning(string message) { _source.TraceEvent(System.Diagnostics.TraceEventType.Warning, 0, message); }
        public void Error(string message, Exception exception)
        {
            _source.TraceEvent(System.Diagnostics.TraceEventType.Error, 0, message + Environment.NewLine + exception);
        }
    }
}
