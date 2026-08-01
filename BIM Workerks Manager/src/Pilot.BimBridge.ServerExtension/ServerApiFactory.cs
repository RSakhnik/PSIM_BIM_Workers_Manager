using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ascon.Pilot.ServerExtensions.SDK;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    [Export(typeof(IServerApiFactory))]
    public sealed class ServerApiFactory : IServerApiFactory, IDisposable
    {
        private readonly IBimBridgeLog _log;
        private readonly BimBridgeRuntime _runtime;
        private readonly string _configurationError;

        public ServerApiFactory()
        {
            _log = new TraceBimBridgeLog();
            try
            {
                var assemblyDirectory = Path.GetDirectoryName(typeof(ServerApiFactory).Assembly.Location);
                var configurationPath = Path.Combine(assemblyDirectory ?? string.Empty, "Pilot.BimBridge.json");
                var options = BimBridgeOptions.Load(configurationPath);
                var binariesPath = Path.GetFullPath((assemblyDirectory ?? string.Empty) + Path.DirectorySeparatorChar);
                var resultsPath = Path.GetFullPath(options.Results.Directory + Path.DirectorySeparatorChar);
                if (resultsPath.StartsWith(binariesPath, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException("Results.Directory must be outside the Pilot-Server extension binaries.");
                var resultStore = new FileSystemBimResultStore(options.Results.Directory);
                resultStore.CleanupOrphansAsync(
                    DateTime.UtcNow,
                    TimeSpan.FromMinutes(options.Results.IncompleteUploadTtlMinutes),
                    TimeSpan.FromMinutes(options.Results.CompletedResultTtlMinutes),
                    CancellationToken.None).GetAwaiter().GetResult();
                _runtime = new BimBridgeRuntime(options, new SystemClock(), resultStore, _log);
            }
            catch (Exception exception)
            {
                _configurationError = exception.Message;
                _log.Error("BIM Bridge initialization failed. Diagnostic API remains available.", exception);
            }
        }

        public bool TryCreate(string interfaceName, IServerApiServiceContainer container, out object api)
        {
            if (interfaceName == nameof(IBimBridgeTaskApi) ||
                interfaceName == nameof(IBimBridgeWorkerApi) ||
                interfaceName == nameof(IBimBridgeDiagnosticsApi))
            {
                api = new BimBridgeApiSession(_runtime, _configurationError, _log);
                return true;
            }
            api = null;
            return false;
        }

        public void Dispose()
        {
            _runtime?.Dispose();
        }
    }

    internal sealed class BimBridgeApiSession :
        IBimBridgeTaskApi,
        IBimBridgeWorkerApi,
        IBimBridgeDiagnosticsApi,
        IDisposable
    {
        private readonly BimBridgeRuntime _runtime;
        private readonly string _configurationError;
        private readonly IBimBridgeLog _log;
        private readonly CancellationTokenSource _sessionClosed = new CancellationTokenSource();

        public BimBridgeApiSession(BimBridgeRuntime runtime, string configurationError, IBimBridgeLog log)
        {
            _runtime = runtime;
            _configurationError = configurationError;
            _log = log;
        }

        public Task<BimTaskAcceptedDto> SubmitTaskAsync(SubmitBimTaskRequestDto request)
            => Guard(() => _runtime.Submit(request));

        public Task<BimTaskStatusDto> GetTaskStatusAsync(Guid taskId)
            => Guard(() => _runtime.GetTaskStatus(taskId));

        public Task<BimResultMetadataDto> GetResultMetadataAsync(Guid taskId)
            => GuardAsync(token => _runtime.GetResultMetadataAsync(taskId, token));

        public Task<BimResultChunkDto> ReadResultChunkAsync(Guid taskId, int chunkIndex)
            => GuardAsync(token => _runtime.ReadResultChunkAsync(taskId, chunkIndex, token));

        public Task<CancelBimTaskResponseDto> CancelTaskAsync(Guid taskId)
            => Guard(() => _runtime.Cancel(taskId));

        public Task<RegisterWorkerResponseDto> RegisterWorkerAsync(RegisterWorkerRequestDto request)
            => Guard(() => _runtime.Register(request));

        public Task UnregisterWorkerAsync(UnregisterWorkerRequestDto request)
        {
            if (_runtime == null) return Task.CompletedTask;
            try { _runtime.Unregister(request); }
            catch (Exception exception) { _log.Error("UnregisterWorkerAsync failed.", exception); }
            return Task.CompletedTask;
        }

        public Task<WaitForTaskResponseDto> WaitForTaskAsync(WaitForTaskRequestDto request)
            => GuardAsync(token => _runtime.WaitForTaskAsync(request, token));

        public Task<StartTaskResponseDto> StartTaskAsync(StartTaskRequestDto request)
            => Guard(() => _runtime.StartTask(request));

        public Task<WorkerHeartbeatResponseDto> SendHeartbeatAsync(WorkerHeartbeatRequestDto request)
            => Guard(() => _runtime.Heartbeat(request));

        public Task<BeginResultUploadResponseDto> BeginResultUploadAsync(BeginResultUploadRequestDto request)
            => GuardAsync(token => _runtime.BeginUploadAsync(request, token));

        public Task<UploadResultChunkResponseDto> UploadResultChunkAsync(UploadResultChunkRequestDto request)
            => GuardAsync(token => _runtime.UploadChunkAsync(request, token));

        public Task<CompleteTaskResponseDto> CompleteTaskAsync(CompleteTaskRequestDto request)
            => GuardAsync(token => _runtime.CompleteAsync(request, token));

        public Task<FailTaskResponseDto> FailTaskAsync(FailTaskRequestDto request)
            => Guard(() => _runtime.Fail(request));

        public Task<BimBridgeHealthDto> GetHealthAsync()
        {
            if (_runtime != null)
                return GuardAsync(token => _runtime.GetHealthAsync(token));
            return Task.FromResult(new BimBridgeHealthDto
            {
                State = "MISCONFIGURED",
                ProtocolVersion = BimBridgeProtocol.Version,
                SupportedProtocolVersions = BimBridgeProtocol.SupportedVersions,
                Message = _configurationError,
                Error = ResponseErrors.Create(BimBridgeErrorCodes.InternalError, _configurationError ?? "BIM Bridge is not initialized.")
            });
        }

        public Task<System.Collections.Generic.List<WorkerStatusDto>> GetWorkersAsync()
        {
            if (_runtime == null)
                return Task.FromResult(new System.Collections.Generic.List<WorkerStatusDto>());
            try { return Task.FromResult(_runtime.GetWorkers()); }
            catch (Exception exception)
            {
                _log.Error("GetWorkersAsync failed.", exception);
                return Task.FromResult(new System.Collections.Generic.List<WorkerStatusDto>());
            }
        }

        public Task<BimBridgeStatisticsDto> GetStatisticsAsync()
            => Guard(() => _runtime.GetStatistics());

        public void Dispose()
        {
            _sessionClosed.Cancel();
            _sessionClosed.Dispose();
        }

        private Task<T> Guard<T>(Func<T> operation) where T : BimBridgeResponseDto, new()
        {
            if (_runtime == null)
                return Task.FromResult(ResponseErrors.Error<T>(BimBridgeErrorCodes.InternalError, _configurationError ?? "BIM Bridge is not initialized."));
            try { return Task.FromResult(operation()); }
            catch (Exception exception)
            {
                _log.Error("BIM Bridge API call failed.", exception);
                return Task.FromResult(ResponseErrors.Error<T>(BimBridgeErrorCodes.InternalError, "Internal server error.", true));
            }
        }

        private async Task<T> GuardAsync<T>(Func<CancellationToken, Task<T>> operation) where T : BimBridgeResponseDto, new()
        {
            if (_runtime == null)
                return ResponseErrors.Error<T>(BimBridgeErrorCodes.InternalError, _configurationError ?? "BIM Bridge is not initialized.");
            try { return await operation(_sessionClosed.Token).ConfigureAwait(false); }
            catch (OperationCanceledException)
            {
                return ResponseErrors.Error<T>(BimBridgeErrorCodes.InternalError, "Client session was closed.", true);
            }
            catch (Exception exception)
            {
                _log.Error("BIM Bridge API call failed.", exception);
                return ResponseErrors.Error<T>(BimBridgeErrorCodes.InternalError, "Internal server error.", true);
            }
        }
    }
}
