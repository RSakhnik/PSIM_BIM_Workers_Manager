using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class SubmitRequestJournal
    {
        private readonly object _gate = new object();
        private readonly string _path;
        private readonly IBimBridgeLog _log;

        internal SubmitRequestJournal(string resultsDirectory, IBimBridgeLog log)
        {
            _path = Path.Combine(resultsDirectory, "submit-requests.log");
            _log = log;
        }

        internal void Write(Guid callId, Guid? clientRequestId, Guid? taskId, string phase,
            Stopwatch elapsed, int? status = null, string errorCode = null)
        {
            // Only identifiers and bounded protocol values are recorded; never write headers or payloads.
            var line = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture) +
                " call=" + callId.ToString("D") +
                " clientRequest=" + (clientRequestId?.ToString("D") ?? "-") +
                " task=" + (taskId?.ToString("D") ?? "-") +
                " phase=" + phase +
                " elapsedMs=" + elapsed.Elapsed.TotalMilliseconds.ToString("F1", CultureInfo.InvariantCulture) +
                " status=" + (status?.ToString(CultureInfo.InvariantCulture) ?? "-") +
                " error=" + (errorCode ?? "-") + Environment.NewLine;
            try
            {
                lock (_gate)
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(_path));
                    File.AppendAllText(_path, line);
                }
            }
            catch (Exception error)
            {
                _log.Warning("BIM Bridge submit journal write failed: " + error.GetType().Name);
            }
        }
    }
}
