// Narrow environment doubles for the linked production ArtifactTransfer.cs.
// These tests run without WPF, Pilot process startup or settings-file access.
using System;
using System.IO;
using Pilot.BimBridge.Contracts;
namespace AdvancedTools.BimBridge
{
    public sealed class BimBridgeSettings
    {
        public string ResultsDirectory {get;set;}
        public string DurableWorkerId {get;set;}="test-owner";
        public long MaxArtifactBytes {get;set;}=1024*1024;
        public long ArtifactDiskQuotaBytes {get;set;}=2*1024*1024;
        public int MaxPreparedArtifacts {get;set;}=2;
        public long MaxPreparedArtifactBytes {get;set;}=2*1024*1024;
        public int ArtifactRetentionHours {get;set;}=24;
        public int ResultTransferMaxAttempts {get;set;}=3;
        public int ResultTransferTimeoutSeconds {get;set;}=5;
        public int MaxInFlightChunks {get;set;}=3;
        public int MaxUploadChunkBytes {get;set;}=4*1024*1024;
        public long MaxExportElements {get;set;}=1000000;
        public int MaxExportRecordBytes {get;set;}=1024*1024;
        public long SelectionDiskQuotaBytes {get;set;}=1024*1024*64;
        public int CursorRetentionMinutes {get;set;}=60;
        public ResourceLimitSettings ResourceLimits {get;}=new ResourceLimitSettings();
    }
    public interface IBimBridgeLog {void Info(string message);void Warn(string message);}
    public sealed class PreparedResult:IDisposable
    {
        public string Path {get;set;}
        public long Length {get;set;}
        public string Sha256 {get;set;}
        public string ContentType {get;set;}
        public string ContentEncoding {get;set;}
        public string FileName {get;set;}
        public string ResultKind {get;set;}
        public bool DeleteOnDispose {get;set;}
        public void Dispose() {if(DeleteOnDispose)File.Delete(Path);}
    }
    public sealed class ResourceLimitSettings {public int MaxSearchResults {get;set;}=10000;}
    internal static class TemporaryFileCleaner {public static void TryDelete(string path) {File.Delete(path);}}
    public sealed class BimBridgeException:Exception
    {
        public string Code {get;}
        public bool IsRetryable {get;}
        public BimBridgeException(string code,string message,bool retryable=false,Exception inner=null):base(message,inner) {Code=code;IsRetryable=retryable;}
        public static void ThrowIfError(BimBridgeResponseDto result)
        {if(result.Error!=null) throw new BimBridgeException(result.Error.Code,result.Error.Message,result.Error.IsRetryable);}
    }
    public static class TransportFailurePolicy
    {
        public static BimBridgeException Classify(Exception error) => error as BimBridgeException
            ?? new BimBridgeException("TRANSPORT_ERROR",error.Message,error is IOException);
    }
}
