using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class StoredResultMetadata
    {
        public Guid TaskId { get; set; }
        public string ContentType { get; set; }
        public string ContentEncoding { get; set; }
        public long TotalLength { get; set; }
        public int TotalChunks { get; set; }
        public int ChunkSizeBytes { get; set; }
        public string Sha256 { get; set; }
        public DateTime CreatedAtUtc { get; set; }
        public string FileName { get; set; }
        public string ResultKind { get; set; }
    }

    internal sealed class ResultStoreException : Exception
    {
        public ResultStoreException(string code, string message) : base(message) { Code = code; }
        public string Code { get; }
    }

    internal interface IBimResultStore
    {
        Task BeginUploadAsync(UploadRecord upload, CancellationToken cancellationToken);
        Task<bool> WriteChunkAsync(UploadRecord upload, int chunkIndex, byte[] data, CancellationToken cancellationToken);
        Task<StoredResultMetadata> CompleteUploadAsync(UploadRecord upload, int chunkSizeBytes, DateTime now, CancellationToken cancellationToken);
        Task<StoredResultMetadata> GetMetadataAsync(Guid taskId, CancellationToken cancellationToken);
        Task<byte[]> ReadChunkAsync(Guid taskId, int chunkIndex, int chunkSizeBytes, CancellationToken cancellationToken);
        Task DeleteAsync(Guid taskId, CancellationToken cancellationToken);
        Task DeleteUploadAsync(Guid uploadId, CancellationToken cancellationToken);
        Task<long> GetStoredBytesAsync(CancellationToken cancellationToken);
        Task CleanupOrphansAsync(DateTime now, TimeSpan incompleteUploadTtl, TimeSpan completedResultTtl, CancellationToken cancellationToken);
    }

    internal sealed partial class FileSystemBimResultStore : IBimResultStore
    {
        private readonly string _root;
        private readonly string _uploadsRoot;
        private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _uploadLocks = new ConcurrentDictionary<Guid, SemaphoreSlim>();
        private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _resultLocks = new ConcurrentDictionary<Guid, SemaphoreSlim>();

        public FileSystemBimResultStore(string root)
        {
            _root = Path.GetFullPath(root);
            _uploadsRoot = Path.Combine(_root, ".uploads");
            Directory.CreateDirectory(_root);
            Directory.CreateDirectory(_uploadsRoot);
        }

        public Task BeginUploadAsync(UploadRecord upload, CancellationToken cancellationToken)
            => BeginPartAsync(upload, cancellationToken);

        public Task<bool> WriteChunkAsync(UploadRecord upload, int chunkIndex, byte[] data, CancellationToken cancellationToken)
            => WritePartAsync(upload, chunkIndex, data, cancellationToken);

        public Task<StoredResultMetadata> CompleteUploadAsync(UploadRecord upload, int chunkSizeBytes, DateTime now, CancellationToken cancellationToken)
            => CompletePartAsync(upload, now, cancellationToken);

        public async Task<StoredResultMetadata> GetMetadataAsync(Guid taskId, CancellationToken cancellationToken)
        {
            var gate = _resultLocks.GetOrAdd(taskId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var path = Path.Combine(ResultPath(taskId), "metadata.json");
                if (!File.Exists(path))
                    return null;
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true))
                using (var reader = new StreamReader(stream, Encoding.UTF8))
                    return JsonConvert.DeserializeObject<StoredResultMetadata>(await reader.ReadToEndAsync().ConfigureAwait(false));
            }
            finally
            {
                gate.Release();
            }
        }

        public async Task<byte[]> ReadChunkAsync(Guid taskId, int chunkIndex, int chunkSizeBytes, CancellationToken cancellationToken)
        {
            if (chunkIndex < 0)
                throw new ResultStoreException(BimBridgeErrorCodes.ChunkOutOfRange, "Chunk index cannot be negative.");
            var gate = _resultLocks.GetOrAdd(taskId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var path = Path.Combine(ResultPath(taskId), "result.bin");
                if (!File.Exists(path))
                    return null;
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, true))
                {
                    var offset = (long)chunkIndex * chunkSizeBytes;
                    if (offset >= stream.Length)
                        throw new ResultStoreException(BimBridgeErrorCodes.ChunkOutOfRange, "Chunk index is outside the result.");
                    stream.Seek(offset, SeekOrigin.Begin);
                    var length = (int)Math.Min(chunkSizeBytes, stream.Length - offset);
                    var buffer = new byte[length];
                    var read = 0;
                    while (read < length)
                    {
                        var count = await stream.ReadAsync(buffer, read, length - read, cancellationToken).ConfigureAwait(false);
                        if (count == 0) break;
                        read += count;
                    }
                    return buffer;
                }
            }
            finally
            {
                gate.Release();
            }
        }

        public async Task DeleteAsync(Guid taskId, CancellationToken cancellationToken)
        {
            var gate = _resultLocks.GetOrAdd(taskId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var path = ResultPath(taskId);
                if (Directory.Exists(path))
                    Directory.Delete(path, true);
            }
            finally
            {
                gate.Release();
            }
        }

        public async Task DeleteUploadAsync(Guid uploadId, CancellationToken cancellationToken)
        {
            var gate=_uploadLocks.GetOrAdd(uploadId,_=>new SemaphoreSlim(1,1));
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var path = UploadPath(uploadId);
                if (Directory.Exists(path))Directory.Delete(path, true);
            }
            finally {gate.Release();}
        }

        public Task<long> GetStoredBytesAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long total = 0;
            if (Directory.Exists(_root))
            {
                foreach (var path in Directory.EnumerateFiles(_root, "result.bin", SearchOption.AllDirectories))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    total += new FileInfo(path).Length;
                }
            }
            return Task.FromResult(total);
        }

        public Task CleanupOrphansAsync(
            DateTime now,
            TimeSpan incompleteUploadTtl,
            TimeSpan completedResultTtl,
            CancellationToken cancellationToken)
        {
            if (Directory.Exists(_uploadsRoot))
            {
                foreach (var directory in Directory.EnumerateDirectories(_uploadsRoot))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (now - Directory.GetLastWriteTimeUtc(directory) > incompleteUploadTtl)
                        Directory.Delete(directory, true);
                }
            }

            foreach (var directory in Directory.EnumerateDirectories(_root))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.Equals(directory, _uploadsRoot, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetFileName(directory).StartsWith(".", StringComparison.Ordinal))
                    continue;
                var metadataPath = Path.Combine(directory, "metadata.json");
                var resultPath = Path.Combine(directory, "result.bin");
                var lastWrite = Directory.GetLastWriteTimeUtc(directory);
                if ((!File.Exists(metadataPath) || !File.Exists(resultPath)) && now - lastWrite > incompleteUploadTtl)
                    Directory.Delete(directory, true);
                else if (File.Exists(metadataPath) && File.Exists(resultPath) && now - lastWrite > completedResultTtl)
                    Directory.Delete(directory, true);
            }
            return Task.CompletedTask;
        }

        private string UploadPath(Guid uploadId) => Path.Combine(_uploadsRoot, uploadId.ToString("N"));
        private string ChunkPath(Guid uploadId, int index) => Path.Combine(UploadPath(uploadId), "chunk-" + index.ToString("D8") + ".bin");
        private string ResultPath(Guid taskId) => Path.Combine(_root, taskId.ToString("N"));

        private static async Task<string> CalculateSha256Async(string path, CancellationToken cancellationToken)
        {
            using (var hash = SHA256.Create())
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, true))
            {
                var buffer = new byte[81920];
                int read;
                while ((read = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false)) > 0)
                    hash.TransformBlock(buffer, 0, read, null, 0);
                hash.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                return string.Concat(hash.Hash.Select(value => value.ToString("x2")));
            }
        }
    }
}
