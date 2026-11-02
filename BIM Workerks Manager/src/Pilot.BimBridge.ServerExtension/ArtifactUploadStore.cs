using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Pilot.BimBridge.ServerExtension
{
    // Shared by legacy and resumable uploads. The descriptor is immutable. A chunk
    // commit is data -> flush -> hash -> flush -> presence byte -> flush. An
    // interrupted uncommitted write is overwritten on resume, never trusted.
    internal sealed partial class FileSystemBimResultStore
    {
        private const int StateRecordBytes = 33;

        internal IEnumerable<Tuple<UploadRecord, StoredResultMetadata>> RecoverArtifacts()
        {
            foreach (var directory in Directory.EnumerateDirectories(_uploadsRoot)
                .Concat(Directory.EnumerateDirectories(_root).Where(path => path != _uploadsRoot)))
            {
                var descriptor = ReadDescriptor(directory);
                if (descriptor == null || string.IsNullOrEmpty(descriptor.WorkerInstanceId)) continue;
                var metadataPath = Path.Combine(directory, "metadata.json");
                // Metadata in .uploads is preparation, not publication.
                var metadata = Path.GetDirectoryName(directory) == _uploadsRoot || !File.Exists(metadataPath)
                    ? null : JsonConvert.DeserializeObject<StoredResultMetadata>(File.ReadAllText(metadataPath));
                yield return Tuple.Create(descriptor, metadata);
            }
        }

        private static string Hash(byte[] data)
        {
            using (var sha = SHA256.Create())
                return BitConverter.ToString(sha.ComputeHash(data)).Replace("-", "").ToLowerInvariant();
        }

        internal async Task<UploadRecord> LoadUploadAsync(Guid id, CancellationToken token)
        {
            var gate = _uploadLocks.GetOrAdd(id, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var descriptor = ReadDescriptor(UploadPath(id));
                if (descriptor != null) return descriptor;
                // Complete moves the whole directory atomically; retain the
                // descriptor there so a lost Complete ACK survives a restart.
                foreach (var directory in Directory.EnumerateDirectories(_root))
                {
                    if (directory == _uploadsRoot) continue;
                    descriptor = ReadDescriptor(directory);
                    if (descriptor?.UploadId == id) return descriptor;
                }
                return null;
            }
            finally { gate.Release(); }
        }

        private static UploadRecord ReadDescriptor(string directory)
        {
            var path = Path.Combine(directory, "upload.json");
            return File.Exists(path) ? JsonConvert.DeserializeObject<UploadRecord>(File.ReadAllText(path)) : null;
        }

        private static bool SameDescriptor(UploadRecord a, UploadRecord b) =>
            a.UploadId == b.UploadId && a.TaskId == b.TaskId && a.AssignmentId == b.AssignmentId &&
            a.WorkerInstanceId == b.WorkerInstanceId && a.TotalLength == b.TotalLength &&
            a.TotalChunks == b.TotalChunks && a.ChunkSizeBytes == b.ChunkSizeBytes &&
            string.Equals(a.Sha256, b.Sha256, StringComparison.OrdinalIgnoreCase) &&
            a.ContentType == b.ContentType && a.ContentEncoding == b.ContentEncoding &&
            a.FileName == b.FileName && a.ResultKind == b.ResultKind;

        private static void ValidateDescriptor(UploadRecord upload)
        {
            if (upload.UploadId == Guid.Empty || upload.TaskId == Guid.Empty || upload.TotalLength < 0 ||
                upload.ChunkSizeBytes <= 0 || upload.TotalChunks !=
                (upload.TotalLength / upload.ChunkSizeBytes + (upload.TotalLength % upload.ChunkSizeBytes == 0 ? 0 : 1)))
                throw new ResultStoreException("INVALID_REQUEST", "Invalid artifact size or chunk geometry.");
        }

        private async Task BeginPartAsync(UploadRecord upload, CancellationToken token)
        {
            ValidateDescriptor(upload);
            var gate = _uploadLocks.GetOrAdd(upload.UploadId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var path = UploadPath(upload.UploadId);
                var previous = ReadDescriptor(path) ?? ReadDescriptor(ResultPath(upload.TaskId));
                if (previous != null)
                {
                    if (!SameDescriptor(previous, upload))
                        throw new ResultStoreException("TRANSFER_CONFLICT", "Transfer identity has different metadata.");
                    return;
                }
                Directory.CreateDirectory(path);
                // A descriptor is published last; a crash during initialization is
                // recoverable by repeating Begin with the same stable identity.
                using (var part = new FileStream(Path.Combine(path, "result.part"), FileMode.Create, FileAccess.Write, FileShare.None))
                { part.SetLength(upload.TotalLength); part.Flush(true); }
                using (var state = new FileStream(Path.Combine(path, "chunks.state"), FileMode.Create, FileAccess.Write, FileShare.None))
                { state.SetLength((long)upload.TotalChunks * StateRecordBytes); state.Flush(true); }
                var temporary = Path.Combine(path, "upload.tmp");
                File.WriteAllText(temporary, JsonConvert.SerializeObject(upload));
                using (var descriptor = new FileStream(temporary, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) descriptor.Flush(true);
                File.Move(temporary, Path.Combine(path, "upload.json"));
            }
            finally { gate.Release(); }
        }

        internal async Task<int[]> MissingChunksAsync(UploadRecord upload, CancellationToken token)
        {
            var gate = _uploadLocks.GetOrAdd(upload.UploadId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (File.Exists(Path.Combine(ResultPath(upload.TaskId), "metadata.json"))) return Array.Empty<int>();
                using (var state = File.OpenRead(Path.Combine(UploadPath(upload.UploadId), "chunks.state")))
                    return MissingChunks(state, upload.TotalChunks, token);
            }
            finally { gate.Release(); }
        }

        private static int[] MissingChunks(Stream state, int count, CancellationToken token)
        {
            var missing = new List<int>();
            for (var index = 0; index < count; index++)
            {
                token.ThrowIfCancellationRequested();
                state.Position = (long)index * StateRecordBytes;
                if (state.ReadByte() != 1) missing.Add(index);
            }
            return missing.ToArray();
        }

        private async Task<bool> WritePartAsync(UploadRecord upload, int index, byte[] data, CancellationToken token)
        {
            ValidateDescriptor(upload);
            if (index < 0 || index >= upload.TotalChunks)
                throw new ResultStoreException("CHUNK_OUT_OF_RANGE", "Chunk index is outside the artifact.");
            var offset = (long)index * upload.ChunkSizeBytes;
            if (data == null || data.Length != Math.Min(upload.ChunkSizeBytes, upload.TotalLength - offset))
                throw new ResultStoreException("INVALID_REQUEST", "Chunk length does not match its position.");
            var hash = Hash(data);
            var gate = _uploadLocks.GetOrAdd(upload.UploadId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var path = UploadPath(upload.UploadId);
                var descriptor = ReadDescriptor(path);
                if (descriptor == null || !SameDescriptor(descriptor, upload))
                    throw new ResultStoreException("TRANSFER_CONFLICT", "Upload metadata is not current.");
                using (var state = new FileStream(Path.Combine(path, "chunks.state"), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                using (var part = new FileStream(Path.Combine(path, "result.part"), FileMode.Open, FileAccess.ReadWrite, FileShare.None, 81920, true))
                {
                    var position = (long)index * StateRecordBytes;
                    state.Position = position;
                    if (state.ReadByte() == 1)
                    {
                        var bytes = new byte[32];
                        if (state.Read(bytes, 0, bytes.Length) != bytes.Length ||
                            BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant() != hash)
                            throw new ResultStoreException("CHUNK_CONFLICT", "Committed chunk differs from the retry.");
                        return false;
                    }
                    part.Position = offset;
                    await part.WriteAsync(data, 0, data.Length, token).ConfigureAwait(false);
                    part.Flush(true);
                    byte[] digest;
                    using (var sha = SHA256.Create()) digest = sha.ComputeHash(data);
                    state.Position = position + 1;
                    state.Write(digest, 0, digest.Length); state.Flush(true);
                    state.Position = position; state.WriteByte(1); state.Flush(true);
                }
                return true;
            }
            finally { gate.Release(); }
        }

        private long _shaTicks;
        internal double ShaDurationMs=>Interlocked.Read(ref _shaTicks)*1000d/System.Diagnostics.Stopwatch.Frequency;
        private async Task<StoredResultMetadata> CompletePartAsync(UploadRecord upload, DateTime now, CancellationToken token)
        {
            var gate = _uploadLocks.GetOrAdd(upload.UploadId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                var path = UploadPath(upload.UploadId);
                var destination = ResultPath(upload.TaskId);
                var existing = ReadDescriptor(destination);
                if (existing != null)
                {
                    if (!SameDescriptor(existing, upload)) throw new ResultStoreException("TRANSFER_CONFLICT", "A different result is already published.");
                    return JsonConvert.DeserializeObject<StoredResultMetadata>(File.ReadAllText(Path.Combine(destination, "metadata.json")));
                }
                var descriptor = ReadDescriptor(path);
                if (descriptor == null || !SameDescriptor(descriptor, upload))
                    throw new ResultStoreException("TRANSFER_CONFLICT", "Upload metadata is not current.");
                using (var state = File.OpenRead(Path.Combine(path, "chunks.state")))
                    if (MissingChunks(state, upload.TotalChunks, token).Length != 0)
                        throw new ResultStoreException("UPLOAD_INCOMPLETE", "Chunks are missing.");
                // The full pass is deliberate: validates disk contents, including
                // committed chunks recovered after a process interruption.
                var part = Path.Combine(path, "result.part");
                if (!File.Exists(part)) part = Path.Combine(path, "result.bin");
                if (new FileInfo(part).Length != upload.TotalLength)
                    throw new ResultStoreException("INTEGRITY_ERROR", "Artifact length mismatch.");
                var shaStarted=System.Diagnostics.Stopwatch.GetTimestamp();
                string hash;
                try {hash = await CalculateSha256Async(part, token).ConfigureAwait(false);}
                finally {Interlocked.Add(ref _shaTicks,System.Diagnostics.Stopwatch.GetTimestamp()-shaStarted);}
                if (!string.IsNullOrEmpty(upload.Sha256) && !string.Equals(upload.Sha256, hash, StringComparison.OrdinalIgnoreCase))
                    throw new ResultStoreException("INTEGRITY_ERROR", "Artifact SHA-256 mismatch.");
                var metadata = new StoredResultMetadata { TaskId = upload.TaskId, ContentType = upload.ContentType,
                    ContentEncoding = upload.ContentEncoding, TotalLength = upload.TotalLength, TotalChunks = upload.TotalChunks,
                    ChunkSizeBytes = upload.ChunkSizeBytes, Sha256 = hash, CreatedAtUtc = now,
                    FileName = upload.FileName, ResultKind = upload.ResultKind };
                var temporary = Path.Combine(path, "metadata.tmp");
                File.WriteAllText(temporary, JsonConvert.SerializeObject(metadata));
                using (var file = new FileStream(temporary, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) file.Flush(true);
                var final = Path.Combine(path, "metadata.json");
                if (File.Exists(final)) File.Delete(final); // unpublished preparation from a previous interrupted Complete
                File.Move(temporary, final);
                if (part.EndsWith("result.part", StringComparison.Ordinal)) File.Move(part, Path.Combine(path, "result.bin"));
                token.ThrowIfCancellationRequested();
                // Same filesystem: publish data and metadata together. No result
                // reader can observe half of the completed artifact.
                Directory.Move(path, destination);
                return metadata;
            }
            finally { gate.Release(); }
        }
    }
}
