using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class ArtifactStoreTests
    {
        private string _root;
        private FileSystemBimResultStore _store;
        private static readonly CancellationToken Token = CancellationToken.None;
        [SetUp] public void Setup()
        {
            _root = Path.Combine(Path.GetTempPath(), "bim-artifact-tests", Guid.NewGuid().ToString("N"));
            _store = new FileSystemBimResultStore(_root);
        }
        [TearDown] public void Cleanup() { Directory.Delete(_root, true); }
        private static UploadRecord Descriptor(byte[] bytes)
        {
            string hash;
            using (var sha = SHA256.Create()) hash = BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", "").ToLowerInvariant();
            return new UploadRecord { UploadId = Guid.NewGuid(), TaskId = Guid.NewGuid(), AssignmentId = Guid.NewGuid(),
                TotalLength = bytes.Length, ChunkSizeBytes = 4, TotalChunks = (bytes.Length+3)/4, Sha256 = hash,
                ContentType = "application/octet-stream", CreatedAtUtc = DateTime.UtcNow, WorkerInstanceId = "durable-worker" };
        }
        [Test] public async Task LostBeginAndChunkAcksCanBeRetriedAcrossStoreRestart()
        {
            var bytes = new byte[] {1,2,3,4,5,6}; var upload = Descriptor(bytes);
            await _store.BeginUploadAsync(upload, Token);
            _store = new FileSystemBimResultStore(_root);
            await _store.BeginUploadAsync(upload, Token);
            Assert.That(await _store.WriteChunkAsync(upload, 1, bytes.Skip(4).ToArray(), Token), Is.True);
            _store = new FileSystemBimResultStore(_root);
            Assert.That(await _store.WriteChunkAsync(upload, 1, bytes.Skip(4).ToArray(), Token), Is.False);
            Assert.That(await _store.MissingChunksAsync(upload, Token), Is.EqualTo(new[]{0}));
            await _store.WriteChunkAsync(upload, 0, bytes.Take(4).ToArray(), Token);
            var result = await _store.CompleteUploadAsync(upload, 4, DateTime.UtcNow, Token);
            Assert.That(result.Sha256, Is.EqualTo(upload.Sha256));
            Assert.That(await _store.ReadChunkAsync(upload.TaskId, 1, 4, Token), Is.EqualTo(new byte[]{5,6}));
            Assert.That(Directory.GetFiles(_root, "chunk-*.bin", SearchOption.AllDirectories), Is.Empty);
            // Lost Complete ACK: publication is idempotent too.
            await _store.CompleteUploadAsync(upload, 4, DateTime.UtcNow, Token);
        }
        [Test] public async Task ConflictingChunkAndDescriptorAreRejected()
        {
            var upload = Descriptor(new byte[]{1,2,3,4});
            await _store.BeginUploadAsync(upload, Token);
            await _store.WriteChunkAsync(upload, 0, new byte[]{1,2,3,4}, Token);
            var conflict = Assert.ThrowsAsync<ResultStoreException>(async () =>
                await _store.WriteChunkAsync(upload, 0, new byte[]{4,3,2,1}, Token));
            Assert.That(conflict.Code, Is.EqualTo("CHUNK_CONFLICT"));
            upload.ContentEncoding = "gzip";
            conflict = Assert.ThrowsAsync<ResultStoreException>(async () => await _store.BeginUploadAsync(upload, Token));
            Assert.That(conflict.Code, Is.EqualTo("TRANSFER_CONFLICT"));
        }
        [Test] public async Task IncompleteAndCorruptArtifactsNeverPublish()
        {
            var upload = Descriptor(new byte[]{1,2,3,4,5});
            await _store.BeginUploadAsync(upload, Token);
            await _store.WriteChunkAsync(upload, 0, new byte[]{1,2,3,4}, Token);
            var error = Assert.ThrowsAsync<ResultStoreException>(async () => await _store.CompleteUploadAsync(upload,4,DateTime.UtcNow,Token));
            Assert.That(error.Code, Is.EqualTo("UPLOAD_INCOMPLETE"));
            await _store.WriteChunkAsync(upload, 1, new byte[]{6}, Token);
            error = Assert.ThrowsAsync<ResultStoreException>(async () => await _store.CompleteUploadAsync(upload,4,DateTime.UtcNow,Token));
            Assert.That(error.Code, Is.EqualTo("INTEGRITY_ERROR"));
            Assert.That(await _store.GetMetadataAsync(upload.TaskId,Token), Is.Null);
        }
        [Test] public async Task UncommittedPartialWriteIsOverwrittenOnResume()
        {
            var upload = Descriptor(new byte[]{1,2,3,4});
            await _store.BeginUploadAsync(upload, Token);
            File.WriteAllBytes(Path.Combine(_root,".uploads",upload.UploadId.ToString("N"),"result.part"),new byte[]{9,9,9,9});
            _store = new FileSystemBimResultStore(_root);
            await _store.WriteChunkAsync(upload,0,new byte[]{1,2,3,4},Token);
            await _store.CompleteUploadAsync(upload,4,DateTime.UtcNow,Token);
        }
        [Test] public async Task EmptyArtifactAndExpiredTransfer()
        {
            var upload = Descriptor(Array.Empty<byte>());
            await _store.BeginUploadAsync(upload,Token);
            await _store.CompleteUploadAsync(upload,4,DateTime.UtcNow,Token);
            upload = Descriptor(new byte[]{1});
            await _store.BeginUploadAsync(upload,Token);
            await _store.CleanupOrphansAsync(DateTime.UtcNow.AddDays(2),TimeSpan.FromDays(1),TimeSpan.FromDays(3),Token);
            Assert.That(await _store.LoadUploadAsync(upload.UploadId,Token),Is.Null);
        }
    }
}
