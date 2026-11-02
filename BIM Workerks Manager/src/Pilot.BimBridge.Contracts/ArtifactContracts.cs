using System;
using System.Threading.Tasks;
using ProtoBuf;

namespace Pilot.BimBridge.Contracts
{
    public static class ArtifactProtocol
    {
        public const string Revision = "2.0";
        public const string ResumableUpload = "artifact-upload-v2";
        public const string ChunkDownload = "artifact-chunk-download-v2";
        public const string PagedSearch = "paged-search-v2";
        public const string ExportElements = "export-elements";
        public const string ExportSpatial = "export-spatial";
        public const string BinarySpatial = "spatial-binary-v2";
        public const string ResultPrepared = "RESULT_PREPARED";
        public const string ResultUploading = "RESULT_UPLOADING";
    }

    // Separate interface: old Pilot custom API proxies and protocol 1.0 remain valid.
    public interface IBimBridgeArtifactApi
    {
        Task<ArtifactUploadStatusDto> ReserveArtifactAsync(ReserveArtifactDto request);
        Task<ArtifactUploadStatusDto> BeginOrResumeUploadAsync(BeginArtifactUploadDto request);
        Task<ArtifactUploadStatusDto> GetUploadStatusAsync(ArtifactTransferRequestDto request);
        Task<ArtifactChunkResponseDto> WriteArtifactChunkAsync(ArtifactChunkRequestDto request);
        Task<CompleteTaskResponseDto> CompleteArtifactAsync(ArtifactTransferRequestDto request);
    }

    [ProtoContract]
    public sealed class ReserveArtifactDto
    {
        [ProtoMember(1)] public ArtifactTransferRequestDto Transfer { get; set; }
        [ProtoMember(2)] public Guid TaskId { get; set; }
        [ProtoMember(3)] public Guid AssignmentId { get; set; }
    }

    [ProtoContract]
    public sealed class BeginArtifactUploadDto
    {
        [ProtoMember(1)] public Guid TransferId { get; set; }
        [ProtoMember(2)] public string WorkerInstanceId { get; set; }
        [ProtoMember(3)] public BeginResultUploadRequestDto Descriptor { get; set; }
    }

    [ProtoContract]
    public sealed class ArtifactTransferRequestDto
    {
        [ProtoMember(1)] public Guid TransferId { get; set; }
        [ProtoMember(2)] public Guid RegistrationId { get; set; }
        [ProtoMember(3)] public string WorkerInstanceId { get; set; }
    }

    [ProtoContract]
    public sealed class ArtifactChunkRequestDto
    {
        [ProtoMember(1)] public ArtifactTransferRequestDto Transfer { get; set; }
        [ProtoMember(2)] public int ChunkIndex { get; set; }
        [ProtoMember(3)] public byte[] Data { get; set; }
        [ProtoMember(4)] public string Sha256 { get; set; }
    }

    [ProtoContract]
    public sealed class MissingChunkRangeDto
    {
        [ProtoMember(1)] public int Start { get; set; }
        [ProtoMember(2)] public int Count { get; set; }
    }

    [ProtoContract]
    public sealed class ArtifactUploadStatusDto : BimBridgeResponseDto
    {
        [ProtoMember(1)] public Guid TransferId { get; set; }
        [ProtoMember(2)] public int ChunkSizeBytes { get; set; }
        [ProtoMember(3)] public MissingChunkRangeDto[] MissingRanges { get; set; }
        [ProtoMember(4)] public bool Completed { get; set; }
        [ProtoMember(5)] public DateTime ExpiresAtUtc { get; set; }
    }

    [ProtoContract]
    public sealed class ArtifactChunkResponseDto : BimBridgeResponseDto
    {
        [ProtoMember(1)] public int ChunkIndex { get; set; }
        [ProtoMember(2)] public bool AlreadyPresent { get; set; }
    }
}
