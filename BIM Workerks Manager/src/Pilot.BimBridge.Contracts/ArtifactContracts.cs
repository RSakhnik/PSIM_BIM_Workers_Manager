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
        [ProtoMember(4)] public double? WorkerProcessingWallMs {get;set;}
        [ProtoMember(5)] public SpatialTaskPerformanceDto SpatialPerformance {get;set;}
        [ProtoMember(6)] public DiagnosticEnvelopeDto Diagnostics {get;set;}
    }

    // The Manager stores this bounded payload without interpreting worker-specific fields.
    [ProtoContract]
    public sealed class DiagnosticEnvelopeDto
    {
        [ProtoMember(1)] public string ContentType {get;set;}
        [ProtoMember(2)] public int SchemaVersion {get;set;}
        [ProtoMember(3)] public byte[] PayloadBytes {get;set;}
        [ProtoMember(4)] public string Sha256 {get;set;}
    }

    [ProtoContract]
    public sealed class SpatialTaskPerformanceDto
    {
        [ProtoMember(1)] public int SchemaVersion {get;set;} = 1;
        [ProtoMember(2)] public Guid TaskId {get;set;}
        [ProtoMember(3)] public Guid AssignmentId {get;set;}
        [ProtoMember(4)] public int AttemptNumber {get;set;}
        [ProtoMember(5)] public long SourceElements {get;set;}
        [ProtoMember(6)] public long TraversedNodes {get;set;}
        [ProtoMember(7)] public long Instances {get;set;}
        [ProtoMember(8)] public long UniqueMeshes {get;set;}
        [ProtoMember(9)] public double SdkTraversalReadMs {get;set;}
        [ProtoMember(10)] public double SdkMeshAcquireMs {get;set;}
        [ProtoMember(11)] public double SdkVerticesReadMs {get;set;}
        [ProtoMember(12)] public double BoundsCpuMs {get;set;}
        [ProtoMember(13)] public long? MeshCacheHits {get;set;}
        [ProtoMember(14)] public long? MeshCacheMisses {get;set;}
        [ProtoMember(15)] public double? BinaryEncodingActiveMs {get;set;}
        [ProtoMember(16)] public double? ArtifactWriteWallMs {get;set;}
        [ProtoMember(17)] public double? ArtifactFinalizeWallMs {get;set;}
        [ProtoMember(18)] public double? ModelAccessWallMs {get;set;}
        [ProtoMember(19)] public double? SnapshotIndexWallMs {get;set;}
        [ProtoMember(20)] public long? SnapshotIndexHits {get;set;}
        [ProtoMember(21)] public long? SnapshotIndexMisses {get;set;}
        [ProtoMember(22)] public double? SdkDispatchWaitWallMs {get;set;}
        [ProtoMember(23)] public double? SpoolPreparationWallMs {get;set;}
        [ProtoMember(24)] public int? WorkerMeshCacheEntries {get;set;}
        [ProtoMember(25)] public long? WorkerMeshCacheBytes {get;set;}
        [ProtoMember(26)] public long? WorkerMeshCacheEvictions {get;set;}
        [ProtoMember(27)] public int? WorkerSnapshotIndexEntries {get;set;}
        [ProtoMember(28)] public long? WorkerSnapshotIndexBytes {get;set;}
        [ProtoMember(29)] public long? WorkerSnapshotIndexEvictions {get;set;}
    }

    [ProtoContract]
    public sealed class ArtifactTransferRequestDto
    {
        [ProtoMember(1)] public Guid TransferId { get; set; }
        [ProtoMember(2)] public Guid RegistrationId { get; set; }
        [ProtoMember(3)] public string WorkerInstanceId { get; set; }
        [ProtoMember(4)] public double? UploadWallMs {get;set;}
        [ProtoMember(5)] public int? UploadRetryCount {get;set;}
        [ProtoMember(6)] public double? UploadRetryWaitMs {get;set;}
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
