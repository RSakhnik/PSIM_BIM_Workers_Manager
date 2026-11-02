# ADR 0002 — Separate queries from durable bulk artifacts

Status: implemented as additive v2 capabilities alongside protocol 1.0.

## Decision

Small query operations remain JSON tasks. Mass exports execute selection and
reading on the worker and create a durable artifact before delivery. Consumers
do not download millions of IDs merely to send them back as property requests.
Properties use independently readable NDJSON records; geometry uses framed
binary arrays. A later columnar format can be introduced behind the artifact
writer/result abstraction without changing task delivery.

## Transport boundary

Manager is an `IServerApiFactory` Pilot-Server extension. Its network boundary is
Pilot Custom API, not a managed ASP.NET REST service. HttpPilotClient is the
vendor transport. No Kestrel/REST facade is added. PSIMpy's direct transport uses
a local stdio adapter to HttpPilotClient and the existing task/result APIs.

Existing v1 interfaces/methods and protobuf field numbers are retained. New
worker delivery methods live on separate `IBimBridgeArtifactApi`:

1. ReserveArtifact: stable TransferId/worker/task/assignment intent before SDK work.
2. BeginOrResumeUpload: immutable descriptor, including total bytes/chunks/hash.
3. GetUploadStatus: compact missing index ranges and completion/retention status.
4. WriteArtifactChunk: index, payload and SHA256. Same bytes are AlreadyPresent;
   different bytes at a committed index produce CHUNK_CONFLICT.
5. CompleteArtifact: completeness, full size/SHA verification and atomic publication.

Health capabilities advertise artifact-upload-v2 and artifact-chunk-download-v2.
Worker capabilities advertise paged-search-v2, export-elements, export-spatial
and spatial-binary-v2. Feature support is negotiated before invoking new APIs.

## State and retention

Execution lease and delivery retention are separate. Prepared results transition
to RESULT_PREPARED / RESULT_UPLOADING, then COMPLETED. A new worker registration
for the same durable identity may resume an existing transfer. The reservation
also authorizes a locally ready artifact whose first Begin was lost before
server-side preparation. A changed descriptor cannot reuse the transfer ID.

Storage layout:

```text
.uploads/<transfer-id>/upload.json
.uploads/<transfer-id>/result.part
.uploads/<transfer-id>/chunks.state
.reservations/<transfer-id>.json
.task-states/<task-id>.json
<task-id>/result.bin
<task-id>/metadata.json
```

Each chunk state record is a committed presence byte plus 32-byte SHA256.
Writes occur directly at index × ChunkSizeBytes. Data is flushed before hash,
and hash before presence. An uncommitted partial write can safely be overwritten.
Complete verifies the full on-disk hash and atomically renames the prepared
directory to the published task directory, so readers cannot see half a result.
There is no chunk-file concatenation pass. Retrying Complete after a lost ACK
returns the already published metadata. Terminal task state/tombstones protect
cancellation and integrity failures across runtime restarts.

Upload buffers are bounded by the worker's configured in-flight window and
chunk limit. The result artifact remains local until confirmation. Retries use
bounded exponential backoff/jitter; pending timed-out calls retain immutable
buffers. Quotas bound concurrent transfers, artifact size, disk bytes, records,
selection storage and geometry traversal scratch space.

## Download

GetResultMetadataAsync identifies immutable content using TaskId, TotalLength,
TotalChunks, ChunkSizeBytes, SHA256 and content type/encoding. ReadResultChunkAsync
is deterministic. PSIMpy saves a flushed contiguous prefix and durable state,
rechecks metadata and local prefix hash after restart, reads the first missing
chunk, then verifies the full hash before publishing the destination. HTTP Range,
206 and REST ETag semantics are unnecessary for this transport.

## Tradeoffs

- First paged search materializes a bounded on-disk identifier selection; later
  pages do not depend on SDK offset ordering. Cursors are worker-affine and expire.
- Artifact preparation needs disk space and does not expose a partially formed
  export as completed. Resume avoids repeating completed extraction, rather
  than attempting arbitrary restart inside a native SDK call.
- Server Complete performs one full hash pass for crash/disk correctness.
- BBSPAT2 is engineering metadata plus typed binary blocks, not a rendering
  interchange format. Scene/geodetic coordinates remain unverified.
- Native Pilot authentication/SDK execution is not exercised by offline tests.
  The user explicitly prohibited starting/reloading their working Pilot.


## 2026-11-02 compatibility correction

The actual existing Colab notebook uses token-authenticated `/bim-bridge` and the
deployed configuration contains RestApi. That HTTP binding was absent from source
and unintentionally omitted from the initial v2 package. Restore the opt-in
TokenHttpAdapter within the existing Manager extension/runtime, rather than require
new Pilot login credentials or a separate server. Native Custom API and upload
protocol remain unchanged. Resume uses metadata/chunk calls through either binding,
not HTTP Range. Local integration tests cover token authentication, JSON submission,
cancellation, full download and deterministic chunk responses. Live installation is
manual; no service is restarted by packaging/tests.
