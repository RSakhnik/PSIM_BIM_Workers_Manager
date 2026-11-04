# BIM Bridge release candidate: installation and rollback

This document describes preparation and a later operator-led deployment. No
production service, configuration, token or Pilot SDK is changed by the build.
Record exact Git SHAs with `git rev-parse HEAD` in all three checkouts and use
the matching binaries and PSIMpy package together.

| Component | Source | Protocol | Required capabilities |
| --- | --- | --- | --- |
| Manager | `PSIM_BIM_Workers_Manager` release candidate SHA | query 1.0, additive artifact v2 | `artifact-upload-v2`, `artifact-chunk-download-v2` |
| Worker | `AdvancedTools-PSIM_ADMIN-` release candidate SHA | query 1.0, additive artifact v2 | `export-spatial`, `spatial-binary-v2` |
| PSIMpy | `PSIMpy` release candidate SHA | query 1.0, additive artifact v2 | uses Manager health/capability negotiation |

The additive `DiagnosticEnvelopeDto` occupies new protobuf tags only. Current
workers send both the opaque envelope and legacy `SpatialPerformance`; an older
Manager ignores the new field, and the updated Manager accepts an older worker.
Old PSIMpy can still read the legacy status field. Updated PSIMpy prefers a
verified known envelope and falls back to the legacy field. Unknown schemas
remain opaque at the Manager and appear as unavailable to current PSIMpy.
`AverageProcessingWallMsByWorker` remains on the wire for old clients but is
empty: use export profiles for worker processing rates.

## Manager

From `BIM Workerks Manager`, build in an isolated directory:

```powershell
dotnet build Pilot.BimBridge.sln -c Release -p:BimBridgeBuildRoot=<isolated-build-root>
```

Install the contents of `<isolated-build-root>/Pilot.BimBridge.ServerExtension/Release/netstandard2.0`
directly into the existing Pilot-Server `Development` extension directory.
The deployment archive has DLLs and dependency files at its root. It excludes
the sample `Pilot.BimBridge.json`: preserve the existing file beside the DLLs,
especially `Results.Directory`, network binding and token settings. Keep the
durable results directory outside binaries and do not clear it during update.
Stop Pilot-Server gracefully, back up its previous extension directory and
configuration, replace only extension binaries, then start the service. Check
`GetHealthAsync`, registered workers and artifact capabilities. For rollback,
stop the service, restore the previous binaries and unchanged configuration,
then start and check health. Retain results/spool bytes for in-flight transfers.

## BimBridge worker

Build `AdvancedTools/AdvancedTools.csproj` with Visual Studio MSBuild in Release
and override `OutputPath` and `BaseIntermediateOutputPath` to isolated paths;
the project default writes into a live Pilot development directory. Install
the archive contents directly into the existing Pilot-BIM `Development`
extension directory. Keep the existing `%LOCALAPPDATA%/PilotBimBridge/settings.json`,
durable spool and the existing `AdvancedTools.ext2.dll.config`; the archive
excludes that application config. Do not replace Pilot SDK or binary spatial v2.
After Pilot-BIM reload/restart, verify worker registration, model availability
and advertised capabilities. Roll back by restoring the previous extension
binaries while retaining settings and spool.

## PSIMpy

Pin the Git SHA (or an immutable wheel built from it), then install with
`python -m pip install --upgrade "git+https://github.com/RSakhnik/PSIMpy.git@<sha>"`.
Python >=3.10 is required; optional `streaming`, `pandas` and `numpy` extras
are used only for their documented features. Connect with the existing token
or token file, inspect `client.health()` and `client.statistics()`, then run a
small spatial export. Example:

```python
with client.export_spatial_parallel(model_key, destination, element_ids=small_selection) as export:
    export.wait()
    profile = export.profile()
    profile.save_json('profile.json')
    profile.save_html('timeline.html')
```

## Sequence and verification

1. Back up Manager binaries/configuration and worker binaries/settings; preserve durable bytes.
2. Update Manager, then workers, then pin/install PSIMpy.
3. Verify health, registration, capabilities and an idle task queue.
4. Export a small selection, validate the spatial SHA/result and inspect `profile.json`.
5. Run the real performance benchmark only after the control export passes.

The local fake SDK harness covers transfer/resume, diagnostic forwarding and
profile generation. It does not validate Pilot-Server MEF loading, the live
HTTP binding, actual Pilot SDK latency or production memory usage. These are
deployment checks, not claims from the synthetic suite.

## Local validation, 04 November 2026

The isolated Release builds of Manager and worker succeeded. Manager tests:
17 unit and 73 integration passed, including four `TokenHttpAdapterTests` with
loopback access. The separate local HTTP parallel harness passed, as did
the stdio cross-language round-trip. PSIMpy: 145 passed, one skipped (146 total).

Four synthetic runs used a 786432-byte artifact, 12 chunks and injected
20 ms/chunk latency. Median transfer wall time with/without a 788-byte
diagnostic envelope was 456.90/479.75 ms at window 1, and 223.68/217.88 ms
at window 3. All runs made 12 chunk calls. The opposite signs and small sample
do not establish an upload speed change. The envelope has a 64 KiB hard cap;
process RSS and real Pilot throughput were not measured.
