using System;
using System.Diagnostics;
using AdvancedTools.BimBridge;
using NUnit.Framework;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class WorkerPerformanceTests
    {
        [Test] public void TaskSnapshotsStayIsolatedWhileProcessTotalsAccumulate()
        {
            var taskA = new SpatialPerformanceMetrics();
            var taskB = new SpatialPerformanceMetrics(true);
            var process = new SpatialPerformanceMetrics();
            taskA.Source();
            taskA.AddEncoding(Stopwatch.Frequency);
            taskA.AddModelAccess(Stopwatch.Frequency / 4);
            taskA.AddSdkDispatchWait(Stopwatch.Frequency / 5);
            taskA.MeshLookup(true);
            taskB.Source();
            taskB.Source();
            taskB.AddArtifactWrite(Stopwatch.Frequency / 2);
            taskB.MeshLookup(false);
            taskB.AddSnapshotIndex(Stopwatch.Frequency / 10, true);
            taskB.AddSnapshotIndex(Stopwatch.Frequency / 10, false);
            process.Add(taskA);
            process.Add(taskB);
            var a = taskA.Snapshot(Guid.NewGuid(), Guid.NewGuid(), 1);
            var b = taskB.Snapshot(Guid.NewGuid(), Guid.NewGuid(), 2);
            Assert.That(a.SourceElements, Is.EqualTo(1));
            Assert.That(b.SourceElements, Is.EqualTo(2));
            Assert.That(process.SourceElements, Is.EqualTo(3));
            Assert.That(a.BinaryEncodingActiveMs, Is.EqualTo(1000).Within(0.001));
            Assert.That(b.BinaryEncodingActiveMs, Is.Zero);
            Assert.That(a.MeshCacheHits, Is.Null);
            Assert.That(a.ModelAccessWallMs, Is.EqualTo(250).Within(0.001));
            Assert.That(a.SdkDispatchWaitWallMs, Is.EqualTo(200).Within(0.001));
            Assert.That(b.SdkDispatchWaitWallMs, Is.Zero);
            Assert.That(a.SnapshotIndexWallMs, Is.Null);
            Assert.That(b.MeshCacheMisses, Is.EqualTo(1));
            Assert.That(b.SnapshotIndexWallMs, Is.EqualTo(200).Within(0.001));
            Assert.That(b.SnapshotIndexHits, Is.EqualTo(1));
            Assert.That(b.SnapshotIndexMisses, Is.EqualTo(1));
            Assert.That(process.ArtifactWriteWallMs, Is.GreaterThan(0));
        }
    }
}
