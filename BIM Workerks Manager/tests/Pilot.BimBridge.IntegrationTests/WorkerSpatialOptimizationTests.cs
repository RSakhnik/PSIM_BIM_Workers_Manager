using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using NUnit.Framework;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class WorkerSpatialOptimizationTests
    {
        private static async Task<byte[]> Export(ModelContextProvider provider,SnapshotElementIndexCache indexCache,
            MeshGeometryCache meshCache,bool coalesced,bool includeMeshes,CancellationToken token,SpatialPerformanceMetrics metrics=null)
        {
            var root=Path.Combine(Path.GetTempPath(),"worker-spatial-opt",Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
            try
            {
                using(var model=await provider.GetModelAsync(null,null,token))
                using(var result=await new ArtifactFileWriter(root,new BimBridgeSettings{ResultsDirectory=root}).WriteAsync(
                    Guid.NewGuid(),"application/vnd.pilot.bim-spatial;version=2","spatial.bbsp",false,
                    async (stream,cancel)=>
                    {
                        using(var disk=new SpatialExportIndex(root,16*1024*1024))
                        using(var writer=new BinarySpatialWriter(stream,
                            new SpatialSdkSource(model,includeMeshes,includeMeshes,includeMeshes,indexCache,coalesced,meshCache,metrics),disk,
                            new ExportSpatialRequest{IncludeDescendants=true,IncludeMeshes=includeMeshes,IncludeNormals=includeMeshes,
                                IncludeIndices=includeMeshes,IncludeEdgeIndices=includeMeshes},
                            provider.ModelId,model.ModelVersionUtc,_=>true,100,cancel,metrics))
                        {
                            await writer.WriteSourceAsync(new ElementKey{ModelPartId=provider.Parts[0],ElementId=ModelContextProvider.ElementId(0)});
                            writer.Complete();
                        }
                    },token))return File.ReadAllBytes(result.Path);
            }
            finally{Directory.Delete(root,true);}
        }
        [Test]public async Task CoalescedIndexAndMeshReuseMatchLegacyBinaryBytesForBoundsAndFullMesh()
        {
            foreach(var full in new[]{false,true})
            {
                var old=new ModelContextProvider{SpatialFixture=true};var baseline=await Export(old,null,null,false,full,CancellationToken.None);
                var optimized=new ModelContextProvider{SpatialFixture=true};var index=new SnapshotElementIndexCache(1024*1024,2);
                var cache=new MeshGeometryCache(1024*1024,20);var metrics=new SpatialPerformanceMetrics();
                var actual=await Export(optimized,index,cache,true,full,CancellationToken.None,metrics);
                Assert.That(actual,Is.EqualTo(baseline),"Binary bytes must retain exact bounds, placements, ordering and mesh definitions.");
                Assert.That(old.LoadElementCalls,Is.EqualTo(2));Assert.That(optimized.LoadElementCalls,Is.Zero);
                Assert.That(old.MeshOpens,Is.EqualTo(2));Assert.That(optimized.MeshOpens,Is.EqualTo(1));
                Assert.That(optimized.VertexReads,Is.EqualTo(1));Assert.That(optimized.ChildOpens,Is.EqualTo(2));
                Assert.That(optimized.InstanceOpens,Is.EqualTo(2));Assert.That(optimized.Enumerated,Is.EqualTo(5));
                Assert.That(cache.Hits,Is.EqualTo(1));Assert.That(cache.Misses,Is.EqualTo(1));
                Assert.That(metrics.SourceElements,Is.EqualTo(1));Assert.That(metrics.TraversedNodes,Is.EqualTo(2));
                Assert.That(metrics.Instances,Is.EqualTo(2));Assert.That(metrics.UniqueMeshes,Is.EqualTo(1));
                Assert.That(metrics.SdkTraversalReadMs,Is.GreaterThan(0));Assert.That(metrics.SdkMeshAcquireMs,Is.GreaterThan(0));
                Assert.That(metrics.SdkVerticesReadMs,Is.GreaterThan(0));Assert.That(metrics.BoundsCpuMs,Is.GreaterThan(0));
                Assert.That(optimized.NormalReads,Is.EqualTo(full?1:0));Assert.That(optimized.IndexReads,Is.EqualTo(full?1:0));
                Assert.That(optimized.EdgeReads,Is.EqualTo(full?1:0));
            }
        }
        [Test]public async Task WideNodeFallsBackToBoundedLegacyCursorWithoutLosingInstances()
        {
            var baselineProvider=new ModelContextProvider{SpatialFixture=true,SpatialInstancesPerNode=17};
            var optimizedProvider=new ModelContextProvider{SpatialFixture=true,SpatialInstancesPerNode=17};
            var expected=await Export(baselineProvider,null,null,false,false,CancellationToken.None);
            var actual=await Export(optimizedProvider,new SnapshotElementIndexCache(1024*1024,2),
                new MeshGeometryCache(1024*1024,20),true,false,CancellationToken.None);
            Assert.That(actual,Is.EqualTo(expected));Assert.That(optimizedProvider.LoadElementCalls,Is.Zero);
            Assert.That(optimizedProvider.InstanceOpens,Is.EqualTo(4)); // Probe then original cursor per node.
        }
        [Test]public async Task MeshCachePromotesBoundsVerticesToFullMeshWithoutRereadingVertices()
        {
            var provider=new ModelContextProvider{SpatialFixture=true};var index=new SnapshotElementIndexCache(1024*1024,2);
            var cache=new MeshGeometryCache(1024*1024,20);
            await Export(provider,index,cache,true,false,CancellationToken.None);
            Assert.That(provider.MeshOpens,Is.EqualTo(1));Assert.That(provider.VertexReads,Is.EqualTo(1));
            var expected=await Export(new ModelContextProvider{SpatialFixture=true},null,null,false,true,CancellationToken.None);
            var actual=await Export(provider,index,cache,true,true,CancellationToken.None);
            Assert.That(actual,Is.EqualTo(expected));Assert.That(provider.VertexReads,Is.EqualTo(1));
            Assert.That(provider.NormalReads+provider.IndexReads+provider.EdgeReads,Is.EqualTo(3));
            Assert.That(index.Builds,Is.EqualTo(1));Assert.That(cache.Entries,Is.EqualTo(1));
        }
        [Test]public void InvalidGeometryNeverEntersWorkerCache()
        {
            var provider=new ModelContextProvider{SpatialFixture=true,MalformedNormals=true};
            var cache=new MeshGeometryCache(1024*1024,20);
            Assert.ThrowsAsync<BimBridgeException>(async()=>await Export(provider,
                new SnapshotElementIndexCache(1024*1024,2),cache,true,true,CancellationToken.None));
            Assert.That(cache.Entries,Is.Zero);
        }
        [Test]public void CoalescedNodeCancellationClosesSdkCursorsAndDoesNotPublish()
        {
            var provider=new ModelContextProvider{SpatialFixture=true};var cancellation=new CancellationTokenSource();provider.OnSpatialOpen=cancellation.Cancel;
            Assert.ThrowsAsync<OperationCanceledException>(async()=>await Export(provider,
                new SnapshotElementIndexCache(1024*1024,2),new MeshGeometryCache(1024*1024,20),true,false,cancellation.Token));
        }
        [Test]public void MeshCacheIsVersionScopedBoundedAndEvictsOldEntries()
        {
            var provider=new ModelContextProvider();var cache=new MeshGeometryCache(1024,1);
            var a=new ModelAccess(provider,provider.Version);var newer=new ModelAccess(provider,provider.Version.AddMinutes(1));
            var one=MeshGeometryCache.Key(a,provider.Parts[0],provider.SharedMesh);
            var two=MeshGeometryCache.Key(newer,provider.Parts[0],provider.SharedMesh);
            Assert.That(one,Is.Not.EqualTo(two));
            var geo=new MeshGeometryCache.Geometry{Vertices=new System.Collections.Generic.List<double[]>{new double[9]}};
            cache.Put(one,geo);Assert.That(cache.Find(one,false,false,false),Is.Not.Null);
            cache.Put(two,geo);Assert.That(cache.Entries,Is.EqualTo(1));Assert.That(cache.Find(one,false,false,false),Is.Null);
            Assert.That(cache.Bytes,Is.LessThanOrEqualTo(1024));
            cache.Put(one,new MeshGeometryCache.Geometry{Vertices=new System.Collections.Generic.List<double[]>{new double[1000]}});
            Assert.That(cache.Entries,Is.EqualTo(1));
        }
    }
}
