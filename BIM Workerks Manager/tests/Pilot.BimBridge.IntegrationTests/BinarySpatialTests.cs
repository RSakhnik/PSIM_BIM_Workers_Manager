using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using NUnit.Framework;
using WorkerSettings=AdvancedTools.BimBridge.BimBridgeSettings;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class BinarySpatialTests
    {
        internal static readonly Guid Model=Guid.Parse("11111111-1111-4111-8111-111111111111");
        internal static readonly Guid Part=Guid.Parse("22222222-2222-4222-8222-222222222222");
        internal static readonly Guid Element=Guid.Parse("33333333-3333-4333-8333-333333333333");
        public static async Task<PreparedResult> Build(string root,bool geometry=true)
        {
            var settings=new WorkerSettings {ResultsDirectory=root};
            var writer=new ArtifactFileWriter(root,settings);
            return await writer.WriteAsync(Guid.NewGuid(),"application/vnd.pilot.bim-spatial;version=2","spatial.bbsp",false,
                async (stream,token)=>
                {
                    using(var index=new SpatialExportIndex(root,16*1024*1024))
                    using(var spatial=new BinarySpatialWriter(stream,new Source(geometry),index,
                        new ExportSpatialRequest {IncludeDescendants=true,IncludeNormals=true},Model,"2026-01-01T00:00:00Z",_=>true,100,token))
                    {await spatial.WriteSourceAsync(new ElementKey {ModelPartId=Part,ElementId=Element});spatial.Complete();}
                },CancellationToken.None);
        }
        [Test] public async Task BinaryWriterDeduplicatesMeshesAndProducesExactHash()
        {
            var root=Path.Combine(Path.GetTempPath(),"bim-binary-tests",Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
            try
            {
                using(var result=await Build(root))
                {
                    var data=File.ReadAllBytes(result.Path);
                    Assert.That(System.Text.Encoding.ASCII.GetString(data,0,8),Is.EqualTo("BBSPAT2\n"));
                    using(var sha=SHA256.Create())Assert.That(result.Sha256,Is.EqualTo(BitConverter.ToString(sha.ComputeHash(data)).Replace("-","").ToLowerInvariant()));
                    var interop=Environment.GetEnvironmentVariable("BIM_BRIDGE_INTEROP_OUTPUT");
                    if(!string.IsNullOrEmpty(interop))
                    {Directory.CreateDirectory(interop);File.Copy(result.Path,Path.Combine(interop,"spatial.bbsp"),true);File.WriteAllText(Path.Combine(interop,"spatial.sha256"),result.Sha256);}
                }
                Assert.That(Directory.GetFiles(root,"*.spatial-index.db"),Is.Empty);
                using(var result=await Build(root,false))Assert.That(result.Length,Is.GreaterThan(8));
            }
            finally {Directory.Delete(root,true);}
        }
        [Test] public async Task DiskQueueDeduplicatesCyclesAndMeshKeys()
        {
            var root=Path.Combine(Path.GetTempPath(),"bim-index-tests",Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
            try
            {
                using(var index=new SpatialExportIndex(root,1024*1024))
                {
                    var key=new ElementKey {ModelPartId=Part,ElementId=Element};index.BeginRoot(key);index.AddNode(key);
                    Assert.That(index.NextNode().ElementId,Is.EqualTo(Element));Assert.That(index.NextNode(),Is.Null);
                    index.AddNode(key);Assert.That(index.NextNode(),Is.Null);
                    Assert.That(index.AddMesh("mesh"),Is.True);Assert.That(index.AddMesh("mesh"),Is.False);
                }
                await Task.CompletedTask;
            }
            finally {Directory.Delete(root,true);}
        }
        private sealed class Source:ISpatialSource
        {
            private readonly bool _geometry;
            public Source(bool geometry) {_geometry=geometry;}
            public Task<bool> ExistsAsync(ElementKey key,CancellationToken token)=>Task.FromResult(true);
            public Task VisitChildrenAsync(ElementKey key,Func<ElementKey,bool> visit,CancellationToken token)
            {visit(key);return Task.CompletedTask;} // self-cycle must not loop
            public async Task VisitInstancesAsync(ElementKey key,Func<SpatialInstance,Task> visit,CancellationToken token)
            {
                if(!_geometry)return;
                foreach(var offset in new[]{0,10})await visit(new SpatialInstance {MeshPartId=Part,MeshId=Model,
                    Placement=new double[]{1,0,0,offset,0,1,0,0,0,0,1,0,0,0,0,1}});
            }
            public async Task VisitMeshAsync(SpatialInstance instance,Func<string,double[],Task> floats,Func<string,uint[],Task> indices,CancellationToken token)
            {
                await floats("Vertices",new double[]{0,0,0,1.25,0,0,0,2.5,0});
                await floats("Normals",new double[]{0,0,1,0,0,1,0,0,1});
                await indices("Indices",new uint[]{0,1,2});await indices("EdgeIndices",new uint[]{0,1});
            }
        }
    }
}
