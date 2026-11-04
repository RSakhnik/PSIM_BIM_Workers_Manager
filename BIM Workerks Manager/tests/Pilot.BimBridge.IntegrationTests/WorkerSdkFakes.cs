// Environment boundary doubles only. The selection reader, filter builder,
// snapshot/property reader, NDJSON writer and cursor implementation are linked
// production code; no Pilot application or service is started by these tests.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ascon.Pilot.Bim.SDK;
using Ascon.Pilot.Bim.SDK.ModelStorage;
using Ascon.Pilot.Bim.SDK.Search;
using Ascon.Pilot.Bim.Search.SDK.Serialization;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace AdvancedTools.BimBridge
{
    public sealed class TaskExecutionContext
    {
        public BimTaskAssignmentDto Assignment {get;set;}
        public CancellationToken CancellationToken {get;set;}
        public Action<int,string> ReportProgress {get;set;}
        public FakeResourceScope Resources {get;set;}=new FakeResourceScope();
    }
    public sealed class FakeResourceScope {public void Track(IDisposable value) {}}
    public interface IBimOperationHandler
    {
        IReadOnlyCollection<string> OperationCodes {get;}
        string Capability {get;}
        bool IsAvailable {get;}
        Task<object> ExecuteAsync(TaskExecutionContext context);
    }
    public interface IModelKeyResolverHandler {}
    public static class BimOperationDispatcher
    {
        public static T ReadPayload<T>(BimTaskAssignmentDto assignment)=>JsonConvert.DeserializeObject<T>(System.Text.Encoding.UTF8.GetString(assignment.Payload));
    }
    public static class SearchElementsOperationHandler
    {
        public static SearchSetState BuildSearchState(SearchElementsRequest request)=>SearchFilters.Build(request);
    }
    public static class SnapshotElementReader
    {
        public static PropertyReadSnapshot ReadProperties(IModelStorage storage,ElementKey key,DateTime version,ISet<string> names,CancellationToken token)
            => SnapshotPropertyReader.ReadProperties(storage,key,version,names,token);
        public static void AssertSearchElement(IModelStorage storage,ElementKey key,DateTime version)
        {if(storage.LoadElement(new ModelElementId(key.ModelPartId,key.ElementId),version)==null)throw new BimBridgeException("SEARCH_INDEX_MISMATCH","injected missing element");}
    }
    public sealed class SearchTargetAccess
    {public SearchTargetAccess(Guid model,IReadOnlyCollection<Guid> parts) {ModelId=model;TargetModelPartIds=parts;}public Guid ModelId;public IReadOnlyCollection<Guid> TargetModelPartIds;}
    public sealed class SearchServiceLease:IDisposable
    {public IModelSearchService Service;public void Dispose()=>Service.Dispose();}
    public sealed class ModelAccess:IDisposable
    {
        private readonly ModelContextProvider _owner;
        public Guid ModelId=>_owner.ModelId;
        public DateTime Version,InitialLatestVersion;
        public string ModelVersionUtc=>Version.ToUniversalTime().ToString("O");
        public IReadOnlyCollection<Guid> TargetModelPartIds {get;}=Array.Empty<Guid>();
        public FakeSdkAdapter Sdk {get;}
        public ModelAccess(ModelContextProvider owner,DateTime version) {_owner=owner;Version=version;InitialLatestVersion=owner.Version;Sdk=new FakeSdkAdapter(owner.Storage);}
        public Task<bool> HasPartAsync(Guid part,CancellationToken token)=>Task.FromResult(_owner.Parts.Contains(part));
        public Task AssertUnchangedAsync(CancellationToken token)
        {token.ThrowIfCancellationRequested();if(_owner.Version!=InitialLatestVersion)throw new BimBridgeException("MODEL_CHANGED","Injected snapshot change.");return Task.CompletedTask;}
        public void Dispose() {}
    }
    public sealed class FakeSdkAdapter
    {
        private readonly IModelStorage _storage;
        public FakeSdkAdapter(IModelStorage storage) {_storage=storage;}
        public Task<T> ReadAsync<T>(Func<IModelStorage,T> read,CancellationToken token,Action<long> dispatchWait=null)
        {token.ThrowIfCancellationRequested();dispatchWait?.Invoke(0);return Task.FromResult(read(_storage));}
        public Task ReleaseOwnedAsync(IDisposable value) {value.Dispose();return Task.CompletedTask;}
    }
    public class ModelContextProvider
    {
        public Guid ModelId=Guid.Parse("11111111-1111-4111-8111-111111111111");
        public Guid[] Parts={Guid.Parse("22222222-2222-4222-8222-222222222222"),Guid.Parse("44444444-4444-4444-8444-444444444444")};
        public DateTime Version=new DateTime(2026,1,1,0,0,0,DateTimeKind.Utc);
        public int ElementsPerPart=5;
        public bool SpatialFixture,MalformedNormals;
        public int SpatialInstancesPerNode=1;
        public int LoadElementCalls,InstanceOpens,ChildOpens,MeshOpens,VertexReads,NormalReads,IndexReads,EdgeReads;
        public Action OnSpatialOpen;
        public readonly Guid SharedMesh=Guid.Parse("55555555-5555-4555-8555-555555555555");
        public long Enumerated;
        public int MissingOrdinal=-1;
        public Action AfterPropertyRead;
        public bool HasModelServices=>true;
        public IModelStorage Storage {get;}
        public ModelContextProvider()
        {
            Storage=FakeProxy.Create<IModelStorage>((method,args)=>
            {
                switch(method.Name)
                {
                    case "GetModelPartsIds":return Parts;
                    case "GetVersions":return new[]{Version};
                    case "LoadElements":return Elements((Guid)args[0]);
                    case "LoadElement":
                        LoadElementCalls++;
                        var id=(IModelElementId)args[0];return id.ElementId==ElementId(MissingOrdinal)?null:Element(id.ModelPartId,id.ElementId);
                    case "LoadElementProperties":AfterPropertyRead?.Invoke();return PropertySets();
                    case "LoadElementMeshInstances":
                        InstanceOpens++;OnSpatialOpen?.Invoke();
                        return SpatialFixture?Enumerable.Repeat(MeshInstance(((IModelElementId)args[0]).ElementId),SpatialInstancesPerNode).ToArray():Array.Empty<IMeshInstance>();
                    case "LoadChildElements":
                        ChildOpens++;OnSpatialOpen?.Invoke();
                        return SpatialFixture&&((IModelElementId)args[0]).ElementId==ElementId(0)
                            ?new[]{Element(((IModelElementId)args[0]).ModelPartId,ElementId(1))}:Array.Empty<IModelElement>();
                    case "LoadMeshDefinition":MeshOpens++;return SpatialFixture?MeshDefinition():null;
                    default:return FakeProxy.Default(method.ReturnType);
                }
            });
        }
        public static Guid ElementId(int number) {var bytes=new byte[16];BitConverter.GetBytes(number+1).CopyTo(bytes,0);return new Guid(bytes);}
        private IEnumerable<IModelElement> Elements(Guid part)
        {for(var number=0;number<ElementsPerPart;number++) {Enumerated++;yield return Element(part,ElementId(number));}}
        private IModelElement Element(Guid part,Guid element)=>FakeProxy.Create<IModelElement>((method,args)=>
            method.Name=="get_Id"?new ModelElementId(part,element):method.Name=="get_Version"?(object)Version:FakeProxy.Default(method.ReturnType));
        private IMeshInstance MeshInstance(Guid element)=>FakeProxy.Create<IMeshInstance>((method,args)=>
        {
            if(method.Name=="get_MeshDefinitionId")return FakeProxy.Create<IMeshDefinitionId>((m,a)=>m.Name=="get_MeshId"?SharedMesh:m.Name=="get_ModelPartId"?Parts[0]:FakeProxy.Default(m.ReturnType));
            if(method.Name=="get_Placement")return element==ElementId(0) ? new Matrix4x4(1,0,0,10,0,1,0,2,0,0,1,3,0,0,0,1) : new Matrix4x4(0,-1,0,20,1,0,0,2,0,0,1,3,0,0,0,1);
            return FakeProxy.Default(method.ReturnType);
        });
        private IMeshDefinition MeshDefinition()=>FakeProxy.Create<IMeshDefinition>((method,args)=>
        {
            switch(method.Name)
            {
                case "get_Vertices":VertexReads++;return new float[]{0,0,0,1,0,0,0,1,0};
                case "get_Normals":NormalReads++;return MalformedNormals?new float[]{0,0,1}:new float[]{0,0,1,0,0,1,0,0,1};
                case "get_Indices":IndexReads++;return new uint[]{0,1,2};
                case "get_EdgeIndices":EdgeReads++;return new uint[]{0,1};
                default:return FakeProxy.Default(method.ReturnType);
            }
        });
        private IEnumerable<IPropertySet> PropertySets()
        {
            IProperty Property(string name,object value)=>FakeProxy.Create<IProperty>((method,args)=>method.Name=="get_Name"?name:method.Name=="get_Value"?value:FakeProxy.Default(method.ReturnType));
            return new[]{FakeProxy.Create<IPropertySet>((method,args)=>method.Name=="get_Name"?"Common":method.Name=="get_Properties"?
                (object)new[]{Property("Name","Pipe"),Property("Mass",12.5)}:FakeProxy.Default(method.ReturnType))};
        }
        public Task<ModelAccess> GetModelAsync(string key,DateTime? version,CancellationToken token)=>Task.FromResult(new ModelAccess(this,version??Version));
        public bool IsPartInScope(Guid model,IReadOnlyCollection<Guid> parts,Guid part)=>model==ModelId&&Parts.Contains(part)&&(parts.Count==0||parts.Contains(part));
        public Guid[] GetKnownModelPartIds(Guid model)=>Parts;
        public CatalogReadiness GetCatalogReadiness()=>new CatalogReadiness {IsComplete=true};
        public Task ResolveModelPartsAsync(Guid model,IEnumerable<Guid> parts,bool fail,CancellationToken token)
        {if(parts.Any(p=>!Parts.Contains(p)))throw new BimBridgeException("PART_NOT_AVAILABLE","Unknown fake part.");return Task.CompletedTask;}
        public Task<SearchServiceLease> GetSearchServiceAsync(SearchTargetAccess access,Guid part,CancellationToken token)
        {
            var service=FakeProxy.Create<IModelSearchService>((method,args)=>method.Name=="Search"?
                (object)Enumerable.Range(0,ElementsPerPart).Select(n=>(IModelElementId)new ModelElementId(part,ElementId(n))):FakeProxy.Default(method.ReturnType));
            return Task.FromResult(new SearchServiceLease {Service=service});
        }
    }
    public sealed class CatalogReadiness {public bool IsComplete;}
    public class FakeProxy:DispatchProxy
    {
        public Func<MethodInfo,object[],object> InvokeMethod;
        protected override object Invoke(MethodInfo method,object[] args)=>InvokeMethod(method,args);
        public static T Create<T>(Func<MethodInfo,object[],object> invoke) where T:class
        {var value=Create<T,FakeProxy>();((FakeProxy)(object)value).InvokeMethod=invoke;return value;}
        public static object Default(Type type)=>type==typeof(void)?null:type.IsValueType?Activator.CreateInstance(type):null;
    }
}
