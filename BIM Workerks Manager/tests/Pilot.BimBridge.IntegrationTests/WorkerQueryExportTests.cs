using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AdvancedTools.BimBridge;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using WorkerException=AdvancedTools.BimBridge.BimBridgeException;
using WorkerSettings=AdvancedTools.BimBridge.BimBridgeSettings;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class WorkerQueryExportTests
    {
        private string _root;
        private ModelContextProvider _models;
        private WorkerSettings _settings;
        [SetUp] public void Setup()
        {_root=Path.Combine(Path.GetTempPath(),"bim-export-tests",Guid.NewGuid().ToString("N"));Directory.CreateDirectory(_root);_models=new ModelContextProvider();_settings=new WorkerSettings {ResultsDirectory=_root};}
        [TearDown] public void Cleanup()=>Directory.Delete(_root,true);
        private TaskExecutionContext Context(object request)=>new TaskExecutionContext {
            Assignment=new BimTaskAssignmentDto {TaskId=Guid.NewGuid(),TargetModelKey=_models.ModelId.ToString("D"),Payload=Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request))}};
        private SearchElementsRequest Query()=>new SearchElementsRequest {TargetModelKey=_models.ModelId.ToString("D"),TargetModelPartIds=new System.Collections.Generic.List<Guid>{_models.Parts[0]},PageSize=2};
        [Test] public async Task ActualCursorCachePagesAreRepeatableAndSnapshotBound()
        {
            var paging=new PagedSearch(_models,_settings,"worker");var request=Query();
            var first=JObject.FromObject(await paging.ExecuteAsync(request,Context(request)));
            Assert.That(first["Elements"].Count(),Is.EqualTo(2));request.Cursor=first.Value<string>("NextCursor");
            var second=JObject.FromObject(await paging.ExecuteAsync(request,Context(request)));
            var repeated=JObject.FromObject(await paging.ExecuteAsync(request,Context(request)));
            Assert.That(JToken.DeepEquals(second,repeated),Is.True);
            request.Cursor=second.Value<string>("NextCursor");
            var last=JObject.FromObject(await paging.ExecuteAsync(request,Context(request)));
            Assert.That(last.Value<bool>("HasMore"),Is.False);Assert.That(last["Elements"].Count(),Is.EqualTo(1));
            _models.Version=_models.Version.AddHours(1);
            var error=Assert.ThrowsAsync<WorkerException>(async()=>await paging.ExecuteAsync(request,Context(request)));
            Assert.That(error.Code,Is.EqualTo("MODEL_CHANGED"));
        }
        [Test] public async Task EmptyAndInvalidCursorPaths()
        {
            var paging=new PagedSearch(_models,_settings,"worker");var request=Query();_models.ElementsPerPart=0;
            var empty=JObject.FromObject(await paging.ExecuteAsync(request,Context(request)));
            Assert.That(empty.Value<bool>("HasMore"),Is.False);Assert.That(empty["Elements"],Is.Empty);
            Assert.That(Directory.GetFiles(_root,"*.selection",SearchOption.AllDirectories),Is.Empty);
            request.Cursor="invalid";
            Assert.That(Assert.ThrowsAsync<WorkerException>(async()=>await paging.ExecuteAsync(request,Context(request))).Code,Is.EqualTo("INVALID_CURSOR"));
        }
        [Test] public async Task ExportPropertiesUsesActualFilterAndStreamsNdjsonAcrossParts()
        {
            _models.ElementsPerPart=3;_models.MissingOrdinal=1;
            var request=new ExportElementsRequest {TargetModelKey=_models.ModelId.ToString("D"),ModelPartIds=_models.Parts.ToList(),
                PropertyNames=new System.Collections.Generic.List<string>{"Name"}};
            var handler=new ExportElementsOperationHandler(_models,_settings,new ArtifactFileWriter(_root,_settings));
            using(var result=(PreparedResult)await handler.ExecuteAsync(Context(request)))
            using(var file=File.OpenRead(result.Path))
            using(var gzip=new GZipStream(file,CompressionMode.Decompress))
            using(var reader=new StreamReader(gzip))
            {
                var records=(await reader.ReadToEndAsync()).Split('\n',StringSplitOptions.RemoveEmptyEntries).Select(JObject.Parse).ToArray();
                Assert.That(records[0].Value<string>("Type"),Is.EqualTo("manifest"));
                var elements=records.Where(r=>r.Value<string>("Type")=="element").ToArray();
                Assert.That(elements.Length,Is.EqualTo(4));
                Assert.That(elements.All(r=>r["PropertySets"][0]["Properties"].Count()==1),Is.True);
                Assert.That(elements.All(r=>r["PropertySets"][0]["Properties"][0].Value<string>("Name")=="Name"),Is.True);
                Assert.That(records.Last().Value<long>("SelectedCount"),Is.EqualTo(6));
                Assert.That(records.Last().Value<long>("FailedCount"),Is.EqualTo(2));
                Assert.That(records.Last().Value<bool>("Complete"),Is.False);
            }
        }
        [Test] public void ChangedSnapshotNeverPublishesArtifact()
        {
            _models.AfterPropertyRead=()=>_models.Version=_models.Version.AddHours(1);
            var request=new ExportElementsRequest {TargetModelKey=_models.ModelId.ToString("D"),ModelPartIds=new System.Collections.Generic.List<Guid>{_models.Parts[0]}};
            var handler=new ExportElementsOperationHandler(_models,_settings,new ArtifactFileWriter(_root,_settings));
            var error=Assert.ThrowsAsync<WorkerException>(async()=>await handler.ExecuteAsync(Context(request)));
            Assert.That(error.Code,Is.EqualTo("MODEL_CHANGED"));Assert.That(Directory.GetFiles(_root,"*.artifact.tmp"),Is.Empty);
        }
        [Test] public async Task LargeSelectionNeverGetsMoreThanOneSdkBatchAheadOfConsumer()
        {
            _models.ElementsPerPart=100000;long consumed=0,maximumAhead=0;
            using(var model=await _models.GetModelAsync(_models.ModelId.ToString("D"),null,CancellationToken.None))
            {
                var count=await new BulkSelectionReader(_models,_settings).VisitAsync(model,new BulkSelectionRequest {ModelPartIds=new System.Collections.Generic.List<Guid>{_models.Parts[0]}},key=>
                {maximumAhead=Math.Max(maximumAhead,_models.Enumerated-consumed);consumed++;return Task.CompletedTask;},CancellationToken.None);
                Assert.That(count,Is.EqualTo(100000));Assert.That(maximumAhead,Is.LessThanOrEqualTo(64));
            }
        }
    }
}
