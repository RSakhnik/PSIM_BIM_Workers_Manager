// Test-only local stdio boundary. Uses real Manager/store/export writers and
// uploader with fake SDK environment. Never connects to a Pilot server.
using System.Security.Cryptography;
using AdvancedTools.BimBridge;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.IntegrationTests;
using Pilot.BimBridge.ServerExtension;
using WorkerSettings=AdvancedTools.BimBridge.BimBridgeSettings;

var root=Environment.GetEnvironmentVariable("BIM_BRIDGE_HARNESS_ROOT")??throw new InvalidOperationException("Explicit harness root required.");
Directory.CreateDirectory(root);
if(args.Length>0&&args[0]=="--parallel") {await ParallelHarness.RunAsync(root,args[1],int.Parse(args[2]));return;}
var options=new BimBridgeOptions();options.Results.Directory=Path.Combine(root,"results");options.Results.ChunkSizeBytes=128;
using var runtime=new BimBridgeRuntime(options,new SystemClock(),new FileSystemBimResultStore(options.Results.Directory),new Log(),false);
var models=new ModelContextProvider {ElementsPerPart=100};
var registration=runtime.Register(new RegisterWorkerRequestDto {ProtocolVersion="1.0",WorkerInstanceId="harness-worker",
    Capabilities=new[]{"export-elements","export-spatial","spatial-binary-v2","resolve-model-key"},AvailableModelKeys=new[]{models.ModelId.ToString("D")}}).RegistrationId;
var settings=new WorkerSettings {ResultsDirectory=Path.Combine(root,"worker")};
Directory.CreateDirectory(settings.ResultsDirectory);
var output=Console.Out;Console.SetOut(Console.Error);
string line;
while((line=Console.ReadLine())!=null)
{
    try
    {
        var request=JObject.Parse(line);object response;
        switch(request.Value<string>("Method"))
        {
            case "Connect":response=new {Connected=true};break;
            case "GetHealth":response=await runtime.GetHealthAsync(CancellationToken.None);break;
            case "GetWorkers":response=runtime.GetWorkers();break;
            case "GetStatistics":response=runtime.GetStatistics();break;
            case "GetTaskStatus":response=runtime.GetTaskStatus(Guid.Parse(request.Value<string>("TaskId")));break;
            case "GetResultMetadata":response=await runtime.GetResultMetadataAsync(Guid.Parse(request.Value<string>("TaskId")),CancellationToken.None);break;
            case "CancelTask":response=runtime.Cancel(Guid.Parse(request.Value<string>("TaskId")));break;
            case "ReadResultChunk":
                if(request.Value<int>("ChunkIndex")==1&&!File.Exists(Path.Combine(root,"download-fault")))
                {File.WriteAllText(Path.Combine(root,"download-fault"),"1");response=new {Error=new {Code="PILOT_TRANSPORT_ERROR",Message="injected interrupted chunk read",IsRetryable=true}};}
                else response=await runtime.ReadResultChunkAsync(Guid.Parse(request.Value<string>("TaskId")),request.Value<int>("ChunkIndex"),CancellationToken.None);
                break;
            case "SubmitTask":
                var submit=request["Request"].ToObject<SubmitBimTaskRequestDto>();
                var accepted=runtime.Submit(submit);response=accepted;
                if(accepted.IsSuccess&&runtime.GetTaskStatus(accepted.TaskId).State==BimTaskStates.Queued)
                {
                    var assigned=await runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=registration,WaitTimeoutSeconds=1},CancellationToken.None);
                    runtime.StartTask(new StartTaskRequestDto {RegistrationId=registration,TaskId=accepted.TaskId,AssignmentId=assigned.Assignment.AssignmentId});
                    var countPath=Path.Combine(root,"extraction-count");
                    var count=File.Exists(countPath)?int.Parse(File.ReadAllText(countPath)):0;File.WriteAllText(countPath,(count+1).ToString());
                    using var spool=new DurableArtifactSpool(settings,"harness-worker",null);
                    var reservation=spool.Reserve(assigned.Assignment);
                    var uploader=new ResumableArtifactUploader(new FaultingClient(runtime),settings,null);
                    await uploader.ReserveAsync(reservation,registration,CancellationToken.None);
                    using var result=submit.OperationCode=="export-spatial"?await BinarySpatialTests.Build(settings.ResultsDirectory):
                        (PreparedResult)await new ExportElementsOperationHandler(models,settings,new ArtifactFileWriter(settings.ResultsDirectory,settings))
                            .ExecuteAsync(new TaskExecutionContext {Assignment=assigned.Assignment});
                    if(submit.OperationCode=="export-spatial")
                        result.SpatialPerformance=new SpatialTaskPerformanceDto {TaskId=accepted.TaskId,
                            AssignmentId=assigned.Assignment.AssignmentId,AttemptNumber=1,SourceElements=1,
                            TraversedNodes=1,Instances=1,UniqueMeshes=1};
                    var artifact=spool.Prepare(result,assigned.Assignment,options.Results.ChunkSizeBytes,reservation);
                    await uploader.UploadAsync(artifact,registration,CancellationToken.None);spool.Remove(artifact);
                }
                break;
            default:throw new InvalidOperationException("Unknown harness command.");
        }
        await output.WriteLineAsync(JsonConvert.SerializeObject(response));await output.FlushAsync();
    }
    catch(Exception error)
    {
        await output.WriteLineAsync(JsonConvert.SerializeObject(new {Error=new {Code="HARNESS_ERROR",Message=error.ToString(),IsRetryable=false}}));await output.FlushAsync();
    }
}
sealed class Log:Pilot.BimBridge.ServerExtension.IBimBridgeLog
{public void Info(string message){}public void Warning(string message){}public void Error(string message,Exception error){}}
sealed class FaultingClient:IBimBridgeArtifactClient
{
    private readonly BimBridgeRuntime _runtime;private bool _lostBegin,_lostChunk;
    public FaultingClient(BimBridgeRuntime runtime) {_runtime=runtime;}
    public Task<ArtifactUploadStatusDto> ReserveArtifactAsync(ReserveArtifactDto request)=>_runtime.ReserveArtifactAsync(request,CancellationToken.None);
    public async Task<ArtifactUploadStatusDto> BeginOrResumeAsync(BeginArtifactUploadDto request)
    {var response=await _runtime.BeginOrResumeArtifactAsync(request,CancellationToken.None);if(!_lostBegin) {_lostBegin=true;throw new IOException("injected lost Begin ACK");}return response;}
    public Task<ArtifactUploadStatusDto> UploadStatusAsync(ArtifactTransferRequestDto request)=>_runtime.ArtifactStatusAsync(request,CancellationToken.None);
    public async Task<ArtifactChunkResponseDto> WriteArtifactChunkAsync(ArtifactChunkRequestDto request)
    {
        var response=await _runtime.WriteArtifactChunkAsync(request,CancellationToken.None);
        if(request.ChunkIndex==0&&!_lostChunk) {_lostChunk=true;throw new IOException("injected lost chunk ACK");}return response;
    }
    public Task<CompleteTaskResponseDto> CompleteArtifactAsync(ArtifactTransferRequestDto request)=>_runtime.CompleteArtifactAsync(request,CancellationToken.None);
}
