// Offline integration host. Reuses the existing Manager token adapter; never connects to Pilot.
using System.Collections.Concurrent;
using System.Diagnostics;
using AdvancedTools.BimBridge;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;
using Settings=AdvancedTools.BimBridge.BimBridgeSettings;

internal static class ParallelHarness
{
    private static readonly Guid Model=Guid.Parse("11111111-1111-4111-8111-111111111111");
    private static readonly Guid Part=Guid.Parse("22222222-2222-4222-8222-222222222222");
    public static async Task RunAsync(string root,string prefix,int count)
    {
        var options=new BimBridgeOptions();options.Results.Directory=Path.Combine(root,"manager");options.Results.ChunkSizeBytes=512;
        options.RestApi=new RestApiOptions {Enabled=true,Prefix=prefix,Token="parallel-test-only-token",MaxRequestBytes=1048576};
        using var runtime=new BimBridgeRuntime(options,new SystemClock(),new FileSystemBimResultStore(options.Results.Directory),new Log(),false);
        using var adapter=new TokenHttpAdapter(options.RestApi,runtime,new Log());
        using var stop=new CancellationTokenSource();
        var errors=new ConcurrentQueue<string>();var metrics=new ConcurrentDictionary<int,WorkerMetrics>();
        var loops=Enumerable.Range(0,count).Select(i=>Task.Run(()=>WorkerAsync(i,root,runtime,metrics,errors,stop.Token))).ToArray();
        while(metrics.Count<count)await Task.Delay(10);
        adapter.Start();Console.WriteLine(JsonConvert.SerializeObject(new {Ready=true,Prefix=prefix}));
        try
        {
            string command;
            while((command=await Task.Run(()=>Console.ReadLine()))!=null && command!="stop")
            {
                if(command=="metrics")Console.WriteLine(JsonConvert.SerializeObject(new {Manager=runtime.GetStatistics(),Workers=metrics,Errors=errors.ToArray()}));
            }
        }
        finally {stop.Cancel();await Task.WhenAll(loops);}
    }
    private sealed class WorkerMetrics
    {
        public int MaxSdkConcurrency,MaxPreparedDepth,UploadOverlapStarts,Started,Completed;
    }
    private static async Task WorkerAsync(int number,string root,BimBridgeRuntime runtime,
        ConcurrentDictionary<int,WorkerMetrics> metrics,ConcurrentQueue<string> errors,CancellationToken token)
    {
        var name="fake-machine:user:durable-"+number;
        var settings=new Settings {ResultsDirectory=Path.Combine(root,"worker-"+number),DurableWorkerId="durable-"+number,
            MaxArtifactBytes=1048576,MaxPreparedArtifactBytes=2097152,ArtifactDiskQuotaBytes=2097152};
        Directory.CreateDirectory(settings.ResultsDirectory);
        using var spool=new DurableArtifactSpool(settings,name,null);
        var registration=runtime.Register(new RegisterWorkerRequestDto {WorkerInstanceId=name,ProtocolVersion="1.0",
            AvailableModelKeys=new[]{Model.ToString("D")},Capabilities=new[]{ArtifactProtocol.ExportSpatial,ArtifactProtocol.BinarySpatial}}).RegistrationId;
        var stats=new WorkerMetrics();metrics[number]=stats;
        var uploading=0;var sdk=0;
        var pipeline=new PreparedArtifactPipeline(spool,settings,async(artifact,t)=> {
            var uploader=new ResumableArtifactUploader(new Client(runtime),settings,null);
            await uploader.PrepareAsync(artifact,registration,t);spool.MarkPrepared(artifact);
            Interlocked.Increment(ref uploading);
            try {await Task.Delay(70,t);await uploader.UploadAsync(artifact,registration,t);Interlocked.Increment(ref stats.Completed);}
            finally{Interlocked.Decrement(ref uploading);}
        },error=>errors.Enqueue(error.ToString()));
        using var deliveryStop=CancellationTokenSource.CreateLinkedTokenSource(token);
        var delivery=Task.Run(()=>pipeline.RunAsync(deliveryStop.Token));
        try
        {
            while(!token.IsCancellationRequested)
            {
                await pipeline.WaitForCapacityAsync(token);
                var next=await runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=registration,WaitTimeoutSeconds=1},token);
                if(!next.HasTask)continue;
                var assignment=next.Assignment;
                var started=runtime.StartTask(new StartTaskRequestDto {RegistrationId=registration,TaskId=assignment.TaskId,AssignmentId=assignment.AssignmentId});
                if(!started.IsSuccess)throw new Exception(started.Error.Message);
                var reservation=spool.Reserve(assignment);
                await new ResumableArtifactUploader(new Client(runtime),settings,null).ReserveAsync(reservation,registration,token);
                stats.MaxSdkConcurrency=Math.Max(stats.MaxSdkConcurrency,Interlocked.Increment(ref sdk));
                if(Volatile.Read(ref uploading)>0)Interlocked.Increment(ref stats.UploadOverlapStarts);
                Interlocked.Increment(ref stats.Started);
                PreparedResult result;
                var processingWatch=Stopwatch.StartNew();
                try
                {
                    await Task.Delay(number==1?140:20,token);
                    var request=BimOperationDispatcher.ReadPayload<ExportSpatialRequest>(assignment);
                    result=await new ArtifactFileWriter(settings.ResultsDirectory,settings).WriteAsync(assignment.TaskId,
                        "application/vnd.pilot.bim-spatial;version=2","spatial.bbsp",false,async(stream,t)=> {
                        using var index=new SpatialExportIndex(settings.ResultsDirectory,16*1024*1024);
                        using var writer=new BinarySpatialWriter(stream,new Geometry(),index,request,Model,
                            request.ModelVersionUtc.Value.ToUniversalTime().ToString("O"),p=>p==Part,10000,t);
                        foreach(var element in request.ElementIds)await writer.WriteSourceAsync(element);
                        writer.Complete();
                    },token);
                }
                finally{Interlocked.Decrement(ref sdk);}
                processingWatch.Stop();
                DurableArtifact artifact;
                using(result)artifact=spool.Prepare(result,assignment,512,reservation,processingWatch.Elapsed.TotalMilliseconds);
                await new ResumableArtifactUploader(new Client(runtime),settings,null).PrepareAsync(artifact,registration,token);
                spool.MarkPrepared(artifact);
                stats.MaxPreparedDepth=Math.Max(stats.MaxPreparedDepth,pipeline.PreparedQueueDepth);
            }
        }
        catch(OperationCanceledException) when(token.IsCancellationRequested){}
        catch(Exception error){errors.Enqueue(error.ToString());}
        finally{deliveryStop.Cancel();await delivery;}
    }
    private sealed class Client:BimBridgeArtifactClientBase
    {
        public Client(BimBridgeRuntime runtime):base(runtime){}
    }
    private sealed class Geometry:ISpatialSource
    {
        public Task<bool> ExistsAsync(ElementKey key,CancellationToken token)=>Task.FromResult(true);
        public Task VisitChildrenAsync(ElementKey key,Func<ElementKey,bool> visit,CancellationToken token)=>Task.CompletedTask;
        public Task VisitInstancesAsync(ElementKey key,Func<SpatialInstance,Task> visit,CancellationToken token)=>visit(new SpatialInstance {
            MeshPartId=Part,MeshId=Model,Placement=new double[]{1,0,0,10,0,1,0,20,0,0,1,30,0,0,0,1}});
        public async Task VisitMeshAsync(SpatialInstance instance,Func<string,double[],Task> floats,Func<string,uint[],Task> indices,CancellationToken token)
        {
            await floats("Vertices",new double[]{0,0,0,1,0,0,0,1,0});
            await floats("Normals",new double[]{0,0,1,0,0,1,0,0,1});await indices("Indices",new uint[]{0,1,2});
        }
    }
}
internal class BimBridgeArtifactClientBase:IBimBridgeArtifactClient
{
    private readonly BimBridgeRuntime _runtime;
    public BimBridgeArtifactClientBase(BimBridgeRuntime runtime){_runtime=runtime;}
    public Task<ArtifactUploadStatusDto> ReserveArtifactAsync(ReserveArtifactDto r)=>_runtime.ReserveArtifactAsync(r,default);
    public Task<ArtifactUploadStatusDto> BeginOrResumeAsync(BeginArtifactUploadDto r)=>_runtime.BeginOrResumeArtifactAsync(r,default);
    public Task<ArtifactUploadStatusDto> UploadStatusAsync(ArtifactTransferRequestDto r)=>_runtime.ArtifactStatusAsync(r,default);
    public Task<ArtifactChunkResponseDto> WriteArtifactChunkAsync(ArtifactChunkRequestDto r)=>_runtime.WriteArtifactChunkAsync(r,default);
    public Task<CompleteTaskResponseDto> CompleteArtifactAsync(ArtifactTransferRequestDto r)=>_runtime.CompleteArtifactAsync(r,default);
}
