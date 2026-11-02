using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Pilot.BimBridge.Contracts;
using Pilot.BimBridge.ServerExtension;

namespace Pilot.BimBridge.IntegrationTests
{
    public sealed class TokenHttpAdapterTests
    {
        private string _root;
        private BimBridgeRuntime _runtime;
        private TokenHttpAdapter _adapter;
        private HttpClient _http;
        private BimBridgeOptions _options;
        [SetUp] public void Setup()
        {
            _root=Path.Combine(Path.GetTempPath(),"bim-http-test",Guid.NewGuid().ToString("N"));
            var port=new TcpListener(IPAddress.Loopback,0);port.Start();var number=((IPEndPoint)port.LocalEndpoint).Port;port.Stop();
            _options=new BimBridgeOptions();_options.Results.Directory=_root;_options.Results.ChunkSizeBytes=4;
            _options.RestApi=new RestApiOptions {Enabled=true,Prefix="http://127.0.0.1:"+number+"/bim-bridge/",Token="test-only-token",MaxRequestBytes=2048};
            _runtime=new BimBridgeRuntime(_options,new SystemClock(),new FileSystemBimResultStore(_root),new SilentIntegrationLog(),false);
            _adapter=new TokenHttpAdapter(_options.RestApi,_runtime,new SilentIntegrationLog());_adapter.Start();
            _http=new HttpClient(new HttpClientHandler {UseProxy=false}) {BaseAddress=new Uri(_options.RestApi.Prefix),Timeout=TimeSpan.FromSeconds(5)};
            _http.DefaultRequestHeaders.Add("X-Api-Token","test-only-token");
        }
        [TearDown] public void Cleanup()
        {_http.Dispose();_adapter.Dispose();_runtime.Dispose();if(Directory.Exists(_root))Directory.Delete(_root,true);}
        [Test] public async Task RequiresTokenAndPreservesHealthAndWorkers()
        {
            _http.DefaultRequestHeaders.Remove("X-Api-Token");
            Assert.That((await _http.GetAsync("health")).StatusCode,Is.EqualTo(HttpStatusCode.Unauthorized));
            _http.DefaultRequestHeaders.Add("Authorization","Bearer test-only-token");
            var health=JObject.Parse(await _http.GetStringAsync("health"));
            Assert.That(health["Capabilities"].ToObject<string[]>(),Does.Contain("token-http-chunks-v2"));
            Assert.That(JArray.Parse(await _http.GetStringAsync("workers")).Count,Is.Zero);
        }
        [Test] public async Task NotebookPayloadReachesNativeTaskAndChunksRemainDeterministic()
        {
            var worker=_runtime.Register(new RegisterWorkerRequestDto {WorkerInstanceId="http-worker",ProtocolVersion="1.0",Capabilities=new[]{"echo"}});
            var reply=await _http.PostAsync("tasks",new StringContent("{\"operationCode\":\"echo\",\"clientRequestId\":\""+Guid.NewGuid()+"\",\"payload\":{\"value\":320},\"requiredCapabilities\":[\"echo\"]}",Encoding.UTF8,"application/json"));
            Assert.That(reply.StatusCode,Is.EqualTo(HttpStatusCode.OK));
            var task=Guid.Parse((string)JObject.Parse(await reply.Content.ReadAsStringAsync())["TaskId"]);
            var assigned=(await _runtime.WaitForTaskAsync(new WaitForTaskRequestDto {RegistrationId=worker.RegistrationId,WaitTimeoutSeconds=1},CancellationToken.None)).Assignment;
            Assert.That(Encoding.UTF8.GetString(assigned.Payload),Does.Contain("320"));
            _runtime.StartTask(new StartTaskRequestDto {RegistrationId=worker.RegistrationId,TaskId=task,AssignmentId=assigned.AssignmentId});
            var data=Encoding.UTF8.GetBytes("{\"ok\":320}");
            var begin=await _runtime.BeginUploadAsync(new BeginResultUploadRequestDto {RegistrationId=worker.RegistrationId,TaskId=task,AssignmentId=assigned.AssignmentId,TotalLength=data.Length,TotalChunks=(data.Length+3)/4,ContentType="application/json"},CancellationToken.None);
            for(var i=0;i<(data.Length+3)/4;i++)
            {
                var chunk=new byte[Math.Min(4,data.Length-i*4)];Array.Copy(data,i*4,chunk,0,chunk.Length);
                await _runtime.UploadChunkAsync(new UploadResultChunkRequestDto {UploadId=begin.UploadId,ChunkIndex=i,Data=chunk},CancellationToken.None);
            }
            var done=await _runtime.CompleteAsync(new CompleteTaskRequestDto {RegistrationId=worker.RegistrationId,TaskId=task,AssignmentId=assigned.AssignmentId,UploadId=begin.UploadId},CancellationToken.None);
            Assert.That(done.IsSuccess,Is.True);
            var metadata=JObject.Parse(await _http.GetStringAsync("tasks/"+task+"/result/metadata"));
            Assert.That((string)metadata["DownloadProtocol"],Is.EqualTo("token-http-chunks-v2"));
            Assert.That(await _http.GetByteArrayAsync("tasks/"+task+"/result"),Is.EqualTo(data));
            var first=await _http.GetStringAsync("tasks/"+task+"/result/chunks/0");
            Assert.That(await _http.GetStringAsync("tasks/"+task+"/result/chunks/0"),Is.EqualTo(first));
        }
        [Test] public async Task RejectsInvalidAndOversizedJsonAndSupportsCancellation()
        {
            Assert.That((await _http.PostAsync("tasks",new StringContent("{"))).StatusCode,Is.EqualTo(HttpStatusCode.BadRequest));
            Assert.That((await _http.PostAsync("tasks",new StringContent(new string('x',2049)))).StatusCode,Is.EqualTo(HttpStatusCode.RequestEntityTooLarge));
            var reply=await _http.PostAsync("tasks",new StringContent("{\"operationCode\":\"echo\",\"clientRequestId\":\""+Guid.NewGuid()+"\",\"payload\":{}}",Encoding.UTF8,"application/json"));
            var task=(string)JObject.Parse(await reply.Content.ReadAsStringAsync())["TaskId"];
            Assert.That((await _http.DeleteAsync("tasks/"+task)).StatusCode,Is.EqualTo(HttpStatusCode.OK));
            Assert.That((string)JObject.Parse(await _http.GetStringAsync("tasks/"+task))["State"],Is.EqualTo("CANCELLED"));
        }
        [Test] public void EnvironmentTokenFallsBackToConfiguredToken()
        {
            var options=new RestApiOptions {Token="configured",TokenEnvironmentVariable="BIM_BRIDGE_TEST_"+Guid.NewGuid().ToString("N")};
            Assert.That(options.ResolveToken(),Is.EqualTo("configured"));
        }
    }
}
