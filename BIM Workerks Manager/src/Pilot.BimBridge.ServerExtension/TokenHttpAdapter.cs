using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    // In-process compatibility binding for the already deployed /bim-bridge URL.
    // All task/storage logic remains in BimBridgeRuntime; no second dispatcher/store.
    internal sealed class TokenHttpAdapter : IDisposable
    {
        private readonly RestApiOptions _options;
        private readonly IBimBridgeLog _log;
        private readonly BimBridgeApiSession _api;
        private readonly HttpListener _listener = new HttpListener();
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private readonly SemaphoreSlim _slots = new SemaphoreSlim(16);
        private readonly byte[] _tokenHash;
        private readonly string _path;
        private Task _loop;
        public TokenHttpAdapter(RestApiOptions options, BimBridgeRuntime runtime, IBimBridgeLog log)
        {
            options.Validate(); _options=options; _log=log;
            _api=new BimBridgeApiSession(runtime,null,log);
            _path=new Uri(options.Prefix).AbsolutePath;
            using(var sha=SHA256.Create()) _tokenHash=sha.ComputeHash(Encoding.UTF8.GetBytes(options.ResolveToken()));
            _listener.Prefixes.Add(options.Prefix);
        }
        public void Start() { _listener.Start(); _loop=Task.Run(AcceptAsync); }
        private async Task AcceptAsync()
        {
            while(!_stop.IsCancellationRequested)
            {
                try
                {
                    await _slots.WaitAsync(_stop.Token).ConfigureAwait(false);
                    HttpListenerContext context;
                    try { context=await _listener.GetContextAsync().ConfigureAwait(false); }
                    catch { _slots.Release(); throw; }
                    _ = ServeAsync(context);
                }
                catch(Exception error) when(error is OperationCanceledException || error is HttpListenerException || error is ObjectDisposedException)
                {
                    if(!_stop.IsCancellationRequested) _log.Error("BIM Bridge HTTP accept failed.",error);
                    break;
                }
            }
        }
        private bool Authenticated(HttpListenerRequest request)
        {
            var token=request.Headers["X-Api-Token"];
            if(string.IsNullOrEmpty(token))
            {
                var auth=request.Headers["Authorization"];
                if(auth!=null && auth.StartsWith("Bearer ",StringComparison.OrdinalIgnoreCase)) token=auth.Substring(7);
            }
            if(string.IsNullOrEmpty(token) || token.Length>16384)return false;
            byte[] hash;using(var sha=SHA256.Create())hash=sha.ComputeHash(Encoding.UTF8.GetBytes(token));
            var diff=0;for(var i=0;i<hash.Length;i++)diff|=hash[i]^_tokenHash[i];return diff==0;
        }
        private static JObject Error(string code,string message) => JObject.FromObject(new {Error=new {Code=code,Message=message,IsRetryable=false}});
        private async Task JsonAsync(HttpListenerResponse response,object value,int status=200)
        {
            var body=value as JToken ?? JToken.FromObject(value);
            if(status==200 && body is JObject obj && obj["Error"] is JObject error)
                status=((string)error["Code"]=="TASK_NOT_FOUND" || (string)error["Code"]=="RESULT_EXPIRED")?404:409;
            var data=Encoding.UTF8.GetBytes(body.ToString(Formatting.None));
            response.StatusCode=status;response.ContentType="application/json; charset=utf-8";
            response.ContentLength64=data.Length;
            await response.OutputStream.WriteAsync(data,0,data.Length,_stop.Token).ConfigureAwait(false);
        }
        private async Task<JObject> RequestAsync(HttpListenerRequest request)
        {
            if(request.ContentLength64>_options.MaxRequestBytes)throw new RequestException("REQUEST_TOO_LARGE",413);
            using(var data=new MemoryStream())
            {
                var buffer=new byte[8192];int count;
                while((count=await request.InputStream.ReadAsync(buffer,0,buffer.Length,_stop.Token).ConfigureAwait(false))>0)
                {
                    if(data.Length+count>_options.MaxRequestBytes)throw new RequestException("REQUEST_TOO_LARGE",413);
                    data.Write(buffer,0,count);
                }
                using(var reader=new JsonTextReader(new StringReader(new UTF8Encoding(false,true).GetString(data.ToArray()))) {MaxDepth=64})
                {var result=JObject.Load(reader);if(reader.Read())throw new JsonReaderException();return result;}
            }
        }
        private async Task ServeAsync(HttpListenerContext context)
        {
            var response=context.Response;var started=false;
            try
            {
                response.Headers["Cache-Control"]="no-store";
                var origin=context.Request.Headers["Origin"];
                if(!string.IsNullOrEmpty(origin) && !string.IsNullOrEmpty(_options.AllowedOrigin))
                {
                    if(_options.AllowedOrigin!="*" && origin!=_options.AllowedOrigin)
                    {await JsonAsync(response,Error("ORIGIN_NOT_ALLOWED","Origin is not allowed."),403);return;}
                    response.Headers["Access-Control-Allow-Origin"]=_options.AllowedOrigin;
                    response.Headers["Vary"]="Origin";
                }
                if(context.Request.HttpMethod=="OPTIONS")
                {
                    response.Headers["Access-Control-Allow-Methods"]="GET, POST, DELETE, OPTIONS";
                    response.Headers["Access-Control-Allow-Headers"]="Content-Type, X-Api-Token, Authorization";
                    response.StatusCode=204;return;
                }
                if(!Authenticated(context.Request)) {await JsonAsync(response,Error("UNAUTHORIZED","Valid BIM Bridge token is required."),401);return;}
                var path=context.Request.Url.AbsolutePath;
                if(!path.StartsWith(_path,StringComparison.Ordinal)) {await JsonAsync(response,Error("NOT_FOUND","Route not found."),404);return;}
                var parts=path.Substring(_path.Length).Trim('/').Split('/');var method=context.Request.HttpMethod;
                if(method=="GET" && parts.SequenceEqual(new[]{"health"}))
                {
                    var health=JObject.FromObject(await _api.GetHealthAsync().ConfigureAwait(false));
                    var capabilities=health["Capabilities"] as JArray ?? new JArray();
                    capabilities.Add("token-http-chunks-v2");health["Capabilities"]=capabilities;
                    await JsonAsync(response,health);return;
                }
                if(method=="GET" && parts.SequenceEqual(new[]{"statistics"})) {await JsonAsync(response,await _api.GetStatisticsAsync());return;}
                if(method=="GET" && parts.SequenceEqual(new[]{"workers"})) {await JsonAsync(response,await _api.GetWorkersAsync());return;}
                if(method=="POST" && parts.SequenceEqual(new[]{"tasks"}))
                {
                    var body=await RequestAsync(context.Request).ConfigureAwait(false);
                    var payload=body.GetValue("payload",StringComparison.OrdinalIgnoreCase) ?? new JObject();
                    foreach(var prop in body.Properties().Where(p=>p.Name.Equals("payload",StringComparison.OrdinalIgnoreCase)).ToArray())prop.Remove();
                    var request=body.ToObject<SubmitBimTaskRequestDto>();
                    request.ProtocolVersion=request.ProtocolVersion??BimBridgeProtocol.Version;
                    request.Payload=Encoding.UTF8.GetBytes(payload.ToString(Formatting.None));
                    request.PayloadContentType="application/json";request.PayloadEncoding="utf-8";
                    await JsonAsync(response,await _api.SubmitTaskAsync(request));return;
                }
                if(parts.Length>=2 && parts[0]=="tasks" && Guid.TryParse(parts[1],out var task))
                {
                    if(parts.Length==2 && method=="GET") {await JsonAsync(response,await _api.GetTaskStatusAsync(task));return;}
                    if(parts.Length==2 && method=="DELETE") {await JsonAsync(response,await _api.CancelTaskAsync(task));return;}
                    if(parts.Length==4 && parts[2]=="result" && parts[3]=="metadata" && method=="GET")
                    {
                        var metadata=JObject.FromObject(await _api.GetResultMetadataAsync(task));
                        if(metadata["Error"]==null || metadata["Error"].Type==JTokenType.Null)metadata["DownloadProtocol"]="token-http-chunks-v2";
                        await JsonAsync(response,metadata);return;
                    }
                    if(parts.Length==5 && parts[2]=="result" && parts[3]=="chunks" && method=="GET" && int.TryParse(parts[4],out var index) && index>=0)
                    {await JsonAsync(response,await _api.ReadResultChunkAsync(task,index));return;}
                    if(parts.Length==3 && parts[2]=="result" && method=="GET")
                    {
                        var metadata=await _api.GetResultMetadataAsync(task);
                        if(metadata.Error!=null) {await JsonAsync(response,metadata);return;}
                        response.ContentType=metadata.ContentType??"application/octet-stream";
                        // ContentEncoding describes the artifact, not HTTP transfer compression.
                        response.ContentLength64=metadata.TotalLength;started=true;
                        for(var chunkNumber=0;chunkNumber<metadata.TotalChunks;chunkNumber++)
                        {
                            var chunk=await _api.ReadResultChunkAsync(task,chunkNumber);
                            if(chunk.Error!=null || chunk.Data==null) {response.Abort();return;}
                            await response.OutputStream.WriteAsync(chunk.Data,0,chunk.Data.Length,_stop.Token).ConfigureAwait(false);
                        }
                        return;
                    }
                }
                await JsonAsync(response,Error("NOT_FOUND","Route not found."),404);
            }
            catch(RequestException error) {if(!started)await TryErrorAsync(response,error.Code,error.Status);}
            catch(Exception error) when(error is JsonException || error is DecoderFallbackException || error is FormatException)
            {if(!started)await TryErrorAsync(response,"INVALID_REQUEST",400);}
            catch(Exception)
            {
                // No exception text: request bodies/headers may contain credentials.
                if(!started)await TryErrorAsync(response,"HTTP_ADAPTER_ERROR",500);
            }
            finally {try{response.Close();}catch{} _slots.Release();}
        }
        private async Task TryErrorAsync(HttpListenerResponse response,string code,int status)
        {try {await JsonAsync(response,Error(code,"BIM Bridge request failed."),status);}catch{response.Abort();}}
        public void Dispose()
        {
            if(_stop.IsCancellationRequested)return;
            _stop.Cancel();_listener.Close();_api.Dispose();
        }
        private sealed class RequestException:Exception
        {public string Code{get;}public int Status{get;}public RequestException(string code,int status){Code=code;Status=status;}}
    }
}
