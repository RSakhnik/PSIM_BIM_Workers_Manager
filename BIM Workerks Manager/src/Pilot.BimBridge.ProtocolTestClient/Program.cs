using System.Security.Cryptography;
using System.Text;
using Ascon.Pilot.Common.DataProtection;
using Ascon.Pilot.DataClasses;
using Ascon.Pilot.Server.Api;
using Pilot.BimBridge.Contracts;

if (args.Length < 2)
{
    Console.WriteLine("Usage: Pilot.BimBridge.ProtocolTestClient <server/database> <login>");
    return 2;
}

Console.Write("Password: ");
var password = ReadPassword();
Console.WriteLine();

const int PilotIceEnterpriseCode = 103;
var credentials = ConnectionCredentials.GetConnectionCredentials(args[0], args[1], password.ConvertToSecureString());
using var client = new HttpPilotClient(credentials.GetConnectionString());
client.Connect(false);
var authentication = client.GetAuthenticationAsyncApi();
await authentication.LoginWithIdentityAsync(
    credentials.DatabaseName,
    credentials.Username,
    credentials.ProtectedPassword,
    PilotIceEnterpriseCode,
    new SelfIdentity { Id = "Pilot.BimBridge.ProtocolTestClient" });

var callback = new NoOpCallback();
var diagnostics = client.GetCustomApi<IBimBridgeDiagnosticsApi, IBimBridgeClientCallback>(callback);
var workers = client.GetCustomApi<IBimBridgeWorkerApi, IBimBridgeClientCallback>(callback);
var tasks = client.GetCustomApi<IBimBridgeTaskApi, IBimBridgeClientCallback>(callback);

var health = await diagnostics.GetHealthAsync();
Print("Health", health.State, health.Error);

var registration = await workers.RegisterWorkerAsync(new RegisterWorkerRequestDto
{
    ProtocolVersion = BimBridgeProtocol.Version,
    WorkerInstanceId = "protocol-test-" + Environment.MachineName,
    MachineName = Environment.MachineName,
    ExtensionVersion = "1.0.0",
    BimSdkVersion = "test",
    Capabilities = new[] { "PROTOCOL_TEST" },
    AvailableModelKeys = new[] { "TEST_MODEL" },
    MaxParallelTasks = 1
});
Print("Register", registration.RegistrationId.ToString(), registration.Error);
if (!registration.IsSuccess) return 1;

var waitTask = workers.WaitForTaskAsync(new WaitForTaskRequestDto
{
    RegistrationId = registration.RegistrationId,
    WaitTimeoutSeconds = registration.LongPollingTimeoutSeconds
});

var accepted = await tasks.SubmitTaskAsync(new SubmitBimTaskRequestDto
{
    ClientRequestId = Guid.NewGuid(),
    ProtocolVersion = BimBridgeProtocol.Version,
    OperationCode = "PROTOCOL_TEST",
    RequiredCapabilities = new[] { "PROTOCOL_TEST" },
    TargetModelKey = "TEST_MODEL",
    Priority = 0,
    PayloadContentType = "application/json",
    PayloadEncoding = "utf-8",
    Payload = Encoding.UTF8.GetBytes("{\"ping\":true}"),
    CanRetry = false,
    MaxAttempts = 1
});
Print("Submit", accepted.TaskId.ToString(), accepted.Error);

var assignment = await waitTask;
Print("Wait", assignment.HasTask ? assignment.Assignment.TaskId.ToString() : "no task", assignment.Error);
if (!assignment.HasTask) return 1;

var started = await workers.StartTaskAsync(new StartTaskRequestDto
{
    RegistrationId = registration.RegistrationId,
    TaskId = assignment.Assignment.TaskId,
    AssignmentId = assignment.Assignment.AssignmentId
});
Print("Start", started.State, started.Error);

var result = Encoding.UTF8.GetBytes("{\"pong\":true}");
var sha = Convert.ToHexString(SHA256.HashData(result)).ToLowerInvariant();
var begin = await workers.BeginResultUploadAsync(new BeginResultUploadRequestDto
{
    RegistrationId = registration.RegistrationId,
    TaskId = assignment.Assignment.TaskId,
    AssignmentId = assignment.Assignment.AssignmentId,
    ContentType = "application/json",
    ContentEncoding = "utf-8",
    TotalLength = result.Length,
    TotalChunks = 1,
    Sha256 = sha
});
Print("Begin upload", begin.UploadId.ToString(), begin.Error);

var uploaded = await workers.UploadResultChunkAsync(new UploadResultChunkRequestDto
{
    UploadId = begin.UploadId,
    ChunkIndex = 0,
    Data = result
});
Print("Upload", uploaded.ReceivedChunks.ToString(), uploaded.Error);

var completed = await workers.CompleteTaskAsync(new CompleteTaskRequestDto
{
    RegistrationId = registration.RegistrationId,
    TaskId = assignment.Assignment.TaskId,
    AssignmentId = assignment.Assignment.AssignmentId,
    UploadId = begin.UploadId
});
Print("Complete", completed.State, completed.Error);

var metadata = await tasks.GetResultMetadataAsync(assignment.Assignment.TaskId);
var chunk = await tasks.ReadResultChunkAsync(assignment.Assignment.TaskId, 0);
Print("Read result", chunk.Data == null ? "empty" : Encoding.UTF8.GetString(chunk.Data), chunk.Error);

await workers.UnregisterWorkerAsync(new UnregisterWorkerRequestDto { RegistrationId = registration.RegistrationId });
client.Disconnect();
return completed.IsSuccess && metadata.IsSuccess && chunk.IsSuccess ? 0 : 1;

static string ReadPassword()
{
    var builder = new StringBuilder();
    ConsoleKeyInfo key;
    while ((key = Console.ReadKey(true)).Key != ConsoleKey.Enter)
    {
        if (key.Key == ConsoleKey.Backspace && builder.Length > 0)
            builder.Length--;
        else if (!char.IsControl(key.KeyChar))
            builder.Append(key.KeyChar);
    }
    return builder.ToString();
}

static void Print(string operation, string value, BimBridgeErrorDto error)
{
    Console.WriteLine(error == null
        ? $"{operation}: OK ({value})"
        : $"{operation}: {error.Code} - {error.Message}");
}

sealed class NoOpCallback : IBimBridgeClientCallback
{
    public void OnNotification(BimBridgeNotificationDto notification) { }
}
