using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class StoredArtifactTaskState
    {
        public Guid TaskId,ClientRequestId;
        public string State,OperationCode;
        public DateTime CreatedAtUtc;
        public DateTime? CompletedAtUtc;
        public BimTaskErrorDto Error;
        public TaskRecord ToTask()=>new TaskRecord {TaskId=TaskId,ClientRequestId=ClientRequestId,State=State,
            OperationCode=OperationCode,CreatedAtUtc=CreatedAtUtc,CompletedAtUtc=CompletedAtUtc,Error=Error};
    }
    internal sealed partial class FileSystemBimResultStore
    {
        private string TaskStateRoot=>Path.Combine(_root,".task-states");
        internal void SaveTaskState(TaskRecord task)
        {
            Directory.CreateDirectory(TaskStateRoot);
            var state=new StoredArtifactTaskState {TaskId=task.TaskId,ClientRequestId=task.ClientRequestId,State=task.State,
                OperationCode=task.OperationCode,CreatedAtUtc=task.CreatedAtUtc,CompletedAtUtc=task.CompletedAtUtc,Error=task.Error};
            var path=Path.Combine(TaskStateRoot,task.TaskId.ToString("N")+".json");var temporary=path+".tmp";
            File.WriteAllText(temporary,JsonConvert.SerializeObject(state));
            using(var stream=new FileStream(temporary,FileMode.Open,FileAccess.ReadWrite,FileShare.None))stream.Flush(true);
            if(File.Exists(path))File.Replace(temporary,path,null);else File.Move(temporary,path);
        }
        internal StoredArtifactTaskState ReadTaskState(Guid taskId)
        {
            var path=Path.Combine(TaskStateRoot,taskId.ToString("N")+".json");
            return File.Exists(path)?JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path)):null;
        }
        internal IEnumerable<StoredArtifactTaskState> RecoverTaskStates()
        {
            if(!Directory.Exists(TaskStateRoot))yield break;
            foreach(var path in Directory.EnumerateFiles(TaskStateRoot,"*.json"))
                yield return JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path));
        }
        internal void CleanupTaskStates(DateTime now,TimeSpan retention)
        {
            if(!Directory.Exists(TaskStateRoot))return;
            foreach(var path in Directory.EnumerateFiles(TaskStateRoot,"*.json"))
            {
                var state=JsonConvert.DeserializeObject<StoredArtifactTaskState>(File.ReadAllText(path));
                if(state.CompletedAtUtc.HasValue&&now-state.CompletedAtUtc.Value>retention)File.Delete(path);
            }
        }
    }
}
