using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Pilot.BimBridge.Contracts;

namespace Pilot.BimBridge.ServerExtension
{
    internal sealed class ArtifactReservation
    {
        public Guid TransferId,TaskId,AssignmentId,ClientRequestId;
        public string WorkerInstanceId,OperationCode;
        public DateTime CreatedAtUtc,TaskCreatedAtUtc;
    }
    internal sealed partial class FileSystemBimResultStore
    {
        internal ArtifactReservation ReadReservation(Guid id)
        {
            var path=Path.Combine(_root,".reservations",id.ToString("N")+".json");
            return File.Exists(path)?JsonConvert.DeserializeObject<ArtifactReservation>(File.ReadAllText(path)):null;
        }
        internal void SaveReservation(ArtifactReservation reservation)
        {
            var directory=Path.Combine(_root,".reservations");Directory.CreateDirectory(directory);
            var path=Path.Combine(directory,reservation.TransferId.ToString("N")+".json");
            var temporary=path+".tmp";
            File.WriteAllText(temporary,JsonConvert.SerializeObject(reservation));
            using(var stream=new FileStream(temporary,FileMode.Open,FileAccess.ReadWrite,FileShare.None))stream.Flush(true);
            File.Move(temporary,path);
        }
        internal void CleanupReservations(DateTime now,TimeSpan retention)
        {
            var directory=Path.Combine(_root,".reservations");
            if(!Directory.Exists(directory))return;
            foreach(var path in Directory.EnumerateFiles(directory,"*.json"))
            {
                var reservation=JsonConvert.DeserializeObject<ArtifactReservation>(File.ReadAllText(path));
                if(now-reservation.CreatedAtUtc>retention)File.Delete(path);
            }
        }
    }
    internal sealed partial class BimBridgeRuntime
    {
        public async Task<ArtifactUploadStatusDto> ReserveArtifactAsync(ReserveArtifactDto request,CancellationToken token)
        {
            if(request?.Transfer==null || request.TaskId==Guid.Empty || request.AssignmentId==Guid.Empty)
                return ResponseErrors.Error<ArtifactUploadStatusDto>("INVALID_REQUEST","Artifact reservation identity is required.");
            await _artifactBeginGate.WaitAsync(token).ConfigureAwait(false);
            try
            {
                lock(_gate)
                {
                    var error=ValidateTransferOwnerLocked(request.Transfer);
                    if(error!=null)return new ArtifactUploadStatusDto {Error=error};
                    var previous=ArtifactStore.ReadReservation(request.Transfer.TransferId);
                    if(previous!=null)
                    {
                        if(previous.TaskId!=request.TaskId || previous.AssignmentId!=request.AssignmentId || previous.WorkerInstanceId!=request.Transfer.WorkerInstanceId)
                            return ResponseErrors.Error<ArtifactUploadStatusDto>("TRANSFER_CONFLICT","Reservation metadata conflicts.");
                        return new ArtifactUploadStatusDto {TransferId=previous.TransferId,ExpiresAtUtc=previous.CreatedAtUtc.AddMinutes(_options.Results.ReservationTtlMinutes)};
                    }
                    if(!TryValidateAssignmentLocked(request.Transfer.RegistrationId,request.TaskId,request.AssignmentId,out _,out var task,out error))
                        return new ArtifactUploadStatusDto {Error=error};
                    ArtifactStore.SaveReservation(new ArtifactReservation {TransferId=request.Transfer.TransferId,TaskId=task.TaskId,
                        AssignmentId=request.AssignmentId,ClientRequestId=task.ClientRequestId,WorkerInstanceId=request.Transfer.WorkerInstanceId,
                        OperationCode=task.OperationCode,CreatedAtUtc=_clock.UtcNow,TaskCreatedAtUtc=task.CreatedAtUtc});
                    return new ArtifactUploadStatusDto {TransferId=request.Transfer.TransferId,
                        ExpiresAtUtc=_clock.UtcNow.AddMinutes(_options.Results.ReservationTtlMinutes)};
                }
            }
            finally {_artifactBeginGate.Release();}
        }
    }
}
