namespace SapAct.Services;

public interface ILockService
{
	Task<(string? leaseId, LockState lockState)> ObtainLockAsync(string tableName, string version, TargetStorageEnum targetStorageEnum);
	Task ReleaseLockAsync(string tableName, string version, TargetStorageEnum targetStorage, string leaseId);
	Task<LockState> WaitForLockDissolvedAsync(string tableName, string version, TargetStorageEnum targetStorage);
	Task<BlobProperties?> GetBlobPropertiesAsync(string tableName, TargetStorageEnum targetStorage);
}
