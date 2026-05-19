namespace SapAct.Services;

public interface ISchemaVersionStore
{
    Task<string?> GetSchemaVersionAsync(string objectType, TargetStorageEnum targetStorage);
    Task SetSchemaVersionAsync(string objectType, TargetStorageEnum targetStorage, string version);
}
