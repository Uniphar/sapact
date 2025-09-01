namespace SapAct.Services;

public interface ISqlDatabaseService
{
    Task<Dictionary<string, int>> PrefillSchemaTableAsync(
        string rootCtx,
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        CancellationToken cancellationToken = default);

    Task SinkJsonObjectAsync(
        string tableName, 
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        JsonElement payload, 
        SQLTableDescriptor schemaDescriptor, 
        KeyDescriptor keyDescriptor, 
        CancellationToken cancellationToken = default);

    Task UpdateSchemaTableAsync(
        string rootCtx, 
        SqlConnection sqlConnection,
        SqlTransaction sqlTransaction,
        Dictionary<string, int> tableNamingCtx,
        CancellationToken cancellationToken = default);

    Task<bool> UpsertSQLTableAsync(
        SQLTableDescriptor schemaDescriptor, 
        SqlConnection connection,
        SqlTransaction transaction, 
        SQLTableDescriptor? parent = null,
        int depth = 0,
        bool dryRun = false, 
        CancellationToken cancellationToken = default);
}