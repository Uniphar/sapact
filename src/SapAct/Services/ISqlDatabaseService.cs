namespace SapAct.Services;

public interface ISqlDatabaseService
{
    Task<(bool exists, IEnumerable<string> columns)> CheckTableExistsAsync(string tableName, SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken = default);
    Task<Dictionary<string, int>> PrefillSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction, CancellationToken cancellationToken = default);
    Task SinkJsonObjectAsync(SqlConnection sqlConnection, SqlTransaction sqlTransaction, string tableName, JsonElement payload, SQLTableDescriptor schemaDescriptor, KeyDescriptor keyDescriptor, CancellationToken cancellationToken = default);
    Task UpdateSchemaTableAsync(string rootCtx, SqlConnection sqlConnection, SqlTransaction sqlTransaction, Dictionary<string, int> tableNamingCtx, CancellationToken cancellationToken = default);
    Task<bool> UpsertSQLTableAsync(SQLTableDescriptor schemaDescriptor, SqlConnection connection, SqlTransaction transaction, SQLTableDescriptor? parent = null, int depth = 0, bool dryRun = false, CancellationToken cancellationToken = default);
}