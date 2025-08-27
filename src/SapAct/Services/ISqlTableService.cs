namespace SapAct.Services;

public interface ISqlTableService
{
    SQLTableDescriptor GenerateSchemaDescriptorInner(string tableName, JsonElement item, int depth = 0);
}