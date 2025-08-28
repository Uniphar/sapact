namespace SapAct.Models;

public record SQLTableDescriptor
{
	public required string TableName { get; init; }
	public string SqlTableName { get; set; } = string.Empty;
	public required int Depth { get; set; }
	public List<SQLColumnDescriptor> Columns { get; init; } = [];
	public List<SQLTableDescriptor> ChildTables { get; init; } = [];
	public SQLTableDescriptor? GetChildTableDescriptor(string childTableName)
	{
		return ChildTables.FirstOrDefault(x => x.TableName == childTableName);
	}

	public bool IsEmpty => Columns.Count == 0 && ChildTables.Count == 0;

	public SQLColumnDescriptor? GetIgnoreCaseColumnDescriptor(string columnName)
	{
		return Columns.FirstOrDefault(x => x.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
	}

    public static SQLTableDescriptor GenerateSchemaDescriptorInner(string tableName, JsonElement item, int depth = 0)
    {
        var schemaDescriptor = new SQLTableDescriptor() { TableName = tableName, Depth = depth };

        if (depth > 0)
        {
            //add PK and FK as explicit columns
            schemaDescriptor.Columns.Add(new SQLColumnDescriptor()
            { ColumnName = Consts.SQLPKColumnName, SQLDataType = Consts.SQLKeyColumnDefaultDataType, IsSchemaColumn = true });
            schemaDescriptor.Columns.Add(new SQLColumnDescriptor()
            { ColumnName = Consts.SQLFKColumnName, SQLDataType = Consts.SQLKeyColumnDefaultDataType, IsSchemaColumn = true });
        }

        if (item.ValueKind == JsonValueKind.Array)
        {
            foreach (var element in item.EnumerateArray())
            {
                if (element.ValueKind == JsonValueKind.Object)
                {
                    ProcessJsonObject(schemaDescriptor, tableName, element, item.ValueKind, depth);
                }
                else
                {
                    throw new InvalidOperationException("Only JSON objects are expected to be iterated over");
                }
            }
        }
        else if (item.ValueKind == JsonValueKind.Object)
        {
            //find all nested types
            foreach (var child in item.EnumerateObject())
            {
                if (child.Value.ValueKind == JsonValueKind.Object || child.Value.ValueKind == JsonValueKind.Array)
                {
                    ProcessJsonObject(schemaDescriptor, child.Name, child.Value, item.ValueKind, depth + 1);
                }
                else
                {
                    schemaDescriptor.Columns.Add(new SQLColumnDescriptor() { ColumnName = child.Name, SQLDataType = Consts.SQLDefaultDataType });
                }
            }
        }
        else
        {
            throw new InvalidOperationException("Unexpected JSON structure - only objects and arrays are expected to be processed");
        }

        return schemaDescriptor;
    }

    private static void ProcessJsonObject(SQLTableDescriptor currentLevel, string tableName, JsonElement element, JsonValueKind jsonValueKind, int depth)
    {
        var levelDesc = GenerateSchemaDescriptorInner(tableName, element, depth);
        //merge vs new
        var existingNode = jsonValueKind == JsonValueKind.Array
            ? currentLevel
            : currentLevel.ChildTables.FirstOrDefault(x => x.TableName == tableName);

        if (existingNode != null)
        {
            MergeTableDescriptors(existingNode, levelDesc);
        }
        else
        {
            //new child table
            currentLevel.ChildTables.Add(levelDesc);
        }
    }

    private static void MergeTableDescriptors(SQLTableDescriptor currentLevel, SQLTableDescriptor levelDesc)
    {
        foreach (var column in levelDesc.Columns)
        {
            if (currentLevel.Columns.All(x => x.ColumnName != column.ColumnName))
            {
                currentLevel.Columns.Add(column);
            }
        }

        foreach (var child in levelDesc.ChildTables)
        {
            var existingChild = currentLevel.ChildTables.FirstOrDefault(x => x.TableName == child.TableName);
            if (existingChild != null)
            {
                MergeTableDescriptors(existingChild, child);
            }
            else
            {
                currentLevel.ChildTables.Add(child);
            }
        }
    }
}

public record SQLColumnDescriptor
{
	public required string ColumnName { get; set; }
	public required string SQLDataType { get; set; }
	public bool IsSchemaColumn { get; init; }
}
