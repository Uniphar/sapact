namespace SapAct.Models;

public record SQLTableDescriptor
{
	public required string TableName { get; init; }
	public string SqlTableName { get; set; } = "";
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
}

public record SQLColumnDescriptor
{
	public required string ColumnName { get; set; }
	public required string SQLDataType { get; init; }
	public bool IsSchemaColumn { get; set; }
}
