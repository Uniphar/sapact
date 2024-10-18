namespace SapAct.Models;

public record SQLTableDescriptor
{
	public required string TableName { get; init; }
	public string SqlTableName { get; set; } = "";
	public required int Depth { get; set; }
	public List<SQLColumnDescriptor> Columns { get; init; } = [];
	public List<SQLTableDescriptor> ChildTables { get; init; } = [];
	public SQLTableDescriptor GetChildTableDescriptor(string childTableName)
	{
		return ChildTables.First(x => x.TableName == childTableName);
	}

	public bool IsEmpty => Columns.Count == 0 && ChildTables.Count == 0;
}

public record SQLColumnDescriptor
{
	public required string ColumnName { get; init; }
	public required string SQLDataType { get; init; }
}
