namespace SapAct.Models;

public record SQLTableDescriptor
{
	public required string TableName { get; init; }
	public string SqlTableName { get; set; }
	public List<SQLColumnDescriptor> Columns { get; init; } = [];
	public List<SQLTableDescriptor> ChildTables { get; init; } = [];
}

public record SQLColumnDescriptor
{
	public required string ColumnName { get; init; }
	public required string SQLDataType { get; init; }
}
