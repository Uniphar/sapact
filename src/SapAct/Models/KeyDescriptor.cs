namespace SapAct.Models;

public record KeyDescriptor
{
    public required string RootKey { get; init; }
    public required string ForeignKey { get; init; }
    public int? ArrayIndex { get; init; }
}