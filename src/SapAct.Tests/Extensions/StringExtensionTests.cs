namespace SapAct.Tests.Extensions;

[TestClass]
[TestCategory("Unit")]
public class StringExtensionTests
{
    [TestMethod]
    public void ToPascalCase_HyphenSeparated_ReturnsPascalCase()
    {
        "integration-suite-log".MakeTableFriendly().Should().Be("integration_suite_log");
    }

    [TestMethod]
    public void ToPascalCase_UnderscoreSeparated_ReturnsPascalCase()
    {
        "integration_suite_log".MakeTableFriendly().Should().Be("integration_suite_log");
    }

    [TestMethod]
    public void ToPascalCase_MixedSeparators_ReturnsPascalCase()
    {
        "integration-suite_log".MakeTableFriendly().Should().Be("integration_suite_log");
    }

    [TestMethod]
    public void ToPascalCase_NoSeparators_PreservesExistingCasing()
    {
        "IntegrationSuiteLog".MakeTableFriendly().Should().Be("IntegrationSuiteLog");
    }

    [TestMethod]
    public void ToPascalCase_SingleWord_CapitalizesFirstLetter()
    {
        "integration".MakeTableFriendly().Should().Be("integration");
    }

    [TestMethod]
    public void ToPascalCase_AlreadyPascalCase_ReturnsSame()
    {
        "SalesOrder".MakeTableFriendly().Should().Be("SalesOrder");
    }

    [TestMethod]
    public void ToPascalCase_WithDigits_PreservesDigits()
    {
        "sales-order-1".MakeTableFriendly().Should().Be("sales_order_1");
    }

    [TestMethod]
    public void ToPascalCase_ConsecutiveSeparators_HandlesGracefully()
    {
        "sales--order".MakeTableFriendly().Should().Be("sales_order");
    }

    [TestMethod]
    public void ToPascalCase_LeadingAndTrailingSeparators_HandlesGracefully()
    {
        "-sales-order-".MakeTableFriendly().Should().Be("sales_order");
    }

    [TestMethod]
    public void ToPascalCase_StripsSpecialCharacters()
    {
        "sales.order@header".MakeTableFriendly().Should().Be("sales_order_header");
    }

    [TestMethod]
    public void ToPascalCase_NullInput_ThrowsArgumentException()
    {
        Action act = () => ((string)null!).MakeTableFriendly();
        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void ToPascalCase_EmptyInput_ThrowsArgumentException()
    {
        Action act = () => "".MakeTableFriendly();
        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void ToPascalCase_WhitespaceInput_ThrowsArgumentException()
    {
        Action act = () => "   ".MakeTableFriendly();
        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void ToPascalCase_StartsWithDigitAfterSanitization_ThrowsArgumentException()
    {
        Action act = () => "123table".MakeTableFriendly();
        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void ToPascalCase_ExceedsMaxLength_ThrowsArgumentException()
    {
        Action act = () => new string('a', 46).MakeTableFriendly();
        act.Should().Throw<ArgumentException>();
    }

    [TestMethod]
    public void ToPascalCase_ExactlyMaxLength_DoesNotThrow()
    {
        var input = new string('a', 45);
        input.MakeTableFriendly().Should().HaveLength(45);
    }
}