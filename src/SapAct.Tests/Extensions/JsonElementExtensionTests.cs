namespace SapAct.Tests.Extensions;

[TestClass, TestCategory("Unit")]
public class JsonElementExtensionTests
{

    [TestMethod]
    public void TestLACOlumnList()
    {
        //arrange
        var json = new
        {
            ObjectKey = "blah",
            data = new
            {
                DataA = "blah",
                DataB = "blah"
            },
        };

        //act
        var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
        var result = jsonElement.GenerateColumnList(TargetStorageEnum.LogAnalytics);

        //assert
        result.Should().HaveCount(4);
        result.Should().Contain(x => x.Name == "TimeGenerated");
        result.Should().Contain(x => x.Name == "ObjectKey");
        result.Should().Contain(x => x.Name == "DataA");
        result.Should().Contain(x => x.Name == "DataB");
    }

    [TestMethod]
    public void TestADXCOlumnList()
    {
        //arrange
        var json = new
        {
            ObjectKey = "blah",
            data = new
            {
                DataA = "blah",
                DataB = "blah"
            },
        };

        //act
        var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
        var result = jsonElement.GenerateColumnList(TargetStorageEnum.ADX);

        //assert
        result.Should().HaveCount(3);
        result.Should().Contain(x => x.Name == "ObjectKey");
        result.Should().Contain(x => x.Name == "DataA");
        result.Should().Contain(x => x.Name == "DataB");
    }


    [TestMethod]
    public void TryGetDataPropertyPresentTests()
    {
        //arrange
        var json = new
        {
            ObjectKey = "blah",
            data = new
            {
                DataA = "blah",
                DataB = "blah"
            },
        };

        //act
        var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
        var result = jsonElement.TryGetDataProperty(out var data);

        //assert
        result.Should().BeTrue();
        data.Should().NotBeNull();
    }

    [TestMethod]
    public void TryGetDataPropertyNotPresentTests()
    {
        //arrange
        var json = new
        {
            ObjectKey = "blah"
        };

        //act
        var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
        var result = jsonElement.TryGetDataProperty(out JsonElement data);

        //assert
        result.Should().BeFalse();
        data.ValueKind.Should().Be(JsonValueKind.Undefined);
    }

}