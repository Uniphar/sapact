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
	public void TestLACOlumnListWithDuplicate()
	{
		//arrange
		var json = new
		{
			ObjectKey = "blah",
            		Property1 = "prop1A",
			data = new
			{
				DataA = "blah",
				DataB = "blah",
				Property1 = "prop1B"
			},
		};

		//act
		var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
		var result = jsonElement.GenerateColumnList(TargetStorageEnum.LogAnalytics);

		//assert
		result.Should().HaveCount(5);
		result.Should().Contain(x => x.Name == "TimeGenerated");
		result.Should().Contain(x => x.Name == "ObjectKey");
		result.Should().Contain(x => x.Name == "DataA");
		result.Should().Contain(x => x.Name == "DataB");
		result.Should().Contain(x => x.Name == "Property1");
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

	[TestMethod]
	public void ExportToFlattenedDictionaryTests()
	{
		//arrange
		var json = new
		{
			ObjectKey = "blah",
			Property1 = "prop1A",
			data = new
			{
				DataA = "blah",
				DataB = "blah",
				Property1 = "prop1B"
			},
		};

		//act
		var jsonElement = JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(json));
		var result = jsonElement.ExportToFlattenedDictionary();
		//assert
		result.Should().HaveCount(4);
		result.Should().ContainKey("ObjectKey");
		result.Should().ContainKey("DataA");
		result.Should().ContainKey("DataB");
		result.Should().ContainKey("Property1");
        result["Property1"].Should().Be("prop1A"); //top level wins
	}

}
