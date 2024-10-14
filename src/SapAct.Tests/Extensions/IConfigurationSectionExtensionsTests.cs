namespace SapAct.Tests.Extensions;

[TestClass, TestCategory("Unit")]
public class IConfigurationSectionExtensionsTests
{
	[TestMethod]
	[DataRow("true", "false", true, false)]
	[DataRow("true", "true", true, true)]
	[DataRow("false", "true", false, true)]
	[DataRow("false", "false", false, false)]
	public void TestADXTopicDisable(string adxSinkConfigValue, string laSinkConfigValue, bool adxValue, bool laValue)
	{
		//arrange
		var inMemorySettings = new List<KeyValuePair<string, string>> {
			new($"{Consts.ServiceBusConfigurationSectionName}:0:{Consts.ServiceBusTopicNameConfigKey}", "TopicA"),
			new($"{Consts.ServiceBusConfigurationSectionName}:0:{Consts.ServiceBusConnectionStringConfigKey}", "ConnStringA"),
			new($"{Consts.ServiceBusConfigurationSectionName}:0:{Consts.ServiceBusTopicADXSinkDisabledConfigKey}", adxSinkConfigValue),
			new($"{Consts.ServiceBusConfigurationSectionName}:0:{Consts.ServiceBusTopicLASinkDisabledConfigKey}", laSinkConfigValue)
		};

		IConfiguration configuration = new ConfigurationBuilder()
			.AddInMemoryCollection(inMemorySettings!)
			.Build();

		//act
		var config = configuration.GetServiceBusTopicConfiguration().First();

		//assert
		config.ADXSinkDisabled.Should().Be(adxValue);
		config.LASinkDisabled.Should().Be(laValue);
	}
}
