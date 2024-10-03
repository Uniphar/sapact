namespace SapAct.Tests;

[TestClass, TestCategory("Developer")]
public class DeveloperTests
{
	private static ServiceBusSender? _messageBusSender;
	private static IConfiguration? _config;

	private static CancellationToken _cancellationToken = default;

	private const string ObjectType = "SapActIntTests";

	[ClassInitialize]
	public static async Task ClassInitialize(TestContext context)
	{
		_cancellationToken = context.CancellationTokenSource.Token;

		var azureKeyVaultName = Environment.GetEnvironmentVariable(Consts.KEYVAULT_CONFIG_URL);

		DefaultAzureCredential credential = new();

		_config = new ConfigurationBuilder()
			.AddAzureKeyVault(new(azureKeyVaultName), credential)
			.Build();

		var sbConnectionString = _config[Consts.ServiceBusConnectionStringConfigKey];

		var sbClient = new ServiceBusClient(sbConnectionString, credential);

		_messageBusSender = sbClient.CreateSender(_config[Consts.ServiceBusTopicNameConfigKey]);
	}

	[TestMethod]
	public async Task PushMessages()
	{
		string version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

		var objectKey = Guid.NewGuid().ToString();

		for (int x = 0; x < 10; x++)
		{
			await _messageBusSender!.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(ObjectType, $"{objectKey}{x}", $"{version}{x}"))), _cancellationToken);
		}
	}
}
