namespace SapAct.Tests;

[TestClass, TestCategory("Integration")]
public class IntegrationTests
{
	private static ServiceBusSender? _messageBusSender;
	private static BlobServiceClient? _blobServiceClient;
	private static BlobContainerClient? _blobContainerClient;
	private static ICslQueryProvider? _adxQueryProvider;
	private static ICslAdminProvider? _adxAdminProvider;
	private static LogsQueryClient? _logsQueryClient;
	private static IConfiguration? _config;
	private static SqlConnection? _sqlConnection;

	private static string _databaseName = "devops";

	private static CancellationToken _cancellationToken = default;

	private const string ObjectType = "SapActIntTests";

	private bool schemaCheckPassed = false;
	private bool adxIngestCheckPassed = false;
	private bool laIngestCheckPassed = false;
	private bool sqlIngestCheckPassed = false;

	[ClassInitialize]
	public static async Task ClassInitialize(TestContext context)
	{
		_cancellationToken = context.CancellationTokenSource.Token;

		var azureKeyVaultName = Environment.GetEnvironmentVariable(Consts.KEYVAULT_CONFIG_URL);

		DefaultAzureCredential credential = new();

		_config = new ConfigurationBuilder()
			.AddAzureKeyVault(new(azureKeyVaultName!), credential)
			.AddEnvironmentVariables()
			.Build();

		var intTestTopicConfig = _config.GetIntTestsServiceBusConfig();

		var sbClient = new ServiceBusClient(intTestTopicConfig.ConnectionString, credential);

		_messageBusSender = sbClient.CreateSender(intTestTopicConfig.TopicName);

		_blobServiceClient = new(new Uri(_config[Consts.LockServiceBlobConnectionStringConfigKey]), credential);
		_blobContainerClient = _blobServiceClient.GetBlobContainerClient(_config.GetLockServiceBlobContainerNameOrDefault());

		_databaseName = _config.GetADXClusterDBNameOrDefault();

		var kcsb = new KustoConnectionStringBuilder(_config[Consts.ADXClusterHostUrlConfigKey], _databaseName)
		   .WithAadTokenProviderAuthentication(async () => (await credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);

		_adxQueryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb);
		_adxAdminProvider = KustoClientFactory.CreateCslAdminProvider(kcsb);

		_logsQueryClient = new LogsQueryClient(credential);

		_sqlConnection = new SqlConnection(_config.GetSQLConnectionString());
		_sqlConnection.Open();
	}

	[TestMethod]
	public async Task E2EMessageIngestionTest()
	{
		//arrange
		await DropSQLTable("SapActIntTests");
		await DropSQLTable("SapActIntTests_SchemaTable");
		await DropSQLTable("SapActIntTests0");

		await DropADXTableAsync();

		string version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
		string extendedVersion = $"{version}Ext";

		var objectKey = Guid.NewGuid().ToString();
		var extendedObjectKey = Guid.NewGuid().ToString();

		await _messageBusSender!.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(ObjectType, objectKey, version))));
		await _messageBusSender!.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(ObjectType, extendedObjectKey, extendedVersion, extendedSchema: true))));

		//act

		bool timerFired = false;
		System.Timers.Timer timer = new(TimeSpan.FromMinutes(20))
		{
			AutoReset = false
		};

		timer.Elapsed += (s, e) => timerFired = true;
		timer.Start();


		//assert

		while (
			!await CheckSchemasProjected(extendedVersion, _cancellationToken)
			|| !await CheckADXDataIngest(objectKey, extendedObjectKey, _cancellationToken)
			|| !await CheckLADataIngest(objectKey, extendedObjectKey, _cancellationToken)
			|| !await CheckSQLDataIngest(objectKey, extendedObjectKey, _cancellationToken)
			)
		{
			if (timerFired)
				throw new TimeoutException("IntegrationTests.E2EMessageIngestionTest timeout");

			await Task.Delay(TimeSpan.FromSeconds(10));
		}
	}

	private async Task<bool> CheckSchemasProjected(string version, CancellationToken cancellationToken = default)
	{
		if (schemaCheckPassed)
			return true;

		try
		{
			var adxBlobProps = await _blobContainerClient!.GetBlobClient(LockService.GetBlobName(ObjectType, TargetStorageEnum.ADX)).GetPropertiesAsync(cancellationToken: cancellationToken);
			var laBlobProps = await _blobContainerClient!.GetBlobClient(LockService.GetBlobName(ObjectType, TargetStorageEnum.LogAnalytics)).GetPropertiesAsync(cancellationToken: cancellationToken);
			var sqlBlobProps = await _blobContainerClient!.GetBlobClient(LockService.GetBlobName(ObjectType, TargetStorageEnum.SQL)).GetPropertiesAsync(cancellationToken: cancellationToken);

			if (adxBlobProps.Value.Metadata.Count != 1 || laBlobProps.Value.Metadata.Count != 1 || sqlBlobProps.Value.Metadata.Count != 1)
			{
				return false;
			}

			var schemaCheckPassed =
				adxBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey] == version &&
				laBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey] == version &&
				sqlBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey] == version;

			return schemaCheckPassed;


		}
		catch (RequestFailedException ex) when (ex.Status == 404)
		{
			return false;
		}
	}

	private async Task<bool> CheckSQLDataIngest(string objectKey, string extendedObjectKey, CancellationToken cancellationToken = default)
	{
		if (sqlIngestCheckPassed)
			return true;

		using var sqlCommand = new SqlCommand($"SELECT * FROM SapActIntTests WHERE objectKey in ('{objectKey}', '{extendedObjectKey}')", _sqlConnection);
		using var reader = await sqlCommand.ExecuteReaderAsync(cancellationToken);

		int rowCount = 0;

		while (reader.Read())
		{
			rowCount++;
		}

		if (rowCount != 2)
			return false;

		sqlIngestCheckPassed = true;

		return true;
	}

	private async Task<bool> CheckADXDataIngest(string objectKey, string extendedObjectKey, CancellationToken cancellationToken = default)
	{
		if (adxIngestCheckPassed)
			return true;

		var result = await _adxQueryProvider!.ExecuteQueryAsync(_databaseName, $"{ObjectType} | where objectKey == '{objectKey}'", null, cancellationToken);
		var extendedResult = await _adxQueryProvider.ExecuteQueryAsync(_databaseName, $"{ObjectType} | where objectKey == '{extendedObjectKey}'", null, cancellationToken);

		if (!result.Read())
			return false;

		if (!extendedResult.Read())
			return false;

		extendedResult[extendedResult.GetOrdinal(PayloadHelper.ExtendedSchemaColumnName)].Should().Be("value");

		adxIngestCheckPassed = true;

		return true;
	}

	private async Task<bool> CheckLADataIngest(string objectKey, string extendedObjectKey, CancellationToken cancellationToken)
	{
		if (laIngestCheckPassed)
			return true;

		var tableName = LogAnalyticsService.GetTableName(ObjectType);

		Response<LogsQueryResult> result = await _logsQueryClient!.QueryWorkspaceAsync(_config!.GetLogAnalyticsWorkspaceId(), $"{tableName} | where objectKey in ('{objectKey}', '{extendedObjectKey}')", QueryTimeRange.All, cancellationToken: cancellationToken);

		int rowCount = result.Value.Table.Rows.Count;

		if (rowCount != 2)
			return false;
		else
		{
			if (!result.Value.Table.Columns.Any((c) => c.Name == PayloadHelper.ExtendedSchemaColumnName))
			{
				throw new InvalidOperationException("Log analytics schema does not contain expected extended schema column");
			}

			laIngestCheckPassed = true;

			return true;
		}
	}

	private async Task DropADXTableAsync()
	{
		try
		{
			await _adxAdminProvider!.ExecuteControlCommandAsync(_databaseName, $".drop table {ObjectType}", null);
		}
		catch (EntityNotFoundException)
		{
			//ignore
		}
	}

	private async Task DropSQLTable(string tableName)
	{
		try
		{
			using var sqlCommand = new SqlCommand($"DROP TABLE {tableName}", _sqlConnection);
			await sqlCommand.ExecuteNonQueryAsync(_cancellationToken);
		}
		catch (SqlException ex) when (ex.Number == 3701)
		{
			//ignore
		}
	}
}
