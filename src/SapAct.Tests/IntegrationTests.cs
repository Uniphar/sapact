namespace SapAct.Tests;

[TestClass, TestCategory("Integration")]
public class IntegrationTests
{
	private static ServiceBusTopicConfiguration? _messageBusConfiguration;
	private static ServiceBusAdministrationClient? _messageBusAdminClient;
	private static ServiceBusSender? _messageBusSender;
	private static BlobServiceClient? _blobServiceClient;
	private static BlobContainerClient? _blobContainerClient;
	private static ICslQueryProvider? _adxQueryProvider;
	private static ICslAdminProvider? _adxAdminProvider;
	private static LogsQueryClient? _logsQueryClient;
	private static IConfiguration? _config;
	private static SqlConnection? _sqlConnection;
	private static DefaultAzureCredential? _credential;

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

		_credential = new();

		_config = new ConfigurationBuilder()
			.AddAzureKeyVault(new(azureKeyVaultName!), _credential)
			.AddEnvironmentVariables()
			.Build();

		_messageBusConfiguration = _config.GetIntTestsServiceBusConfig();

		var sbClient = new ServiceBusClient(_messageBusConfiguration.ConnectionString, _credential);

		_messageBusAdminClient = new(_messageBusConfiguration.ConnectionString, _credential);

		_messageBusSender = sbClient.CreateSender(_messageBusConfiguration.TopicName);
		
		_blobServiceClient = new(new Uri(_config[Consts.LockServiceBlobConnectionStringConfigKey]), _credential);
		_blobContainerClient = _blobServiceClient.GetBlobContainerClient(_config.GetLockServiceBlobContainerNameOrDefault());

		_databaseName = _config.GetADXClusterDBNameOrDefault();

		var kcsb = new KustoConnectionStringBuilder(_config[Consts.ADXClusterHostUrlConfigKey], _databaseName)
		   .WithAadTokenProviderAuthentication(async () => (await _credential.GetTokenAsync(new([Consts.KustoTokenScope]))).Token);

		_adxQueryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb);
		_adxAdminProvider = KustoClientFactory.CreateCslAdminProvider(kcsb);

		_logsQueryClient = new LogsQueryClient(_credential);

		_sqlConnection = new SqlConnection(GetIntTestSqlConnectionString());
		_sqlConnection.Open();
	}

	public static string GetIntTestSqlConnectionString() => $"Server=tcp:{Environment.GetEnvironmentVariable("SQL_SERVER_NAME")}.database.windows.net,1433;Initial Catalog=sapact-dev-db;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Default;";

	[TestMethod]
	public async Task E2EMessageIngestionTest()
	{
		//arrange
		await DropSQLTable("SapActIntTests");
		await DropSQLTable("SapActIntTests_SchemaTable");

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

	[TestMethod]
	public async Task DeltaChangeIngestionTest()
	{
		//arrange
		await DropSQLTable("SapActIntTests");
		await DropSQLTable("SapActIntTests_SchemaTable");

		await DropADXTableAsync();

		string version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

		var objectKey = Guid.NewGuid().ToString();
		var deltaEventKey = Guid.NewGuid().ToString();

		await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<SQLWorker>());
		await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<ADXWorker>());
		await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<LogAnalyticsWorker>());

		await _messageBusSender!.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(ObjectType, objectKey, version))));
		await _messageBusSender!.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(ObjectType, deltaEventKey, version, deltaChangePayload: true))));

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
			!await CheckADXDataIngest(objectKey, cancellationToken: _cancellationToken)
			|| !await CheckLADataIngest(objectKey, cancellationToken: _cancellationToken)
			|| !await CheckSQLDataIngest(objectKey, cancellationToken: _cancellationToken)
			)
		{
			if (timerFired)
				throw new TimeoutException("IntegrationTests.E2EMessageIngestionTest timeout");

			await Task.Delay(TimeSpan.FromSeconds(10));
		}
		
		var postCheck = await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<SQLWorker>())
						&& await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<ADXWorker>())
						&& await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<LogAnalyticsWorker>())
						&& !await CheckLARecordPresentAsync(deltaEventKey)
						&& !await CheckSQLRecordPresentAsync(deltaEventKey)
						&& !await CheckADXRecordPresentAsync(deltaEventKey);

	}

	private async Task<bool> CheckNoDLQMessagePresentForSubscriptionAsync(string subscriptionName)
	{
		var dlqCount = await GetSubscriptionDLQCountAsync(subscriptionName);

		return dlqCount == 0;
	}


	private async Task PurgeDLQForServiceBusSubscriptionAsync(string subscriptionName)
	{
		await using var client = new ServiceBusClient(_messageBusConfiguration.ConnectionString, _credential);
		var receiver = client.CreateReceiver(_messageBusConfiguration.TopicName, subscriptionName, new ServiceBusReceiverOptions
		{
			SubQueue = SubQueue.DeadLetter
		});

		while (true)
		{
			var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
			if (message == null)
			{
				break;
			}

			await receiver.CompleteMessageAsync(message);
			Console.WriteLine($"Message with ID {message.MessageId} has been purged.");
		}

		await receiver.CloseAsync();
	}

	private async Task<long> GetSubscriptionDLQCountAsync(string subscriptionName)
	{
		var subscriptionRuntimeProperties = await _messageBusAdminClient!.GetSubscriptionRuntimePropertiesAsync(_messageBusConfiguration!.TopicName, subscriptionName);
		
		return subscriptionRuntimeProperties.Value.DeadLetterMessageCount;
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

			schemaCheckPassed =
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

	private async Task<bool> CheckSQLDataIngest(string objectKey, string? extendedObjectKey=null, CancellationToken cancellationToken = default)
	{
		if (sqlIngestCheckPassed)
			return true;
		
		bool extendedSchemaColumnPresent = !string.IsNullOrWhiteSpace(extendedObjectKey);

		if (!await CheckSQLRecordPresentAsync(objectKey, cancellationToken))
			return false;

		if (extendedSchemaColumnPresent && !await CheckSQLRecordPresentAsync(extendedObjectKey!, cancellationToken))
			return false;

		sqlIngestCheckPassed = true;

		return true;
	}

	private async Task<bool> CheckSQLRecordPresentAsync(string objectKey, CancellationToken cancellationToken = default)
	{
		using var sqlCommand = new SqlCommand($"SELECT * FROM SapActIntTests WHERE objectKey in ('{objectKey}')", _sqlConnection);
		using var reader = await sqlCommand.ExecuteReaderAsync(cancellationToken);

		int rowCount = 0;

		while (reader.Read())
		{
			rowCount++;
		}

		return rowCount == 1;
	}

	private async Task<bool> CheckADXRecordPresentAsync(string objectKey, CancellationToken cancellationToken= default)
	{
		var result = await GetADXRecordAsync(objectKey, cancellationToken);

		if (!result.Read())
			return false;

		return true;
	}

	private async Task<IDataReader> GetADXRecordAsync(string objectKey, CancellationToken cancellationToken = default)
	{
		return await _adxQueryProvider!.ExecuteQueryAsync(_databaseName, $"{ObjectType} | where objectKey == '{objectKey}'", null, cancellationToken);
	}

	private async Task<bool> CheckADXDataIngest(string objectKey, string? extendedObjectKey=null, CancellationToken cancellationToken = default)
	{
		if (adxIngestCheckPassed)
			return true;

		try
		{
			if (!await CheckADXRecordPresentAsync(objectKey, cancellationToken))
				return false;

			if (!string.IsNullOrEmpty(extendedObjectKey))
			{
				var extendedResult = await GetADXRecordAsync(extendedObjectKey);

				if (!extendedResult.Read())
					return false;

				extendedResult[extendedResult.GetOrdinal(PayloadHelper.ExtendedSchemaColumnName)].Should().Be("value");
			}
		}
		catch (SemanticException)
		{
			return false; //table does not exist yet
		}
		adxIngestCheckPassed = true;

		return true;
	}

	private async Task<bool> CheckLADataIngest(string objectKey, string? extendedObjectKey=null, CancellationToken cancellationToken= default)
	{
		if (laIngestCheckPassed)
			return true;

		bool extendedSchemaColumnPresent = !string.IsNullOrWhiteSpace(extendedObjectKey);

		if (!await CheckLARecordPresentAsync(objectKey, cancellationToken: cancellationToken))
			return false;

		if (extendedSchemaColumnPresent && !await CheckLARecordPresentAsync(extendedObjectKey!, checkExtendedColumn: true, cancellationToken: cancellationToken))
			return false;

		laIngestCheckPassed = true;

		return true;
	}

	private async Task<bool> CheckLARecordPresentAsync(string objectKey, bool checkExtendedColumn = false, CancellationToken cancellationToken= default)
	{
		var tableName = LogAnalyticsService.GetTableName(ObjectType);

		Response<LogsQueryResult> result = await _logsQueryClient!.QueryWorkspaceAsync(_config!.GetLogAnalyticsWorkspaceId(), $"{tableName} | where objectKey in ('{objectKey}')", QueryTimeRange.All, cancellationToken: cancellationToken);

		return result.Value.Table.Rows.Count==1 && (!checkExtendedColumn || result.Value.Table.Columns.Any((c) => c.Name == PayloadHelper.ExtendedSchemaColumnName));
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
