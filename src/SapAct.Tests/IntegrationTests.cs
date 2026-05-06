using Azure.Core;

namespace SapAct.Tests;

[TestClass]
[TestCategory("Integration")]
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
    private static TokenCredential? _credentials;

    private static string _databaseName = "devops";

    private static CancellationToken _cancellationToken;
    private string _objectType = null!;
    private SqlConnection? _sqlConnection;
    private bool adxIngestCheckPassed;
    private bool laIngestCheckPassed;
    private bool schemaCheckPassed;
    private bool sqlIngestCheckPassed;
    private static string? _env;

    // Per-test instance state — MSTest creates a new class instance per test method.
    public TestContext? TestContext { get; set; }

    [ClassInitialize]
    public static async Task ClassInitialize(TestContext context)
    {
        _env = context.Properties["Environment"]!.ToString();
        _cancellationToken = context.CancellationToken;

        _credentials = new AzureCliCredential();


        _config = new ConfigurationBuilder()
            .AddAzureKeyVault(new($"https://uni-devops-app-{_env}-kv.vault.azure.net/"), _credentials)
            .AddEnvironmentVariables()
            .Build();

        _messageBusConfiguration = _config.GetIntTestsServiceBusConfig();

        var sbClient = new ServiceBusClient(_messageBusConfiguration.ConnectionString, _credentials);

        _messageBusAdminClient = new(_messageBusConfiguration.ConnectionString, _credentials);

        _messageBusSender = sbClient.CreateSender(_messageBusConfiguration.TopicName);
        var blobStorageKey = _config["STORAGEACCOUNT:KEY"];

        _blobServiceClient = new(
            $"DefaultEndpointsProtocol=https;AccountName=unidevops{_env};AccountKey={blobStorageKey};EndpointSuffix=core.windows.net");


        _blobContainerClient = _blobServiceClient.GetBlobContainerClient(_config.GetLockServiceBlobContainerNameOrDefault());

        _databaseName = _config.GetADXClusterDBNameOrDefault();

        var kustoConnectionStringBuilder = new KustoConnectionStringBuilder(_config[Consts.ADXClusterHostUrlConfigKey], _databaseName)
            .WithAadTokenProviderAuthentication(async () => (await _credentials.GetTokenAsync(new([Consts.KustoTokenScope]), _cancellationToken)).Token);

        _adxQueryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder);
        _adxAdminProvider = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder);

        _logsQueryClient = new(_credentials);
    }

    [TestInitialize]
    public async Task TestInitialize()
    {
        _objectType = TestContext!.TestName;
        var connectionStringBuilder = new SqlConnectionStringBuilder(_config?.GetSQLConnectionString())
        {
            Authentication = SqlAuthenticationMethod.ActiveDirectoryDefault
        };

        _sqlConnection = new(connectionStringBuilder.ConnectionString);
        await _sqlConnection.OpenAsync(_cancellationToken);
    }

    [TestCleanup]
    public async Task TestCleanup()
    {
        if (_sqlConnection is not null) await _sqlConnection.DisposeAsync();
    }


    [TestMethod]
    public async Task E2EMessageIngestionTest()
    {
        //arrange
        await DropSQLTable(_objectType);
        await DropSQLTable($"{_objectType}_SchemaTable");

        await DropADXTableAsync();

        var version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        var extendedVersion = $"{version}Ext";

        var objectKey = Guid.NewGuid().ToString();
        var extendedObjectKey = Guid.NewGuid().ToString();

        await _messageBusSender!.SendMessageAsync(
            new(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(_objectType, objectKey, version))),
            _cancellationToken);
        // check if the schema are projected for the non-extended version
        await Condition.WaitUntilAsync(() => CheckSchemasProjected(version, _cancellationToken), TimeSpan.FromMinutes(5));
        // insert extended version
        await _messageBusSender.SendMessageAsync(
            new(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(_objectType, extendedObjectKey, extendedVersion, true))),
            _cancellationToken);

        await Condition.WaitUntilAsync(() => CheckSchemasProjected(extendedVersion, _cancellationToken), TimeSpan.FromMinutes(5));
        await Condition.WaitUntilAsync(() => CheckADXDataIngest(objectKey, extendedObjectKey, _cancellationToken), TimeSpan.FromMinutes(5));
        await Condition.WaitUntilAsync(() => CheckLogAnalyticsIngest(objectKey, extendedObjectKey, _cancellationToken), TimeSpan.FromMinutes(5));
        await Condition.WaitUntilAsync(() => CheckSQLDataIngest(objectKey, extendedObjectKey, _cancellationToken), TimeSpan.FromMinutes(5));
    }

    [TestMethod]
    public async Task DeltaChangeIngestionTest()
    {
        //arrange
        await DropSQLTable(_objectType);
        await DropSQLTable($"{_objectType}_SchemaTable");

        await DropADXTableAsync();

        var version = DateTime.UtcNow.ToString("yyyyMMddHHmmss");

        var objectKey = Guid.NewGuid().ToString();
        var deltaEventKey = Guid.NewGuid().ToString();

        await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<SQLWorker>());
        await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<ADXWorker>());
        await PurgeDLQForServiceBusSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<LogAnalyticsWorker>());

        await _messageBusSender!.SendMessageAsync(
            new(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(_objectType, objectKey, version))),
            _cancellationToken);
        await _messageBusSender.SendMessageAsync(
            new(Encoding.UTF8.GetBytes(PayloadHelper.GetPayload(_objectType, deltaEventKey, version, deltaChangePayload: true))),
            _cancellationToken);

        //act + assert

        await Condition.WaitUntilAsync(() => CheckADXDataIngest(objectKey, cancellationToken: _cancellationToken), TimeSpan.FromMinutes(10));
        await Condition.WaitUntilAsync(() => CheckLogAnalyticsIngest(objectKey, cancellationToken: _cancellationToken), TimeSpan.FromMinutes(10));
        await Condition.WaitUntilAsync(() => CheckSQLDataIngest(objectKey, cancellationToken: _cancellationToken), TimeSpan.FromMinutes(10));
        Assert.IsTrue(
            await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<SQLWorker>()),
            "No DLQ messages expected for SQLWorker");
        Assert.IsTrue(
            await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<ADXWorker>()),
            "No DLQ messages expected for ADXWorker");
        Assert.IsTrue(
            await CheckNoDLQMessagePresentForSubscriptionAsync(_config!.GetTopicSubscriptionNameOrDefault<LogAnalyticsWorker>()),
            "No DLQ messages expected for LogAnalyticsWorker");
        Assert.IsFalse(
            await CheckLogAnalyticsRecordPresentAsync(deltaEventKey, cancellationToken: _cancellationToken),
            "Delta event should not be ingested into Log Analytics");
        Assert.IsFalse(
            await CheckSQLRecordPresentAsync(deltaEventKey, _cancellationToken),
            "Delta event should not be ingested into SQL");
        Assert.IsFalse(
            await CheckADXRecordPresentAsync(deltaEventKey, _cancellationToken),
            "Delta event should not be ingested into ADX");
    }

    private async Task<bool> CheckNoDLQMessagePresentForSubscriptionAsync(string subscriptionName)
    {
        var dlqCount = await GetSubscriptionDLQCountAsync(subscriptionName);

        return dlqCount == 0;
    }


    private async Task PurgeDLQForServiceBusSubscriptionAsync(string subscriptionName)
    {
        await using var client = new ServiceBusClient(_messageBusConfiguration!.ConnectionString, _credentials);
        var receiver = client.CreateReceiver(_messageBusConfiguration.TopicName,
            subscriptionName,
            new()
            {
                SubQueue = SubQueue.DeadLetter
            });

        while (true)
        {
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1), _cancellationToken);
            if (message == null) break;

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
        if (schemaCheckPassed) return true;

        try
        {
            var adxBlobProps = await _blobContainerClient!.GetBlobClient(BlobSchemaVersionStore.GetBlobName(_objectType, TargetStorageEnum.ADX)).GetPropertiesAsync(cancellationToken: cancellationToken);
            var laBlobProps = await _blobContainerClient.GetBlobClient(BlobSchemaVersionStore.GetBlobName(_objectType, TargetStorageEnum.LogAnalytics)).GetPropertiesAsync(cancellationToken: cancellationToken);
            var sqlBlobProps = await _blobContainerClient.GetBlobClient(BlobSchemaVersionStore.GetBlobName(_objectType, TargetStorageEnum.SQL)).GetPropertiesAsync(cancellationToken: cancellationToken);

            var adxVersion = adxBlobProps.Value.Metadata.TryGetValue(Consts.SyncedSchemaVersionLockBlobMetadataKey, out var adxV) ? adxV : "<missing>";
            var laVersion = laBlobProps.Value.Metadata.TryGetValue(Consts.SyncedSchemaVersionLockBlobMetadataKey, out var laV) ? laV : "<missing>";
            var sqlVersion = sqlBlobProps.Value.Metadata.TryGetValue(Consts.SyncedSchemaVersionLockBlobMetadataKey, out var sqlV) ? sqlV : "<missing>";

            if (adxBlobProps.Value.Metadata.Count != 1 || laBlobProps.Value.Metadata.Count != 1 || sqlBlobProps.Value.Metadata.Count != 1) return false;
            var check = adxBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey] == version;
            if (!check) return schemaCheckPassed;
            Assert.AreEqual(version, laBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey], "Log Analytics schema version does not match the expected version.");
            var check3 = sqlBlobProps.Value.Metadata[Consts.SyncedSchemaVersionLockBlobMetadataKey] == version;
            if (check3)
                schemaCheckPassed = true;
            else // ADX is updated, so SQL should be updated as well, if not something went wrong
                Assert.Fail("SQL schema version does not match the expected version.");



            return schemaCheckPassed;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
    }

    private async Task<bool> CheckSQLDataIngest(string objectKey, string? extendedObjectKey = null, CancellationToken cancellationToken = default)
    {
        if (sqlIngestCheckPassed) return true;

        var extendedSchemaColumnPresent = !string.IsNullOrWhiteSpace(extendedObjectKey);

        if (!await CheckSQLRecordPresentAsync(objectKey, cancellationToken)) return false;

        if (extendedSchemaColumnPresent && !await CheckSQLRecordPresentAsync(extendedObjectKey!, cancellationToken)) return false;

        sqlIngestCheckPassed = true;

        return true;
    }

    private async Task<bool> CheckSQLRecordPresentAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        await using var sqlCommand = new SqlCommand($"SELECT * FROM [{_objectType}] WHERE objectKey = @objectKey", _sqlConnection);
        sqlCommand.Parameters.AddWithValue("@objectKey", objectKey);
        await using var reader = await sqlCommand.ExecuteReaderAsync(cancellationToken);

        var rowCount = 0;

        while (await reader.ReadAsync(cancellationToken)) rowCount++;

        return rowCount == 1;
    }

    private async Task<bool> CheckADXRecordPresentAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        var result = await GetADXRecordAsync(objectKey, cancellationToken);

        return result.Read();
    }

    private async Task<IDataReader> GetADXRecordAsync(string objectKey, CancellationToken cancellationToken = default) => await _adxQueryProvider!.ExecuteQueryAsync(_databaseName, $"{_objectType} | where objectKey == '{objectKey}'", null, cancellationToken);

    private async Task<bool> CheckADXDataIngest(string objectKey, string? extendedObjectKey = null, CancellationToken cancellationToken = default)
    {
        if (adxIngestCheckPassed) return true;

        try
        {
            if (!await CheckADXRecordPresentAsync(objectKey, cancellationToken)) return false;

            if (!string.IsNullOrEmpty(extendedObjectKey))
            {
                var extendedResult = await GetADXRecordAsync(extendedObjectKey, cancellationToken);

                if (!extendedResult.Read()) return false;

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

    private async Task<bool> CheckLogAnalyticsIngest(string objectKey, string? extendedObjectKey = null, CancellationToken cancellationToken = default)
    {
        if (laIngestCheckPassed) return true;


        if (!await CheckLogAnalyticsRecordPresentAsync(objectKey, cancellationToken: cancellationToken)) return false;
        if (!string.IsNullOrWhiteSpace(extendedObjectKey))
            if (!await CheckLogAnalyticsRecordPresentAsync(extendedObjectKey, true, cancellationToken))
                return false;


        laIngestCheckPassed = true;

        return true;
    }

    private async Task<bool> CheckLogAnalyticsRecordPresentAsync(string objectKey, bool checkExtendedColumn = false, CancellationToken cancellationToken = default)
    {
        var tableName = LogAnalyticsService.GetTableName(_objectType);

        Response<LogsQueryResult> result = await _logsQueryClient!.QueryWorkspaceAsync(
            _config!.GetLogAnalyticsWorkspaceId(),
            $"{tableName} | where objectKey == '{objectKey}'",
            LogsQueryTimeRange.All,
            cancellationToken: cancellationToken);
        var response = result.Value.Table.Rows.Count == 1;
        if (result.Value.Table.Rows.Count > 1) Assert.Fail($"Multiple records found in Log Analytics for objectKey {objectKey}");
        if (checkExtendedColumn)
        {
            // second check, still only one row should be available 
            response = response && result.Value.Table.Columns.Any(c => c.Name == PayloadHelper.ExtendedSchemaColumnName);
        }

        return response;
    }

    private async Task DropADXTableAsync()
    {
        try
        {
            await _adxAdminProvider!.ExecuteControlCommandAsync(_databaseName, $".drop table {_objectType}");
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
            await using var sqlCommand = new SqlCommand($"DROP TABLE {tableName}", _sqlConnection);
            await sqlCommand.ExecuteNonQueryAsync(_cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 3701)
        {
            //ignore
        }
    }
}