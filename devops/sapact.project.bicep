param adxClusterName string
param adxDatabase object
param appKeyVaultName string
param dawnSB object
param devopsSBNamespace string
param dceName string
param logAnalytics object
param storageAccountName string
param environment string
param sqlDatabase object
param workloadIdentityClientId string

param location string = resourceGroup().location

resource actionGroupInfrastructureLow 'microsoft.insights/actionGroups@2024-10-01-preview' existing = {
  name: 'platform-engineering-infrastructure-low'
  scope: resourceGroup('observability')
}

resource actionGroupApplicationsLow 'microsoft.insights/actionGroups@2024-10-01-preview' existing = {
  name: 'platform-engineering-applications-low'
  scope: resourceGroup('observability')
}

resource adxCluster 'Microsoft.Kusto/clusters@2023-08-15' existing = {
  name: adxClusterName
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2021-04-01' existing = {
  name: storageAccountName
}

resource adxDatabaseResource 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  parent: adxCluster
  name: adxDatabase.name
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: adxDatabase.softDeletePeriod
    hotCachePeriod: adxDatabase.hotCachePeriod
  }

  resource permissions 'principalAssignments@2023-08-15' = [
    for permission in adxDatabase.permissions: {
      name: guid(adxClusterName, adxDatabase.name, permission.principalId)
      properties: {
        principalId: permission.principalId
        principalType: permission.principalType
        role: permission.role
      }
    }
  ]
}

module dce 'sapact.dce.module.bicep' = {
  name: 'dce'
  scope: resourceGroup(logAnalytics.ResourceGroupName)
  params: {
    dceName: dceName
    location: location
  }
}

resource DevopsAppKeyVault 'Microsoft.KeyVault/vaults@2022-07-01' existing = {
  name: appKeyVaultName
}

resource SB0ConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--0--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: '${dawnSB.Alias}.servicebus.windows.net'
  }
}

resource SB0TopicNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--0--Name'
  parent: DevopsAppKeyVault
  properties: {
    value: 'sap-events'
  }
}

resource SB1ConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--1--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: '${devopsSBNamespace}.servicebus.windows.net'
  }
}

resource SB1TopicNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--1--Name'
  parent: DevopsAppKeyVault
  properties: {
    value: 'sapactinttests'
  }
}
resource SB2ConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--2--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: '${dawnSB.Alias}.servicebus.windows.net'
  }
}

resource SB2TopicNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--2--Name'
  parent: DevopsAppKeyVault
  properties: {
    value: 'integration-suite-error'
  }
}
resource SB2TopicSinkDisable 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--2--SQLSinkDisabled'
  parent: DevopsAppKeyVault
  properties: {
    value: 'true'
  }
}
resource SB3ConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--3--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: '${dawnSB.Alias}.servicebus.windows.net'
  }
}

resource SB3TopicNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--3--Name'
  parent: DevopsAppKeyVault
  properties: {
    value: 'integration-suite-log'
  }
}
resource SB3TopicSinkDisable 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--Topic--3--SQLSinkDisabled'
  parent: DevopsAppKeyVault
  properties: {
    value: 'true'
  }
}

resource StorageAccountConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LockService--BlobConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: storageAccount.properties.primaryEndpoints.blob
  }
}

resource ADXClusterUrlSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--Adx--HostUrl'
  parent: DevopsAppKeyVault
  properties: {
    value: adxCluster.properties.uri
  }
}

resource ADXDatabaseSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--Adx--Database'
  parent: DevopsAppKeyVault
  properties: {
    value: adxDatabaseResource.name
  }
}

resource DCEEndpointNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--EndpointName'
  parent: DevopsAppKeyVault
  properties: {
    value: dceName
  }
}

resource DCEEndpointUrlSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--EndpointIngestionUrl'
  parent: DevopsAppKeyVault
  properties: {
    value: dce.outputs.monitorDCEIngestionEndpoint
  }
}

resource DCEEndpointRGNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--ResourceGroup'
  parent: DevopsAppKeyVault
  properties: {
    value: logAnalytics.ResourceGroupName
  }
}

resource DCEEndpointSubIdSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--SubscriptionId'
  parent: DevopsAppKeyVault
  properties: {
    value: subscription().subscriptionId
  }
}

resource laNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--WorkspaceName'
  parent: DevopsAppKeyVault
  properties: {
    value: logAnalytics.Name
  }
}

resource laIdSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LogAnalytics--WorkspaceId'
  parent: DevopsAppKeyVault
  properties: {
    value: logAnalytics.CustomerId
  }
}

module alerts 'sapact.alerts.module.bicep' = {
  name: 'alerts'
  params: {
    logAnalytics: logAnalytics
    lowActionGroupIds: [
      actionGroupInfrastructureLow.id
      actionGroupApplicationsLow.id
    ]
    environment: environment
    sbNamespaceId: dawnSB.Id
    sbTopicNames: ['sap-events']
  }
}

module alertsSecondary 'sapact.alerts.module.bicep' = if (environment == 'prod') {
  name: 'alertsSecondary'
  params: {
    logAnalytics: logAnalytics
    lowActionGroupIds: [
      actionGroupInfrastructureLow.id
      actionGroupApplicationsLow.id
    ]
    environment: environment
    sbNamespaceId: dawnSB.SecondaryId
    sbTopicNames: ['sap-events']
  }
}

module sqlDataBaseResource 'sapact.db.module.bicep' = {
  name: 'db'
  scope: resourceGroup(sqlDatabase.resourceGroup.name)
  params: {
    Database: sqlDatabase
    workloadIdentityClientId: workloadIdentityClientId
  }
}

resource sqlConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--SQL--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: sqlDataBaseResource.outputs.connectionString
  }
}
