param adxClusterName string
param adxDatabase object
param appKeyVaultName string
param sbNamespace string
param dceName string
param logAnalytics object

param location string = resourceGroup().location

resource adxCluster 'Microsoft.Kusto/clusters@2023-08-15' existing = {
  name: adxClusterName

}

resource database 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  parent: adxCluster
  name: adxDatabase.name
  location: location
  kind: 'ReadWrite'
  properties: {
    softDeletePeriod: adxDatabase.softDeletePeriod
    hotCachePeriod: adxDatabase.hotCachePeriod
  }

  resource permissions 'principalAssignments@2023-08-15' = [for permission in adxDatabase.permissions : {
    name: guid(adxClusterName, adxDatabase.name, permission.principalId)
    properties: {
      principalId: permission.principalId
      principalType: permission.principalType
      role: permission.role
    }
  }]
}

module dce 'sapact.dce.module.bicep' = {
  name: 'dce'
  scope: resourceGroup(logAnalytics.ResourceGroupName)
  params: {
    dceName: dceName
    location: location
  }
}

resource DevopsAppKeyVault 'Microsoft.KeyVault/vaults@2022-07-01'  existing = {
  name: appKeyVaultName
}

resource SBConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--ConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: '${sbNamespace}.servicebus.windows.net'
  }
}

resource SBTopicNameSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--ServiceBus--TopicName'
  parent: DevopsAppKeyVault
  properties: {
    value: 'sap-events'
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
    value: database.name
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
    value: logAnalytics.Id
  }
}
