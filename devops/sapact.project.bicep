param appKeyVaultName string
param dceName string
param dawnSB object
param logAnalytics object
param storageAccountName string
param environment string

param location string = resourceGroup().location

resource actionGroupInfrastructureLow 'microsoft.insights/actionGroups@2024-10-01-preview' existing = {
  name: 'platform-engineering-infrastructure-low'
  scope: resourceGroup('observability')
}

resource actionGroupApplicationsLow 'microsoft.insights/actionGroups@2024-10-01-preview' existing = {
  name: 'platform-engineering-applications-low'
  scope: resourceGroup('observability')
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2021-04-01' existing = {
  name: storageAccountName
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

resource StorageAccountConnectionStringSecret 'Microsoft.KeyVault/vaults/secrets@2024-04-01-preview' = {
  name: 'SapAct--LockService--BlobConnectionString'
  parent: DevopsAppKeyVault
  properties: {
    value: storageAccount.properties.primaryEndpoints.blob
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
