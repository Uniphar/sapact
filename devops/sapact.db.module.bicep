param Database object
param workloadIdentityClientId string

resource failoverGroup 'Microsoft.Sql/servers/failoverGroups@2024-05-01-preview' existing = {
  name: Database.server.failoverGroupName
  parent: genericSqlServer
}
resource genericSqlServer 'Microsoft.Sql/servers@2022-11-01-preview' existing = {
  name: Database.server.name
}

resource genericElasticPool 'Microsoft.Sql/servers/elasticPools@2022-11-01-preview' existing = {
  name: Database.server.elasticPool.name
  parent: genericSqlServer
}

resource database 'Microsoft.Sql/servers/databases@2022-11-01-preview' = {
  parent: genericSqlServer
  location: Database.resourceGroup.location
  name: Database.name
  properties: {
    elasticPoolId: genericElasticPool.id
  }
}

var fullyQualifiedDomain = contains(failoverGroup.properties.databases, database.id)
  ? '${failoverGroup.name}${environment().suffixes.sqlServerHostname}'
  : genericSqlServer.properties.fullyQualifiedDomainName

output connectionString string = 'Server=tcp:${fullyQualifiedDomain},1433;Initial Catalog=${database.name};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Workload Identity;User Id=${workloadIdentityClientId};'
output databaseId string = database.id
