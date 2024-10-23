param Database object

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
  properties:{
    elasticPoolId: genericElasticPool.id
  }
}

output connectionString string = 'Server=tcp:${genericSqlServer.properties.fullyQualifiedDomainName},1433;Initial Catalog=${database.name};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Authentication=Active Directory Default'
output databaseId string = database.id
