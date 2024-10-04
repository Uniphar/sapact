param adxClusterName string
param adxDatabase object
param location string = resourceGroup().location

resource database 'Microsoft.Kusto/clusters/databases@2023-08-15' = {
  name: '${adxClusterName}/${adxDatabase.name}'
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
