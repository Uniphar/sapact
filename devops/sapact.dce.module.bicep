param dceName string
param location string

resource monitorDCE 'Microsoft.Insights/dataCollectionEndpoints@2023-03-11' = {
  location: location
  name: dceName
  properties:{
    description: 'Data Collection Endpoint for SAP ACT'
    networkAcls: {
      publicNetworkAccess: 'Enabled'
    }
  }
}

output monitorDCEIngestionEndpoint string = monitorDCE.properties.logsIngestion.endpoint
