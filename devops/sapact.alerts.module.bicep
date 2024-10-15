param logAnalytics object
param actionGroupDevOpsLowId string
param environment string
param sbTopicNames array
param sbNamespaceId string

param location string = resourceGroup().location

module ExceptionAlert 'alerts.scheduledqueryrules.bicep' = {
  name: 'SapAct-ExceptionDetectedAlert'
  params: {
    location: location
    alertName: 'SapAct-ExceptionDetectedAlert'
    environment: environment
    logAnalyticsWorkspaceId: logAnalytics.AzureId
    query: '''AppExceptions 
              | where AppRoleInstance startswith "sapact" 
                and ExceptionType != "System.Threading.Tasks.TaskCanceledException"
           '''
    actionGroupId: actionGroupDevOpsLowId
  }
}

resource DLQAlert 'microsoft.insights/metricAlerts@2018-03-01' =  [for sbTopicName in sbTopicNames :{
  name: 'SapAct DLQ message - ${sbTopicName}'
  location: 'global'
  properties: {
    enabled:  environment == 'prod'
    description: 'Alert triggered when the DLQ messages count is greater than 1'
    severity: 3
    scopes: [
      sbNamespaceId
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          name: 'dlqCountOverOne'
          metricNamespace: 'microsoft.servicebus/namespaces'
          criterionType: 'StaticThresholdCriterion'
          timeAggregation: 'Maximum'
          metricName: 'DeadletteredMessages'
          operator: 'GreaterThanOrEqual'
          threshold: 1
          dimensions: [
            {
                name: 'EntityName'
                operator: 'Include'
                values: [sbTopicName]
            }
          ]
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
    }
    actions: [
      {
        actionGroupId: actionGroupDevOpsLowId
        webHookProperties: {}
      }
    ]
  }
}]
