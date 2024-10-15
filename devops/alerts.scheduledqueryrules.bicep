param location string = resourceGroup().location
param alertName string
param environment string
param logAnalyticsWorkspaceId string
param query string
param actionGroupId string

resource alert 'Microsoft.Insights/scheduledQueryRules@2021-08-01' = {
  name: '${alertName}-${environment}'
  location: location
  properties: {
    severity: 1
    enabled: environment == 'prod' ? true : false
    scopes: [
      logAnalyticsWorkspaceId
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          query: query
          operator: 'GreaterThan'
          timeAggregation: 'Count'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    autoMitigate: false
    actions: {
      actionGroups: [
        actionGroupId
      ]
      customProperties: {
      }
    }
  }
}
