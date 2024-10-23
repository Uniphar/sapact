function Initialize-SapAct {
<#
.SYNOPSIS
Deploys the SapAct resources

.DESCRIPTION
Deploys the SapAct resources

.PARAMETER Environment
The environment to deploy the SapAct to

.EXAMPLE
Initialize-SapAct -Environment dev
Initializes SapAct in the dev environment.

#>
    [CmdletBinding(SupportsShouldProcess)]
    param (
        [parameter(Mandatory = $true, Position = 0)]
        [ValidateSet('dev', 'test', 'prod')]
        [string] $Environment
    )    

    $sapactTemplateFile = Join-Path $PSScriptRoot -ChildPath ".\sapact.project.bicep"

    $p_sapactProjectName = "sapact"

    $adxClusterName = Resolve-UniResourceName 'adx-cluster' $p_devopsDomain -Environment $Environment

    $devopsDomainRgName = Resolve-UniResourceName 'resource-group' $p_devopsDomain -Environment $Environment
    $dawnDomainRgName = Resolve-UniResourceName 'resource-group' $p_dawnDomain -Environment $Environment
    $devopsClusterIdentityName = Resolve-UniComputeDomainSAName $Environment $p_devopsDomain
    $cluserIdentity = Get-AzADServicePrincipal -DisplayName $devopsClusterIdentityName
    $clusterIdentityObjectId = $cluserIdentity| Select-Object -ExpandProperty Id
    $clusterIdentityAppId = $cluserIdentity| Select-Object -ExpandProperty AppId

    $githubActionsDevIdentityObjectId = Get-AzADServicePrincipal -DisplayName $adapp_GithubActionsDev | Select-Object -ExpandProperty Id
    $devopsAppKeyVault = Resolve-UniResourceName 'keyvault' "$p_devopsDomain-app" -Dev:$Dev -Environment $Environment
    $dawnServiceBusName = Resolve-UniResourceName 'service-bus' $p_dawnDomain -Environment $Environment
    $devopsServiceBusName = Resolve-UniResourceName 'service-bus' $p_devopsDomain -Environment $Environment
    $dceEndpointName = Resolve-UniResourceName 'monitor-dce' $p_sapactProjectName -Environment $Environment
    $devopsStorageAccountName = Resolve-UniResourceName 'storage' $p_devopsDomain -Dev:$Dev -Environment $Environment
    $logAnalyticsWorkspace = Resolve-UniMainLogAnalytics $Environment
    $actionGroupDevOpsLowId = Resolve-UniResourceId 'devops-low'
    
    $dawnSBResource  = Get-AzResource -Name $dawnServiceBusName -ResourceGroupName $dawnDomainRgName
    
    $logAnalyticsDef = @{
        Name = $logAnalyticsWorkspace.Name
        CustomerId = $logAnalyticsWorkspace.CustomerId
        AzureId = $logAnalyticsWorkspace.ResourceId
        ResourceGroupName = $logAnalyticsWorkspace.ResourceGroupName
    }

    $dawnSB = @{
        Id = $dawnSBResource.Id
        Name = $dawnSBResource.Name
    }

    $adxDatabase = @{
                name = 'sapact'
                softDeletePeriod =  'P10Y'
                hotCachePeriod = ($Environment -Eq 'prod') ? 'P3Y' : 'P30D'
                permissions = @(
                    @{
                        principalId = $clusterIdentityObjectId
                        principalType = 'App'
                        role = 'User'
                    },
                    @{
                        principalId = $githubActionsDevIdentityObjectId
                        principalType = 'App'
                        role = 'Viewer'
                    }
                )
            }

    $rgDatabase = az group show --name (Resolve-UniResourceName 'resource-group' $global:p_dataSql -Environment $Environment) | ConvertFrom-Json
    $replicaRGDatabase = az group show --name (Resolve-UniResourceName 'resource-group' $global:p_dataSql -Environment $Environment -Region 'we') | ConvertFrom-Json
        
    $sqlDatabase = @{
        resourceGroup = @{
            name = $rgDatabase.name
            location = $rgDatabase.location
        }
        replicaResourceGroup = @{
            name = $replicaRGDatabase.name
            location = $replicaRGDatabase.location
        }
        name = Resolve-UniResourceName 'sql-server-database' $p_sapactProjectName -Environment $Environment
        server = @{
            name = Resolve-UniResourceName 'sql-server' $global:p_dataSql -Environment $Environment
            elasticPool = @{
                name = Resolve-UniResourceName 'sql-elastic-pool' $global:p_dataSql -Environment $Environment
            }
        }
        replicaServer = @{
            name = Resolve-UniResourceName 'sql-server' $global:p_dataSql -Environment $Environment -Region "we"
            elasticPool = @{
                name = Resolve-UniResourceName 'sql-elastic-pool' $global:p_dataSql -Environment $Environment -Region "we"
            }
        }
    } 

    if ($PSCmdlet.ShouldProcess('SapAct', 'Deploy')) {

        $deploymentName = Resolve-DeploymentName

        # New-AzResourceGroupDeployment -Mode Incremental `
        #                               -Name $deploymentName `
        #                               -ResourceGroupName $devopsDomainRgName `
        #                               -TemplateFile $sapactTemplateFile `
        #                               -adxClusterName $adxClusterName `
        #                               -adxDatabase $adxDatabase `
        #                               -appKeyVaultName $devopsAppKeyVault `
        #                               -dawnSB $dawnSB `
        #                               -devopsSBNamespace $devopsServiceBusName `
        #                               -dceName $dceEndpointName `
        #                               -logAnalytics $logAnalyticsDef `
        #                               -storageAccountName $devopsStorageAccountName `
        #                               -environment $Environment `
        #                               -actionGroupDevOpsLowId $actionGroupDevOpsLowId.Id `
        #                               -sqlDatabase $sqlDatabase `
        #                               -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)
  
  
        New-SqlDatabaseADUser -ServerName $sqlDatabase.server.name `
                              -DatabaseName $sqlDatabase.name `
                              -UserName $devopsClusterIdentityName `
                              -Roles @("db_datareader", "db_datawriter", "db_ddladmin") `
                              -Verbose:$PSCmdlet.MyInvocation.BoundParameters['Verbose'].IsPresent
}
    else {
        $TestResult = Test-AzResourceGroupDeployment -Mode Incremental `
                                                     -ResourceGroupName $devopsDomainRgName `
                                                     -TemplateFile $sapactTemplateFile `
                                                     -adxClusterName $adxClusterName `
                                                     -adxDatabase $adxDatabase `
                                                     -appKeyVaultName $devopsAppKeyVault `
                                                     -dawnSB $dawnSB `
                                                     -devopsSBNamespace $devopsServiceBusName `
                                                     -dceName $dceEndpointName `
                                                     -logAnalytics $logAnalyticsDef `
                                                     -storageAccountName $devopsStorageAccountName `
                                                     -environment $Environment `
                                                     -actionGroupDevOpsLowId $actionGroupDevOpsLowId.Id `
                                                     -sqlDatabase $sqlDatabase `
                                                     -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)

        if ($TestResult) {
            $TestResult
            throw "The deployment for $sapactTemplateFile did not pass validation."
        }
    }
}