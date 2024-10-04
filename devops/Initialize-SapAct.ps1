function Initialize-SapAct {
<#
.SYNOPSIS
Deploys the SAP ACT database to the Azure Data Explorer cluster.

.DESCRIPTION
This function deploys the SAP ACT database to the Azure Data Explorer cluster.

.PARAMETER Environment
The environment to deploy the SAP ACT database to.

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

    $adxClusterName = Resolve-UniResourceName 'adx-cluster' $p_devopsDomain -Environment $Environment

    $devopsDomainRgName = Resolve-UniResourceName 'resource-group' $p_devopsDomain -Dev:$Dev -Environment $Environment
    $devopsClusterIdentityName = Resolve-UniComputeDomainSAName $Environment $p_devopsDomain
    $clusterIdentityObjectId = Get-AzADServicePrincipal -DisplayName $devopsClusterIdentityName | Select-Object -ExpandProperty Id
    $githubActionsDevIdentityObjectId = Get-AzADServicePrincipal -DisplayName $adapp_GithubActionsDev | Select-Object -ExpandProperty Id

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

    if ($PSCmdlet.ShouldProcess('SapAct', 'Deploy')) {

        $deploymentName = Resolve-DeploymentName

        New-AzResourceGroupDeployment -Mode Incremental `
                                      -Name $deploymentName `
                                      -ResourceGroupName $devopsDomainRgName `
                                      -TemplateFile $sapactTemplateFile `
                                      -adxClusterName $adxClusterName `
                                      -adxDatabase $adxDatabase `
                                      -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)
    }
    else {
        $TestResult = Test-AzResourceGroupDeployment -Mode Incremental `
                                                     -ResourceGroupName $devopsDomainRgName `
                                                     -TemplateFile $sapactTemplateFile `
                                                     -adxClusterName $adxClusterName `
                                                     -adxDatabase $adxDatabase `
                                                     -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)

        if ($TestResult) {
            $TestResult
            throw "The deployment for $sapactTemplateFile did not pass validation."
        }
    }
}