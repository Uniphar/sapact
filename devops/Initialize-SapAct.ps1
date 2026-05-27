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

    $devopsDomainRgName = Resolve-UniResourceName 'resource-group' $p_devopsDomain -Environment $Environment
    $dawnDomainRgName = Resolve-UniResourceName 'resource-group' $p_dawnDomain -Environment $Environment
    $devopsAppKeyVault = Resolve-UniResourceName 'keyvault' "$p_devopsDomain-app" -Dev:$Dev -Environment $Environment
    $dawnServiceBusName = Resolve-UniResourceName 'service-bus' $p_dawnDomain -Environment $Environment
    $devopsServiceBusName = Resolve-UniResourceName 'service-bus' $p_devopsDomain -Environment $Environment
    $dceEndpointName = Resolve-UniResourceName 'monitor-dce' $p_sapactProjectName -Environment $Environment
    $devopsStorageAccountName = Resolve-UniResourceName 'storage' $p_devopsDomain -Dev:$Dev -Environment $Environment
    $logAnalyticsWorkspace = Resolve-UniMainLogAnalytics $Environment
    
    $sbName = $Environment -eq "prod" ? "$dawnServiceBusName-primary" : $dawnServiceBusName

    $dawnSBResource = Get-AzResource -Name $sbName -ResourceGroupName $dawnDomainRgName

    if ($Environment -eq 'prod') {
        $dawnSBResourceSecondary = Get-AzResource -Name "$dawnServiceBusName-secondary" -ResourceGroupName $dawnDomainRgName
    }

    
    $logAnalyticsDef = @{
        Name              = $logAnalyticsWorkspace.Name
        CustomerId        = $logAnalyticsWorkspace.CustomerId
        AzureId           = $logAnalyticsWorkspace.ResourceId
        ResourceGroupName = $logAnalyticsWorkspace.ResourceGroupName
    }

    $dawnSB = @{
        Id          = $dawnSBResource.Id
        Name        = $dawnSBResource.Name
        Alias       = $dawnServiceBusName
        SecondaryId = $dawnSBResourceSecondary.Id
    }


    if ($PSCmdlet.ShouldProcess('SapAct', 'Deploy')) {

        $deploymentName = Resolve-DeploymentName

        New-AzResourceGroupDeployment -Mode Incremental `
            -Name $deploymentName `
            -ResourceGroupName $devopsDomainRgName `
            -TemplateFile $sapactTemplateFile `
            -appKeyVaultName $devopsAppKeyVault `
            -dawnSB $dawnSB `
            -devopsSBNamespace $devopsServiceBusName `
            -dceName $dceEndpointName `
            -logAnalytics $logAnalyticsDef `
            -storageAccountName $devopsStorageAccountName `
            -environment $Environment `
            -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)

    }
    else {
        $TestResult = Test-AzResourceGroupDeployment -Mode Incremental `
            -ResourceGroupName $devopsDomainRgName `
            -TemplateFile $sapactTemplateFile `
            -appKeyVaultName $devopsAppKeyVault `
            -dawnSB $dawnSB `
            -devopsSBNamespace $devopsServiceBusName `
            -dceName $dceEndpointName `
            -logAnalytics $logAnalyticsDef `
            -storageAccountName $devopsStorageAccountName `
            -environment $Environment `
            -Verbose:($PSCmdlet.MyInvocation.BoundParameters["Verbose"].IsPresent -eq $true)

        if ($TestResult) {
            $TestResult
            throw "The deployment for $sapactTemplateFile did not pass validation."
        }
    }
}