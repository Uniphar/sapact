namespace SapAct.Extensions;

public static class ApplicationHostExtensions
{
	public static async Task InitializeResourcesAsync(this IHost host	)
	{
		var resourceInitializerService = host.Services.GetRequiredService<ResourceInitializerService>();
		host.Services.GetRequiredService<SQLService>().CheckConnection();
		await resourceInitializerService.InitializeResourcesAsync();
	}
}
