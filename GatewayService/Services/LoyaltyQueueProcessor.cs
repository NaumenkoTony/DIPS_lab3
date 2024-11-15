using System.Net.Http;
using StackExchange.Redis;

class LoyaltyQueueProcessor(IHttpClientFactory httpClientFactory, IConnectionMultiplexer redis) : BackgroundService
{
    private readonly IConnectionMultiplexer redis = redis;
    private readonly IHttpClientFactory httpClientFactory = httpClientFactory;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = redis.GetDatabase();
        while (!stoppingToken.IsCancellationRequested)
        {
            string? username = await db.ListLeftPopAsync("loyalty-queue");
            try
            {
                if (username == null)
                {
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                var loyaltyClient = httpClientFactory.CreateClient("LoyaltyService");
                var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/loyalties/degrade");
                request.Headers.Add("X-User-Name", username);

                var response = await loyaltyClient.SendAsync(request, stoppingToken);

                if (!response.IsSuccessStatusCode)
                {
                    await db.ListRightPushAsync("loyalty-queue", username);
                }
            }
            catch (Exception ex)
            {
                if (!string.IsNullOrEmpty(username))
                {
                    await db.ListRightPushAsync("loyalty-queue", username);
                    Console.WriteLine($"Ошибка при обработке {username}. Возвращен в очередь: {ex.Message}");
                }
            }
        }
    }
}