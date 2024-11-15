using Microsoft.EntityFrameworkCore;
using PaymentService.Data;
using ReservationService.Data.RepositoriesPostgreSQL;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services.AddDbContext<PaymentsContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("PaymentService")));

builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();

builder.Services.AddAutoMapper(typeof(Program));

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsContext>();
    
    try
    {
        dbContext.Database.OpenConnection();
        dbContext.Database.CloseConnection();
        Console.WriteLine("Connected to database");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error connecting to database: {ex.Message}");
        Environment.Exit(1);
    }
}

app.MapControllers();

app.Run();
