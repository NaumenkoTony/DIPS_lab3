namespace GatewayService.Models.LoyaltyServiceDto;

public class LoyaltyServiceResponse
{
    public required string Status { get; set; }
    public int Discount { get; set; }
    public int ReservationCount { get; set; }
}