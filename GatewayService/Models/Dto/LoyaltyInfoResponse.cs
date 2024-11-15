namespace GatewayService.Models.Dto;

public class LoyaltyInfoResponse
{
    public string Status { get; set; } = "";
    public int Discount { get; set; }
    public int ReservationCount { get; set; }
}