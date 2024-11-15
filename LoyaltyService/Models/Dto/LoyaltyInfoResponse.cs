using LoyaltyService.Models.DomainModels;

namespace LoyaltyService.Models.Dto;

public class LoyaltyResponse
{
    public required string Status { get; set; }
    public int Discount { get; set; }
    public int ReservationCount { get; set; }
}