namespace GatewayService.Models.Dto;
using System.Text.Json.Serialization;
using GatewayService.Models.LoyaltyServiceDto;

public class UserInfoResponse
{
    public List<ReservationResponse> Reservations { get; set; } = [];

    [JsonIgnore]
    public LoyaltyServiceResponse? Loyalty { get; set; }

    [JsonPropertyName("loyalty")]
    public object LoyaltyJson => Loyalty != null ? (object)Loyalty : "";
}