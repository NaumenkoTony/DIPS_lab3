namespace GatewayService.Models.Dto;

public partial class ReservationRequest
{
    public required string Username { get; set; }

    public required string PaymentUid { get; set; }

    public required string HotelId { get; set; }

    public required string Status { get; set; }

    public required string StartDate { get; set; }

    public required string EndData { get; set; }


}
