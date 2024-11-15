namespace GatewayService.Models.Dto;

public partial class CreateReservationRequest
{
    public required string HotelUid { get; set; }

    public required string StartDate { get; set; }

    public required string EndDate { get; set; }
}
