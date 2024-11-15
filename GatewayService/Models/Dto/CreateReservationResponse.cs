namespace GatewayService.Models.Dto;

public partial class CreateReservationResponse
{
    public required string ReservationUid { get; set; }

    public required string HotelUid { get; set; }

    public required string StartDate { get; set; }

    public required string EndDate { get; set; }

    public required double Discount { get; set; }

    public required string Status { get; set; }
    
    public required PaymentInfo Payment { get; set; }
}
