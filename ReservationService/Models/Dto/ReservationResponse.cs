namespace ReservationService.Models.Dto;

public partial class ReservationResponse
{
    public required string ReservationUid { get; set; }

    public required string Username { get; set; }

    public required string PaymentUid { get; set; }

    public required string HotelId { get; set; }

    public required string Status { get; set; }

    public required string StartDate { get; set; }

    public required string EndDate { get; set; }
}
