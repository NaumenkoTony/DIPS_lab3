namespace ReservationService.Models.Dto;

public partial class HotelResponse
{
    public required string Id { get; set; }
    
    public required string HotelUid { get; set; }

    public required string Name { get; set; }

    public required string Country { get; set; }

    public required string City { get; set; }

    public required string Address { get; set; }

    public int? Stars { get; set; }

    public int Price { get; set; }
}
