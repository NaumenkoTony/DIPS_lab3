namespace GatewayService.Models.Dto;

public partial class HotelInfo
{
    public required string HotelUid { get; set; }

    public required string Name { get; set; }

    public required string FullAddress { get; set; }

    public int? Stars { get; set; }
}
