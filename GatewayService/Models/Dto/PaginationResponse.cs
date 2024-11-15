namespace GatewayService.Models.Dto;

public class PaginationResponse
{
    public int Page { get; set; }
    public int PageSize { get; set; }
    public int TotalElements { get; set; }
    public List<HotelResponse> Items { get; set; } = [];
}