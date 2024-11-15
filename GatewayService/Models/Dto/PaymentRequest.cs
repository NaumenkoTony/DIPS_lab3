namespace GatewayService.Models.Dto;

public partial class PaymentRequest
{
    public string Status { get; set; } = null!;

    public int Price { get; set; }
}
