namespace GatewayService.Models.PaymentServiceDto;

public partial class PaymentServiceRequest
{
    public required string Status { get; set; }

    public int Price { get; set; }
}
