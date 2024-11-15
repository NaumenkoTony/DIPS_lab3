
namespace GatewayService.Models.Dto;

public partial class Payment
{
    public int Id { get; set; }

    public Guid PaymentUid { get; set; }

    public string Status { get; set; } = null!;

    public int Price { get; set; }
}
