namespace PaymentService.Models.Dto;

public partial class PaymentRequest
{
    public required string Status { get; set; }

    public int Price { get; set; }
}
