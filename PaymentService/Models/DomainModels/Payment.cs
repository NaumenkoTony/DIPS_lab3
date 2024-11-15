using System;
using System.Collections.Generic;

namespace PaymentService.Models.DomainModels;

public partial class Payment
{
    public int Id { get; set; }

    public Guid PaymentUid { get; set; } = Guid.NewGuid();

    public string Status { get; set; } = null!;

    public int Price { get; set; }
}
