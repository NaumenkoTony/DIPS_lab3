using System;
using System.Collections.Generic;

namespace LoyaltyService.Models.DomainModels;

public partial class Loyalty
{
    public int Id { get; set; }

    public string Username { get; set; } = null!;

    public int ReservationCount { get; set; }

    public string Status { get; set; } = null!;

    public int Discount { get; set; }
}
