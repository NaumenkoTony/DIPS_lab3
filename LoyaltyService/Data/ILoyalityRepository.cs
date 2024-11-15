namespace LoyaltyService.Data;
using LoyaltyService.Models.DomainModels;

public interface ILoyalityRepository : IRepository<Loyalty>
{
    public Task<Loyalty?> GetLoyalityByUsername(string username);

    public Task ImproveLoyality(string username);
    public Task DegradeLoyality(string username);
}
