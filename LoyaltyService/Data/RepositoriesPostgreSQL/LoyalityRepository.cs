namespace LoyaltyService.Data.RepositoriesPostgreSQL;

using LoyaltyService.Models.DomainModels;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Threading.Tasks;
public class LoyalityRepository(LoyaltiesContext context) : Repository<Loyalty>(context), ILoyalityRepository
{
    private LoyaltiesContext db = context;

    public async Task<Loyalty?> GetLoyalityByUsername(string username)
    {
        return await db.Loyalties.FirstOrDefaultAsync(r => r.Username == username);
    }

    public async Task ImproveLoyality(string username)
    {
        var loyalty = await db.Loyalties.FirstOrDefaultAsync(r => r.Username == username);

        if (loyalty != null)
        {
            loyalty.ReservationCount++;
            if (loyalty.ReservationCount >= 20)
            {
                loyalty.Status = "GOLD";
            }
            else if (loyalty.ReservationCount >= 10)
            {
                loyalty.Status = "SILVER";
            }
            else
            {
                loyalty.Status = "BRONZE";
            }

            await db.SaveChangesAsync();
        }
        else
        {
            throw new Exception("User not found");
        }
    }

    public async Task DegradeLoyality(string username)
    {
        var loyalty = await db.Loyalties.FirstOrDefaultAsync(r => r.Username == username);

        if (loyalty != null)
        {
            loyalty.ReservationCount--;
            if (loyalty.ReservationCount >= 20)
            {
                loyalty.Status = "GOLD";
            }
            else if (loyalty.ReservationCount >= 10)
            {
                loyalty.Status = "SILVER";
            }
            else
            {
                loyalty.Status = "BRONZE";
            }

            await db.SaveChangesAsync();
        }
        else
        {
            throw new Exception("User not found");
        }
    }
}