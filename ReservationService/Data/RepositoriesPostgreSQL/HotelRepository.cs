namespace ReservationService.Data.RepositoriesPostgreSQL;

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ReservationService.Models.DomainModels;

public class HotelRepository(ReservationsContext context) : Repository<Hotel>(context), IHotelRepository
{
    private ReservationsContext db = context;    

    public async Task<IEnumerable<Hotel>> GetHotelsAsync(int page, int size)
    {
        return await db.Hotels
                       .Skip(page * size)
                       .Take(size)
                       .ToListAsync();
    }

    public async Task<Hotel?> GetByUidAsync(string uid)
    {
        if (!Guid.TryParse(uid, out Guid parsedUid))
        {
            return null;
        }
        
        return await db.Hotels.FirstOrDefaultAsync(h => h.HotelUid == parsedUid);
    }
}