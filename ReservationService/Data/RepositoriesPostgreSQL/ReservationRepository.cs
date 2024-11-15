namespace ReservationService.Data.RepositoriesPostgreSQL;

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ReservationService.Models.DomainModels;

public class ReservationRepository(ReservationsContext context) : Repository<Reservation>(context), IReservationRepository
{
    private ReservationsContext db = context;

    public async Task<IEnumerable<Reservation>> GetReservationsByUsernameAsync(string username)
    {
        return await db.Reservations
                       .Where(r => r.Username == username)
                       .ToListAsync(); 
    }

    public async Task<Reservation?> GetByUidAsync(string uid)
    {
        if (!Guid.TryParse(uid, out Guid parsedUid))
        {
            return null;
        }
        
        return await db.Reservations.FirstOrDefaultAsync(h => h.ReservationUid == parsedUid);
    }

    public async Task<Reservation?> GetByUsernameUidAsync(string username, string uid)
    {
        if (!Guid.TryParse(uid, out Guid parsedUid))
        {
            return null;
        }
        
        return await db.Reservations.FirstOrDefaultAsync(h => h.ReservationUid == parsedUid && h.Username == username);

    }
}