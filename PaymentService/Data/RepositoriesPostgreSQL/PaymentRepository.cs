namespace ReservationService.Data.RepositoriesPostgreSQL;

using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using PaymentService.Data;
using PaymentService.Models.DomainModels;

public class PaymentRepository(PaymentsContext context) : Repository<Payment>(context), IPaymentRepository
{
    private PaymentsContext db = context;

    
    public async Task<Payment?> GetByUidAsync(string uid)
    {
        if (!Guid.TryParse(uid, out Guid parsedPaymentUid))
        {
            return null;
        }
        
        return await db.Payments.FirstOrDefaultAsync(h => h.PaymentUid == parsedPaymentUid);
    }
}