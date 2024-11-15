using PaymentService.Models.DomainModels;

namespace PaymentService.Data;

public interface IPaymentRepository : IRepository<Payment>
{
    public Task<Payment?> GetByUidAsync(string uid);
}
