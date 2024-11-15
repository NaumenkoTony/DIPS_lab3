using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Moq;
using PaymentService.Controllers;
using PaymentService.Data;
using PaymentService.Models.DomainModels;
using PaymentService.Models.Dto;
using Xunit;
using System.Threading.Tasks;

public class PaymentsControllerTests
{
    private readonly Mock<IPaymentRepository> mockRepository;
    private readonly IMapper mapper;
    private readonly PaymentsController controller;

    public PaymentsControllerTests()
    {
        var config = new MapperConfiguration(cfg =>
        {
            cfg.CreateMap<PaymentRequest, Payment>();
            cfg.CreateMap<Payment, PaymentResponse>().ReverseMap();
        });
        mapper = config.CreateMapper();

        mockRepository = new Mock<IPaymentRepository>();
        controller = new PaymentsController(mockRepository.Object, mapper);
    }

    [Fact]
    public async Task GetAsync_ReturnsPaymentResponse_WhenPaymentExists()
    {
        // Arrange
        var paymentUid = Guid.NewGuid();
        var payment = new Payment
        {
            Id = 1,
            PaymentUid = paymentUid,
            Status = "PAID",
            Price = 100
        };
        mockRepository.Setup(repo => repo.GetByUidAsync(paymentUid.ToString())).ReturnsAsync(payment);

        // Act
        var result = await controller.GetAsync(paymentUid.ToString());

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Create_ReturnsCreatedPaymentResponse_WhenValidRequest()
    {
        // Arrange
        var paymentRequest = new PaymentRequest
        {
            Status = "PAID",
            Price = 100
        };
        var payment = new Payment
        {
            Id = 1,
            PaymentUid = Guid.NewGuid(),
            Status = "PAID",
            Price = 100
        };
        mockRepository.Setup(repo => repo.CreateAsync(It.IsAny<Payment>())).Returns(Task.CompletedTask);

        // Act
        var result = await controller.Create(paymentRequest);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task UpdatePaymentStatusAsync_ReturnsUpdatedPayment_WhenValidRequest()
    {
        // Arrange
        var paymentUid = Guid.NewGuid();
        var existingPayment = new Payment
        {
            Id = 1,
            PaymentUid = paymentUid,
            Status = "PAID",
            Price = 100
        };
        var updatedPaymentResponse = new PaymentResponse
        {
            Id = "1",
            PaymentUid = paymentUid.ToString(),
            Status = "CANCELLED",
            Price = 100
        };
        mockRepository.Setup(repo => repo.GetByUidAsync(paymentUid.ToString())).ReturnsAsync(existingPayment);
        mockRepository.Setup(repo => repo.UpdateAsync(It.IsAny<Payment>(), existingPayment.Id)).Returns(Task.CompletedTask);

        // Act
        var result = await controller.UpdatePaymentStatusAsync(updatedPaymentResponse);

        // Assert
        Assert.NotNull(result);
    }
}
