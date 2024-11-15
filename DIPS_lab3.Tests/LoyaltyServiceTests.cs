namespace LoyaltyServiceTests.Tests;

using Xunit;
using Moq;
using LoyaltyService.Controllers;
using LoyaltyService.Models.DomainModels;
using LoyaltyService.Models.Dto;
using LoyaltyService.Data;
using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using LoyaltyService.Mapping;
using Microsoft.AspNetCore.Mvc;

public class LoyaltyServiceTests
{
    private readonly Mock<ILoyalityRepository> mockRepository;
    private readonly LoyaltiesController controller;
    private readonly IMapper mapper;

    public LoyaltyServiceTests()
    {
        var config = new MapperConfiguration(cfg =>
        {
            cfg.AddProfile<MappingProfile>();
        });

        mapper = config.CreateMapper();

        mockRepository = new Mock<ILoyalityRepository>();

        controller = new LoyaltiesController(mockRepository.Object, mapper);
    }

    [Fact]
    public async Task GetByUsername_ReturnsLoyaltyResponse()
    {
        // Arrange
        var username = "testUser";
        var loyalty = new Loyalty
        {
            Id = 1,
            Username = username,
            ReservationCount = 5,
            Status = "Silver",
            Discount = 10
        };

        mockRepository.Setup(repo => repo.GetLoyalityByUsername(username)).ReturnsAsync(loyalty);

        // Act
        var result = await controller.GetByUsername(username);

        // Assert
        mockRepository.Verify(repo => repo.GetLoyalityByUsername(username), Times.Once);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task ImproveLoyality_ReturnsOk()
    {
        // Arrange
        var username = "testUser";
        mockRepository.Setup(repo => repo.ImproveLoyality(username)).Returns(Task.CompletedTask);

        // Act
        var result = await controller.ImproveLoyality(username);

        // Assert
        Assert.IsType<OkResult>(result);
        mockRepository.Verify(repo => repo.ImproveLoyality(username), Times.Once);
    }

    [Fact]
    public async Task DegradeLoyality_ReturnsOk()
    {
        // Arrange
        var username = "testUser";
        mockRepository.Setup(repo => repo.DegradeLoyality(username)).Returns(Task.CompletedTask);

        // Act
        var result = await controller.DegradeLoyality(username);

        // Assert
        Assert.IsType<OkResult>(result);
        mockRepository.Verify(repo => repo.DegradeLoyality(username), Times.Once);
    }
}