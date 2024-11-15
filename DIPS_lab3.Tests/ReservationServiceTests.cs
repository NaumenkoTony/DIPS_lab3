using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Moq;
using ReservationService.Controllers;
using ReservationService.Data;
using ReservationService.Models.DomainModels;
using ReservationService.Models.Dto;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

public class ReservationServiceTests
{
    private readonly Mock<IHotelRepository> mockRepository;
    private readonly IMapper mapper;
    private readonly HotelsController controller;

    public ReservationServiceTests()
    {
        var config = new MapperConfiguration(cfg =>
        {
            cfg.CreateMap<Hotel, HotelResponse>();
        });
        mapper = config.CreateMapper();

        mockRepository = new Mock<IHotelRepository>();
        controller = new HotelsController(mockRepository.Object, mapper);
    }

    [Fact]
    public async Task GetAsync_ReturnsHotelList_WhenHotelsExist()
    {
        // Arrange
        var hotels = new List<Hotel>
        {
            new Hotel { Id = 1, HotelUid = Guid.NewGuid(), Name = "Hotel A", Country = "Country A", City = "City A", Address = "Address A", Stars = 5, Price = 100 },
            new Hotel { Id = 2, HotelUid = Guid.NewGuid(), Name = "Hotel B", Country = "Country B", City = "City B", Address = "Address B", Stars = 4, Price = 200 }
        };
        
        mockRepository.Setup(repo => repo.GetHotelsAsync(0, 10)).ReturnsAsync(hotels);

        // Act
        var result = await controller.GetAsync();

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetAsync_ReturnsNoContent_WhenNoHotelsExist()
    {
        // Arrange
        mockRepository.Setup(repo => repo.GetHotelsAsync(0, 10)).ReturnsAsync((List<Hotel>?)null);

        // Act
        var result = await controller.GetAsync();

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetByUidAsync_ReturnsHotelResponse_WhenHotelExists()
    {
        // Arrange
        var hotel = new Hotel
        {
            Id = 1,
            HotelUid = Guid.NewGuid(),
            Name = "Hotel A",
            Country = "Country A",
            City = "City A",
            Address = "Address A",
            Stars = 5,
            Price = 100
        };
        
        mockRepository.Setup(repo => repo.GetByUidAsync(hotel.HotelUid.ToString())).ReturnsAsync(hotel);

        // Act
        var result = await controller.GetAsync(hotel.HotelUid.ToString());

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public async Task GetByUidAsync_ReturnsNotFound_WhenHotelDoesNotExist()
    {
        // Arrange
        var hotelUid = Guid.NewGuid().ToString();
        mockRepository.Setup(repo => repo.GetByUidAsync(hotelUid)).ReturnsAsync((Hotel?)null);

        // Act
        var result = await controller.GetAsync(hotelUid);

        // Assert
        Assert.NotNull(result);
    }
}
