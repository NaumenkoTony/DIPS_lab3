using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using ReservationService.Data;
using ReservationService.Models.DomainModels;
using ReservationService.Models.Dto;

namespace ReservationService.Controllers;

public class HotelsController(IHotelRepository repository, IMapper mapper) : Controller
{
    private readonly IHotelRepository repository = repository;
    private readonly IMapper mapper = mapper;

    [Route("api/v1/[controller]")]
    [HttpGet]
    public async Task<ActionResult<IEnumerable<HotelResponse>>> GetAsync([FromQuery] int page = 1, [FromQuery] int size = 10)
    {   
        var hotels = await repository.GetHotelsAsync(page - 1, size);
        if (hotels == null)
        {
            return NoContent();
        }

        return Ok(mapper.Map<IEnumerable<HotelResponse>>(hotels));
    }

    [Route("api/v1/[controller]/{uid}")]
    [HttpGet]
    public async Task<ActionResult<HotelResponse>> GetAsync(string uid)
    {   
        return Ok(mapper.Map<HotelResponse>(await repository.GetByUidAsync(uid)));
    }
}