namespace LoyaltyService.Controllers;

using AutoMapper;
using LoyaltyService.Data;
using LoyaltyService.Models.Dto;
using Microsoft.AspNetCore.Mvc;

public class LoyaltiesController(ILoyalityRepository repository, IMapper mapper) : Controller
{
    private readonly ILoyalityRepository repository = repository;
    private readonly IMapper mapper = mapper;
    
    [Route("/api/v1/[controller]")]
    [HttpGet]
    public async Task<ActionResult<LoyaltyResponse>> GetByUsername([FromHeader(Name = "X-User-Name")] string username)
    {
        return Ok(mapper.Map<LoyaltyResponse>(await repository.GetLoyalityByUsername(username)));
    }

    [Route("/api/v1/[controller]/improve")]
    [HttpGet]
    public async Task<ActionResult> ImproveLoyality([FromHeader(Name = "X-User-Name")] string username)
    {
        await repository.ImproveLoyality(username);
        return Ok();
    }

    
    [Route("/api/v1/[controller]/degrade")]
    [HttpGet]
    public async Task<ActionResult> DegradeLoyality([FromHeader(Name = "X-User-Name")] string username)
    {
        await repository.DegradeLoyality(username);
        return Ok();
    }
}