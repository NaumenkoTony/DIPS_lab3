namespace LoyaltyService.Controllers;

using Microsoft.AspNetCore.Mvc;
using System.Net.Http;

public class HealthController() : ControllerBase
{
    [Route("/manage/health")]
    [HttpGet]
    public IActionResult IsOk()
    {
        return Ok();
    }
}