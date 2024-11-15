namespace GatewayService.Controllers;

using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using System.Threading.Tasks;
using GatewayService.Models.Dto;
using Newtonsoft.Json;
using AutoMapper;
using System.Text;
using GatewayService.Models.ReservationServiceDto;
using GatewayService.Models.LoyaltyServiceDto;
using GatewayService.Models.PaymentServiceDto;
using GatewayService.Services;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

public class GatewayController(IHttpClientFactory httpClientFactory, IMapper mapper, ILogger<GatewayController> logger, IConnectionMultiplexer redis) : ControllerBase
{
    private readonly IHttpClientFactory httpClientFactory = httpClientFactory;
    private readonly IMapper mapper = mapper;
    private readonly ILogger<GatewayController> logger = logger;
    private readonly IConnectionMultiplexer redis = redis;

    private readonly CircuitBreaker reservationServiceCircuitBreaker = new CircuitBreaker(5, TimeSpan.FromSeconds(60));
    private readonly CircuitBreaker loyaltyServiceCircuitBreaker = new CircuitBreaker(5, TimeSpan.FromSeconds(60));
    private readonly CircuitBreaker PaymentServiceCircuitBreaker = new CircuitBreaker(5, TimeSpan.FromSeconds(60));

    private async Task<ReservationResponse> CreateReservationResponseAsync(ReservationServiceResponse reservationServiceResponse)
    {
        HotelInfo? hotelInfo = null;
        PaymentInfo? paymentInfo = null;

        try
        {
            var reservationService = httpClientFactory.CreateClient("ReservationService");
            if (reservationServiceCircuitBreaker.AllowRequest())
            {
                var hotelResponse = await reservationService.GetAsync($"/api/v1/reservations/hotels/{reservationServiceResponse.HotelId}");
                if (!hotelResponse.IsSuccessStatusCode)
                {
                    reservationServiceCircuitBreaker.RecordFailure();
                }
                else
                {
                    reservationServiceCircuitBreaker.RecordSuccess();
                    var hotelJsonString = await hotelResponse.Content.ReadAsStringAsync();
                    var hotel = JsonConvert.DeserializeObject<HotelServiceResponse>(hotelJsonString);
                    hotelInfo = mapper.Map<HotelInfo>(hotel);
                }
            }
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
        }

        try
        {
            var paymentService = httpClientFactory.CreateClient("PaymentService");
            if (PaymentServiceCircuitBreaker.AllowRequest())
            {
                var paymentResponse = await paymentService.GetAsync($"/api/v1/payments/{reservationServiceResponse.PaymentUid}");
                if (!paymentResponse.IsSuccessStatusCode)
                {
                    PaymentServiceCircuitBreaker.RecordFailure();
                }
                else
                {
                    PaymentServiceCircuitBreaker.RecordSuccess();
                    var paymentJsonString = await paymentResponse.Content.ReadAsStringAsync();
                    var payment = JsonConvert.DeserializeObject<PaymentServiceResponse>(paymentJsonString);
                    paymentInfo = mapper.Map<PaymentInfo>(payment);
                }
            }
        }
        catch (Exception)
        {
            PaymentServiceCircuitBreaker.RecordFailure();
        }

        var reservationResponse = new ReservationResponse
        {
            ReservationUid = reservationServiceResponse.ReservationUid,
            Hotel = hotelInfo,
            StartDate = reservationServiceResponse.StartDate,
            EndDate = reservationServiceResponse.EndDate,
            Status = reservationServiceResponse.Status,
            Payment = paymentInfo
        };

        return reservationResponse;
    }

    [Route("api/v1/hotels")]
    [HttpGet]
    public async Task<ActionResult<PaginationResponse>> GetHotelsAsync([FromQuery] int page, [FromQuery] int size)
    {
        if (page < 0 || size < 1)
        {
            return BadRequest(new { message = "Invalid pagination parameters" });
        }

        try
        {
            if (!reservationServiceCircuitBreaker.AllowRequest())
            {
                return StatusCode(503, new { message = "Service unavailable. Please try again later." });
            }

            var reservationService = httpClientFactory.CreateClient("ReservationService");
            var hotelsResponse = await reservationService.GetAsync($"/api/v1/hotels?page={page}&size={size}");
            if (!hotelsResponse.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode(503, new { message = "Error receiving hotels from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var hotelsJsonString = await hotelsResponse.Content.ReadAsStringAsync();
            var hotelsServiceResponse = JsonConvert.DeserializeObject<List<HotelServiceResponse>>(hotelsJsonString);
            if (hotelsServiceResponse == null)
            {
                return StatusCode(503, new { message = "Error parsing hotel data" });
            }

            var pagedResponse = new PaginationResponse
            {
                Page = page,
                PageSize = size,
                TotalElements = hotelsServiceResponse.Count,
                Items = mapper.Map<List<HotelResponse>>(hotelsServiceResponse)
            };

            return Ok(pagedResponse);
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }
    }


    [Route("/api/v1/me")]
    [HttpGet]
    public async Task<ActionResult<UserInfoResponse>> GetInfoAsync([FromHeader(Name = "X-User-Name")] string username)
    {
        try
        {
            if (!reservationServiceCircuitBreaker.AllowRequest())
            {
                return StatusCode(503, new { message = "Reservation Service unavailable" });
            }
            var reservationService = httpClientFactory.CreateClient("ReservationService");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/reservations");
            request.Headers.Add("X-User-Name", username);
            var reservationsResponse = await reservationService.SendAsync(request);
            if (!reservationsResponse.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode((int)reservationsResponse.StatusCode, new { message = "Error receiving reservations from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var reservationsJsonString = await reservationsResponse.Content.ReadAsStringAsync();
            var reservations = JsonConvert.DeserializeObject<List<ReservationServiceResponse>>(reservationsJsonString);
            if (reservations == null)
            {
                return StatusCode(503, new { message = "Error parsing reservation data" });
            }

            List<ReservationResponse> reservationResponses = [];
            foreach (var reservation in reservations)
            {
                reservationResponses.Add(await CreateReservationResponseAsync(reservation));
            }

            LoyaltyServiceResponse? loyalty = null;
            try
            {
                if (loyaltyServiceCircuitBreaker.AllowRequest())
                {
                    var loyaltyService = httpClientFactory.CreateClient("LoyaltyService");
                    request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/loyalties");
                    request.Headers.Add("X-User-Name", username);
                    var loyaltiesResponse = await loyaltyService.SendAsync(request);
                    if (!loyaltiesResponse.IsSuccessStatusCode)
                    {
                        loyaltyServiceCircuitBreaker.RecordFailure();
                        return StatusCode((int)loyaltiesResponse.StatusCode, new { message = "Error receiving loyalty from microservice" });
                    }
                    loyaltyServiceCircuitBreaker.RecordSuccess();
                    var loyaltiesResponseJsonString = await loyaltiesResponse.Content.ReadAsStringAsync();
                    loyalty = JsonConvert.DeserializeObject<LoyaltyServiceResponse>(loyaltiesResponseJsonString);
                    if (loyalty == null)
                    {
                        return StatusCode(503, new { message = "Error parsing loyalty data" });
                    }
                }
            }
            catch (Exception)
            {
                loyaltyServiceCircuitBreaker.RecordFailure();
            }

            UserInfoResponse userInfoResponse = new UserInfoResponse
            {
                Reservations = reservationResponses,
                Loyalty = loyalty
            };

            return Ok(userInfoResponse);
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable." });
        }
    }

    [Route("api/v1/loyalty")]
    [HttpGet]
    public async Task<ActionResult<LoyaltyInfoResponse>> GetLoyalty([FromHeader(Name = "X-User-Name")] string username)
    {
        try
        {
            if (!loyaltyServiceCircuitBreaker.AllowRequest())
            {
                return StatusCode(503, new { message = "Loyalty Service unavailable" });
            }

            var loyaltyService = httpClientFactory.CreateClient("LoyaltyService");
            var request = new HttpRequestMessage(HttpMethod.Get, "/api/v1/loyalties");
            request.Headers.Add("X-User-Name", username);

            var loyaltyResponse = await loyaltyService.SendAsync(request);

            if (!loyaltyResponse.IsSuccessStatusCode)
            {
                loyaltyServiceCircuitBreaker.RecordFailure();
                return StatusCode(503, new { message = "Error receiving loyalties from microservice" });
            }

            loyaltyServiceCircuitBreaker.RecordSuccess();

            var loyaltyJsonString = await loyaltyResponse.Content.ReadAsStringAsync();
            var loyaltyServiceResponse = JsonConvert.DeserializeObject<LoyaltyServiceResponse>(loyaltyJsonString);

            if (loyaltyServiceResponse == null)
            {
                loyaltyServiceCircuitBreaker.RecordFailure();
                return StatusCode(503, new { message = "Error parsing loyalty data" });
            }

            return Ok(mapper.Map<LoyaltyInfoResponse>(loyaltyServiceResponse));
        }
        catch (Exception)
        {
            loyaltyServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Loyalty Service unavailable" });
        }
    }

    [Route("api/v1/reservations")]
    [HttpPost]
    public async Task<ActionResult<LoyaltyInfoResponse>> BookHotel([FromHeader(Name = "X-User-Name")] string username, [FromBody] CreateReservationRequest createReservationRequest)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }


        if (!reservationServiceCircuitBreaker.AllowRequest())
        {
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }

        HotelServiceResponse? hotel;
        HttpClient reservationService;
        try
        {
            reservationService = httpClientFactory.CreateClient("ReservationService");

            var hotelResponse = await reservationService.GetAsync($"/api/v1/hotels/{createReservationRequest.HotelUid}");
            if (!hotelResponse.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode((int)hotelResponse.StatusCode, new { message = $"Error receiving hotel from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var hotelJsonString = await hotelResponse.Content.ReadAsStringAsync();
            hotel = JsonConvert.DeserializeObject<HotelServiceResponse>(hotelJsonString);
            if (null == hotel)
            {
                return NotFound($"Hotel with uid {createReservationRequest.HotelUid} was not found");
            }
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }

        DateTime startDate = DateTime.Parse(createReservationRequest.StartDate);
        DateTime endDate = DateTime.Parse(createReservationRequest.EndDate);
        int nightsNumber = (endDate - startDate).Days;
        int cost = nightsNumber * hotel.Price;
        if (!loyaltyServiceCircuitBreaker.AllowRequest())
        {
            return StatusCode(503, new { message = "Loyalty Service unavailable" });
        }

        LoyaltyInfoResponse? loyalty;
        HttpClient loyaltyService;
        try
        {
            loyaltyService = httpClientFactory.CreateClient("LoyaltyService");

            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/loyalties");
            request.Headers.Add("X-User-Name", username);
            var loyaltyResponse = await loyaltyService.SendAsync(request);
            if (!loyaltyResponse.IsSuccessStatusCode)
            {
                loyaltyServiceCircuitBreaker.RecordFailure();
                return StatusCode((int)loyaltyResponse.StatusCode, new { message = "Error receiving loyalties from microservice" });
            }
            loyaltyServiceCircuitBreaker.RecordSuccess();
            var loyaltyJsonString = await loyaltyResponse.Content.ReadAsStringAsync();
            var loyaltyServiceResponse = JsonConvert.DeserializeObject<LoyaltyServiceResponse>(loyaltyJsonString);
            if (loyaltyServiceResponse == null)
            {
                return StatusCode(503, new { message = "Error parsing loyalty data" });
            }
            loyalty = mapper.Map<LoyaltyInfoResponse>(loyaltyServiceResponse);
        }
        catch (Exception)
        {
            loyaltyServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Loyalty Service unavailable" });
        }
        double discount = loyalty.Discount;
        int costWithDiscount = Convert.ToInt32(cost * (100 - discount) / 100);

        var paymentRequest = new PaymentServiceRequest
        {
            Status = PaymentOperationStatus.PAID.ToString(),
            Price = costWithDiscount
        };

        if (!PaymentServiceCircuitBreaker.AllowRequest())
        {
            return StatusCode(503, new { message = "Payment Service unavailable" });
        }
        PaymentServiceResponse? payment;
        HttpClient paymentClient;
        try
        {
            paymentClient = httpClientFactory.CreateClient("PaymentService");

            string paymentJsonContent = System.Text.Json.JsonSerializer.Serialize(paymentRequest);
            var paymentContent = new StringContent(paymentJsonContent, Encoding.UTF8, "application/json");
            var paymentResponse = await paymentClient.PostAsync("/api/v1/payments", paymentContent);
            if (!paymentResponse.IsSuccessStatusCode)
            {
                PaymentServiceCircuitBreaker.RecordFailure();
                return StatusCode((int)paymentResponse.StatusCode, new { message = "Error posting payments from microservice" });
            }
            PaymentServiceCircuitBreaker.RecordSuccess();
            var paymentResponseJsonString = await paymentResponse.Content.ReadAsStringAsync();
            payment = JsonConvert.DeserializeObject<PaymentServiceResponse>(paymentResponseJsonString);
            if (null == payment)
            {
                return NotFound($"Payment was not created");
            }
        }
        catch (Exception)
        {
            PaymentServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Payment Service unavailable" });
        }

        var reservationRequest = new ReservationServiceRequest
        {
            Username = username,
            HotelId = hotel.Id,
            StartDate = startDate.ToString(),
            EndData = endDate.ToString(),
            Status = ReservationStatus.PAID.ToString(),
            PaymentUid = Convert.ToString(payment.PaymentUid)
        };
        var reservationJsonContent = System.Text.Json.JsonSerializer.Serialize(reservationRequest);
        var reservationContent = new StringContent(reservationJsonContent, Encoding.UTF8, "application/json");

        ReservationServiceResponse? reservation;
        try
        {
            reservationService = httpClientFactory.CreateClient("ReservationService");

            var reservationResponseBody = await reservationService.PostAsync("/api/v1/reservations/", reservationContent);
            if (!reservationResponseBody.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode((int)reservationResponseBody.StatusCode, new { message = "Error creating reservation from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var reservationResponseBodyJsonString = await reservationResponseBody.Content.ReadAsStringAsync();
            reservation = JsonConvert.DeserializeObject<ReservationServiceResponse>(reservationResponseBodyJsonString);
            if (null == reservation)
            {
                return NotFound($"Reservation was not created");
            }
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }

        HttpClient loyaltyClient;
        try
        {
            loyaltyClient = httpClientFactory.CreateClient("LoyaltyService");

            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/loyalties/improve");
            request.Headers.Add("X-User-Name", username);
            var loyaltyResponse = await loyaltyService.SendAsync(request);
        }
        catch (Exception)
        {
            loyaltyServiceCircuitBreaker.RecordFailure();
            await paymentClient.DeleteAsync($"/api/v1/payments/{payment.PaymentUid}");
            return StatusCode(503, new { message = "Loyalty Service unavailable" });
        }

        var createReservationResponse = new CreateReservationResponse
        {
            ReservationUid = reservation.ReservationUid,
            HotelUid = hotel.HotelUid,
            StartDate = createReservationRequest.StartDate,
            EndDate = createReservationRequest.EndDate,
            Discount = discount,
            Status = paymentRequest.Status,
            Payment = mapper.Map<PaymentInfo>(payment)
        };

        return Ok(createReservationResponse);
    }

    [Route("api/v1/reservations/{reservationUid}")]
    [HttpGet]
    public async Task<ActionResult<ReservationResponse>> GetReservationByUsername([FromHeader(Name = "X-User-Name")] string username, string reservationUid)
    {
        try
        {
            if (!reservationServiceCircuitBreaker.AllowRequest())
            {
                return StatusCode(503, new { message = "Reservation Service unavailable" });
            }
            var reservationService = httpClientFactory.CreateClient("ReservationService");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/reservations/{reservationUid}");
            request.Headers.Add("X-User-Name", username);
            var reservationServiceResponse = await reservationService.SendAsync(request);
            if (!reservationServiceResponse.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode(503, new { message = $"Error receiving hotel from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var reservationJsonString = await reservationServiceResponse.Content.ReadAsStringAsync();
            var reservation = JsonConvert.DeserializeObject<ReservationServiceResponse>(reservationJsonString);
            if (null == reservation)
            {
                return NotFound($"Reservations with uid {reservationUid} was not found");
            }

            ReservationResponse reservationResponse = await CreateReservationResponseAsync(reservation);

            return Ok(reservationResponse);
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }
    }

    [Route("api/v1/reservations")]
    [HttpGet]
    public async Task<ActionResult<ReservationResponse>> GetReservationsByUsername([FromHeader(Name = "X-User-Name")] string username)
    {
        try
        {
            if (!reservationServiceCircuitBreaker.AllowRequest())
            {
                return StatusCode(503, new { message = "Reservation Service unavailable" });
            }
            var reservationService = httpClientFactory.CreateClient("ReservationService");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/reservations");
            request.Headers.Add("X-User-Name", username);
            var reservationsResponse = await reservationService.SendAsync(request);
            if (!reservationsResponse.IsSuccessStatusCode)
            {
                reservationServiceCircuitBreaker.RecordFailure();
                return StatusCode(503, new { message = $"Error receiving hotel from microservice" });
            }
            reservationServiceCircuitBreaker.RecordSuccess();
            var reservationsJsonString = await reservationsResponse.Content.ReadAsStringAsync();
            var reservations = JsonConvert.DeserializeObject<List<ReservationServiceResponse>>(reservationsJsonString);
            if (reservations == null)
            {
                return StatusCode(503, new { message = "Error parsing reservation data" });
            }

            var paymentService = httpClientFactory.CreateClient("PaymentService");
            List<ReservationResponse> reservationResponses = [];
            foreach (var reservation in reservations)
            {
                reservationResponses.Add(await CreateReservationResponseAsync(reservation));
            }

            return Ok(reservationResponses);
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }
    }

    [Route("api/v1/reservations/{reservationUid}")]
    [HttpDelete]
    public async Task<ActionResult<ReservationResponse>> UnbookHotel([FromHeader(Name = "X-User-Name")] string username, string reservationUid)
    {
        if (!reservationServiceCircuitBreaker.AllowRequest())
        {
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }

        ReservationServiceResponse? reservation;
        HttpClient reservationService;
        try {
            reservationService = httpClientFactory.CreateClient("ReservationService");

            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/reservations/{reservationUid}");
            request.Headers.Add("X-User-Name", username);
            var reservationServiceResponse = await reservationService.SendAsync(request);
            if (!reservationServiceResponse.IsSuccessStatusCode)
            {
                return StatusCode((int)reservationServiceResponse.StatusCode, new { message = $"Error receiving reservation from microservice" });
            }
            var reservationJsonString = await reservationServiceResponse.Content.ReadAsStringAsync();
            reservation = JsonConvert.DeserializeObject<ReservationServiceResponse>(reservationJsonString);
            if (null == reservation)
            {
                return NotFound($"Reservations with uid {reservationUid} was not found");
            }

            reservation.Status = ReservationStatus.CANCELED.ToString();
            request = new HttpRequestMessage(HttpMethod.Put, $"/api/v1/reservations");
            var json = JsonConvert.SerializeObject(reservation);
            request.Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            reservationServiceResponse = await reservationService.SendAsync(request);
            if (!reservationServiceResponse.IsSuccessStatusCode)
            {
                return StatusCode((int)reservationServiceResponse.StatusCode, new { message = $"Error updating reservation" });
            }
        }
        catch (Exception)
        {
            reservationServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Reservation Service unavailable" });
        }

        if (!PaymentServiceCircuitBreaker.AllowRequest())
        {
            return StatusCode(503, new { message = "Payment Service unavailable" });
        }
        
        HttpClient paymentService;
        try
        {
            paymentService = httpClientFactory.CreateClient("PaymentService");

            var paymentServiceResponse = await paymentService.GetAsync($"/api/v1/payments/{reservation.PaymentUid}");
            var paymentJsonString = await paymentServiceResponse.Content.ReadAsStringAsync();
            var payment = JsonConvert.DeserializeObject<PaymentServiceResponse>(paymentJsonString);
            if (null == payment)
            {
                PaymentServiceCircuitBreaker.RecordFailure();
                return NotFound($"Payment with uid {reservation.PaymentUid} was not found");
            }
            PaymentServiceCircuitBreaker.RecordSuccess();
            payment.Status = PaymentOperationStatus.CANCELED.ToString();
            var request = new HttpRequestMessage(HttpMethod.Put, $"/api/v1/payments");
            var json = JsonConvert.SerializeObject(payment);
            request.Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            paymentServiceResponse = await paymentService.SendAsync(request);
            if (!paymentServiceResponse.IsSuccessStatusCode)
            {
                return StatusCode((int)paymentServiceResponse.StatusCode, new { message = $"Error updating payment" });
            }
        }        
        catch (Exception)
        {
            PaymentServiceCircuitBreaker.RecordFailure();
            return StatusCode(503, new { message = "Payment Service unavailable" });
        }

        if (!loyaltyServiceCircuitBreaker.AllowRequest())
        {
            await EnqueueLoyaltyRequestAsync(username);
            return NoContent();
        }

        try
        {
            var loyaltyClient = httpClientFactory.CreateClient("LoyaltyService");
            var request = new HttpRequestMessage(HttpMethod.Get, $"/api/v1/loyalties/degrade");
            request.Headers.Add("X-User-Name", username);
            var loyaltyResponse = await loyaltyClient.SendAsync(request);

            if (!loyaltyResponse.IsSuccessStatusCode)
            {
                await EnqueueLoyaltyRequestAsync(username);
            }
        }
        catch (Exception)
        {
            await EnqueueLoyaltyRequestAsync(username);
        }

        return NoContent();
    }

    private async Task EnqueueLoyaltyRequestAsync(string username)
    {
        var db = redis.GetDatabase();
        await db.ListRightPushAsync("loyalty-queue", username);
    }
}
