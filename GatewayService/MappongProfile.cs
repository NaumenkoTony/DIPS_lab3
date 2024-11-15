using AutoMapper;
using GatewayService.Models.Dto;
using GatewayService.Models.LoyaltyServiceDto;
using GatewayService.Models.PaymentServiceDto;
using GatewayService.Models.ReservationServiceDto;


public class MappingProfile : Profile
{
    public MappingProfile()
    {
        CreateMap<HotelServiceResponse, HotelResponse>();
        
        CreateMap<HotelServiceResponse, HotelInfo>()
            .ForMember(dest => dest.FullAddress, opt => opt.MapFrom(src => $"{src.Country}, {src.City}, {src.Address}"));

        CreateMap<LoyaltyServiceResponse, LoyaltyInfoResponse>().ReverseMap();

        CreateMap<PaymentServiceResponse, PaymentInfo>().ReverseMap();

        CreateMap<LoyaltyServiceResponse, LoyaltyInfoResponse>().ReverseMap();
    }
}
