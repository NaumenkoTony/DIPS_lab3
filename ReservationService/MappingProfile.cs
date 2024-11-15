namespace ReservationService.Mapping;

using AutoMapper;
using Microsoft.AspNetCore.SignalR;
using ReservationService.Models.DomainModels;
using ReservationService.Models.Dto;

public class MappingProfile : Profile
{
    public MappingProfile()
    {
        CreateMap<Hotel, HotelResponse>()
            .ForMember(dest => dest.HotelUid, opt => opt.MapFrom(src => src.HotelUid.ToString()));

        CreateMap<HotelResponse, Hotel>()
            .ForMember(dest => dest.HotelUid, opt => opt.MapFrom(src => Guid.Parse(src.HotelUid)));
        
        CreateMap<Reservation, ReservationResponse>()
            .ForMember(dest => dest.ReservationUid, opt => opt.MapFrom(src => src.ReservationUid.ToString()))
            .ForMember(dest => dest.PaymentUid, opt => opt.MapFrom(src => src.PaymentUid.ToString()))
            .ForMember(dest => dest.StartDate, opt => opt.MapFrom(src => src.StartDate.HasValue ? src.StartDate.Value.ToString("yyyy-MM-dd") : null))
            .ForMember(dest => dest.EndDate, opt => opt.MapFrom(src => src.EndData.HasValue ? src.EndData.Value.ToString("yyyy-MM-dd") : null));

        CreateMap<ReservationResponse, Reservation>()
            .ForMember(dest => dest.ReservationUid, opt => opt.MapFrom(src => Guid.Parse(src.ReservationUid)))
            .ForMember(dest => dest.PaymentUid, opt => opt.MapFrom(src => Guid.Parse(src.PaymentUid)))
            .ForMember(dest => dest.HotelId, opt => opt.MapFrom(src => !string.IsNullOrEmpty(src.HotelId) ? int.Parse(src.HotelId) : (int?)null))
            .ForMember(dest => dest.StartDate, opt => opt.MapFrom(src => ParseDate(src.StartDate)))
            .ForMember(dest => dest.EndData, opt => opt.MapFrom(src => ParseDate(src.EndDate)));
            
        CreateMap<ReservationRequest, Reservation>()
            .ForMember(dest => dest.StartDate, opt => opt.MapFrom(src => DateTime.SpecifyKind(DateTime.Parse(src.StartDate), DateTimeKind.Utc)))
            .ForMember(dest => dest.EndData, opt => opt.MapFrom(src => DateTime.SpecifyKind(DateTime.Parse(src.EndData), DateTimeKind.Utc)));
    }

    private static DateTime? ParseDate(string dateString)
    {
        if (DateTime.TryParse(dateString, out DateTime date))
        {
            return DateTime.SpecifyKind(date, DateTimeKind.Utc);
        }
        return null;
    }
}
