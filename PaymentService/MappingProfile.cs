namespace PaymentService.Mapping;

using AutoMapper;
using PaymentService.Models.DomainModels;
using PaymentService.Models.Dto;

public class MappingProfile : Profile
{
    public MappingProfile()
    {
        CreateMap<Payment, PaymentResponse>()
            .ForMember(dest => dest.PaymentUid, opt => opt.MapFrom(src => src.PaymentUid.ToString()));

        CreateMap<PaymentResponse, Payment>()
            .ForMember(dest => dest.PaymentUid, opt => opt.MapFrom(src => Guid.Parse(src.PaymentUid)));

        CreateMap<Payment, PaymentRequest>().ReverseMap();
    }
}
