namespace LoyaltyService.Mapping;

using AutoMapper;
using LoyaltyService.Models.DomainModels;
using LoyaltyService.Models.Dto;
public class MappingProfile : Profile
{
    public MappingProfile()
     {
        CreateMap<Loyalty, LoyaltyResponse>().ReverseMap();
    }
}
