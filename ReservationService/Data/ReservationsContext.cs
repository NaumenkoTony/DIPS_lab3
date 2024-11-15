using Microsoft.EntityFrameworkCore;
using ReservationService.Models.DomainModels;

namespace ReservationService.Data;

public partial class ReservationsContext : DbContext
{
    public ReservationsContext()
    {
    }

    public ReservationsContext(DbContextOptions<ReservationsContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Hotel> Hotels { get; set; }

    public virtual DbSet<Reservation> Reservations { get; set; }
    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Hotel>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("hotels_pkey");

            entity.ToTable("hotels");

            entity.HasIndex(e => e.HotelUid, "hotels_hotel_uid_key").IsUnique();

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Address)
                .HasMaxLength(255)
                .HasColumnName("address");
            entity.Property(e => e.City)
                .HasMaxLength(80)
                .HasColumnName("city");
            entity.Property(e => e.Country)
                .HasMaxLength(80)
                .HasColumnName("country");
            entity.Property(e => e.HotelUid).HasColumnName("hotel_uid");
            entity.Property(e => e.Name)
                .HasMaxLength(255)
                .HasColumnName("name");
            entity.Property(e => e.Price).HasColumnName("price");
            entity.Property(e => e.Stars).HasColumnName("stars");
        });

        modelBuilder.Entity<Reservation>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("reservation_pkey");

            entity.ToTable("reservation");

            entity.HasIndex(e => e.ReservationUid, "reservation_reservation_uid_key").IsUnique();

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.EndData).HasColumnName("end_data");
            entity.Property(e => e.HotelId).HasColumnName("hotel_id");
            entity.Property(e => e.PaymentUid).HasColumnName("payment_uid");
            entity.Property(e => e.ReservationUid).HasColumnName("reservation_uid");
            entity.Property(e => e.StartDate).HasColumnName("start_date");
            entity.Property(e => e.Status)
                .HasMaxLength(20)
                .HasColumnName("status");
            entity.Property(e => e.Username)
                .HasMaxLength(80)
                .HasColumnName("username");

            entity.HasOne(d => d.Hotel).WithMany(p => p.Reservations)
                .HasForeignKey(d => d.HotelId)
                .HasConstraintName("reservation_hotel_id_fkey");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
