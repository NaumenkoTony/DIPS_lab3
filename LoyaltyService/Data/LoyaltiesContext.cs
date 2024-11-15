using System;
using System.Collections.Generic;
using LoyaltyService.Models.DomainModels;
using Microsoft.EntityFrameworkCore;

namespace LoyaltyService.Data;

public partial class LoyaltiesContext : DbContext
{
    public LoyaltiesContext()
    {
    }

    public LoyaltiesContext(DbContextOptions<LoyaltiesContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Loyalty> Loyalties { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Loyalty>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("loyalty_pkey");

            entity.ToTable("loyalty");

            entity.HasIndex(e => e.Username, "loyalty_username_key").IsUnique();

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Discount).HasColumnName("discount");
            entity.Property(e => e.ReservationCount)
                .HasDefaultValue(0)
                .HasColumnName("reservation_count");
            entity.Property(e => e.Status)
                .HasMaxLength(80)
                .HasDefaultValueSql("'BRONZE'::character varying")
                .HasColumnName("status");
            entity.Property(e => e.Username)
                .HasMaxLength(80)
                .HasColumnName("username");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
