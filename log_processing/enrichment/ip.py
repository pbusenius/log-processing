import polars as pl
from polars_maxminddb import (
    ip_lookup_city,
    ip_lookup_country,
    ip_lookup_asn,
    ip_lookup_latitude,
    ip_lookup_longitude,
)


def _is_public_ip(ip: str) -> bool:
    """Check if IP is public (not private/localhost)."""
    if not ip:
        return False
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    try:
        first = int(parts[0])
        second = int(parts[1])
        # Private IP ranges: 10.x.x.x, 172.16-31.x.x, 192.168.x.x, 127.x.x.x
        if first == 10:
            return False
        if first == 172 and 16 <= second <= 31:
            return False
        if first == 192 and second == 168:
            return False
        if first == 127:
            return False
        return True
    except ValueError:
        return False


def city_information(df: pl.DataFrame, ip_column: str = "id.orig_h") -> pl.DataFrame:
    # Filter to only public IPs to avoid plugin panics on private IPs
    public_mask = pl.col(ip_column).map_elements(_is_public_ip, return_dtype=pl.Boolean)
    df_public = df.filter(public_mask)
    
    if df_public.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.String).alias("city_information"))
    
    # Only lookup public IPs
    df_public_enriched = df_public.with_columns(
        ip_lookup_city(df_public[ip_column], "data/GeoLite2-City.mmdb").alias(
            "city_information"
        )
    )
    
    # Add null city_information for private IPs
    df_private = df.filter(~public_mask).with_columns(
        pl.lit(None).cast(pl.String).alias("city_information")
    )
    
    # Combine back
    return pl.concat([df_public_enriched, df_private])


def location_information(
    df: pl.DataFrame, ip_column: str = "id.orig_h"
) -> pl.DataFrame:
    # Filter to only public IPs to avoid plugin panics on private IPs
    public_mask = pl.col(ip_column).map_elements(_is_public_ip, return_dtype=pl.Boolean)
    df_public = df.filter(public_mask)
    
    if df_public.is_empty():
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("longitude"),
            pl.lit(None).cast(pl.Float64).alias("latitude"),
        ])
    
    df_public_enriched = df_public.with_columns([
        ip_lookup_longitude(df_public[ip_column], "data/GeoLite2-City.mmdb").alias("longitude"),
        ip_lookup_latitude(df_public[ip_column], "data/GeoLite2-City.mmdb").alias("latitude"),
    ])
    
    df_private = df.filter(~public_mask).with_columns([
        pl.lit(None).cast(pl.Float64).alias("longitude"),
        pl.lit(None).cast(pl.Float64).alias("latitude"),
    ])
    
    return pl.concat([df_public_enriched, df_private])


def country_information(df: pl.DataFrame, ip_column: str = "id.orig_h") -> pl.DataFrame:
    # Filter to only public IPs to avoid plugin panics on private IPs
    public_mask = pl.col(ip_column).map_elements(_is_public_ip, return_dtype=pl.Boolean)
    df_public = df.filter(public_mask)
    
    if df_public.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.String).alias("country_information"))
    
    df_public_enriched = df_public.with_columns(
        ip_lookup_country(df_public[ip_column], "data/GeoLite2-Country.mmdb").alias(
            "country_information"
        )
    )
    
    df_private = df.filter(~public_mask).with_columns(
        pl.lit(None).cast(pl.String).alias("country_information")
    )
    
    return pl.concat([df_public_enriched, df_private])


def asn_information(df: pl.DataFrame, ip_column: str = "id.orig_h") -> pl.DataFrame:
    # Filter to only public IPs to avoid plugin panics on private IPs
    public_mask = pl.col(ip_column).map_elements(_is_public_ip, return_dtype=pl.Boolean)
    df_public = df.filter(public_mask)
    
    if df_public.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.String).alias("asn_information"))
    
    df_public_enriched = df_public.with_columns(
        ip_lookup_asn(df_public[ip_column], "data/GeoLite2-ASN.mmdb").alias("asn_information")
    )
    
    df_private = df.filter(~public_mask).with_columns(
        pl.lit(None).cast(pl.String).alias("asn_information")
    )
    
    return pl.concat([df_public_enriched, df_private])
