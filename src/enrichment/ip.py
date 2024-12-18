import polars as pl
from polars_maxminddb import ip_lookup_city, ip_lookup_country, ip_lookup_asn, ip_lookup_latitude, ip_lookup_longitude


def city_information(df: pl.DataFrame, ip_column: str = "id.resp_h") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_city(df[ip_column], "data/GeoLite2-City.mmdb").alias(
            "city_information"
        )
    )

def location_information(df: pl.DataFrame, ip_column: str = "id.resp_h") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_longitude(df[ip_column], "data/GeoLite2-City.mmdb").alias("longitude"),
        ip_lookup_latitude(df[ip_column], "data/GeoLite2-City.mmdb").alias("latitude")
    )

def country_information(df: pl.DataFrame, ip_column: str = "id.resp_h") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_country(df[ip_column], "data/GeoLite2-Country.mmdb").alias(
            "country_information"
        )
    )


def asn_information(df: pl.DataFrame, ip_column: str = "id.resp_h") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_asn(df[ip_column], "data/GeoLite2-ASN.mmdb").alias("asn_information")
    )
