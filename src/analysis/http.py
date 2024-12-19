import polars as pl
from polars_domain_lookup import is_common_domain


def get_uncommon_domains(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        is_common_domain(df["domain"], "data/cloudflare-radar_top-1000000-domains.csv").alias("is_top_domain")
    )

    df = df.filter(~pl.col("is_top_domain"))

    return df


def get_common_domains(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        is_common_domain(df["domain"], "data/cloudflare-radar_top-1000000-domains.csv").alias("is_top_domain")
    )

    df = df.filter(pl.col("is_top_domain"))

    return df