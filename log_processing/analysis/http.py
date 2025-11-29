import polars as pl
from polars_domain_lookup import is_common_domain


def get_uncommon_domains(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        is_common_domain(
            df["domain"], "data/cloudflare-radar_top-1000000-domains.csv"
        ).alias("is_top_domain")
    )

    df = df.filter(~pl.col("is_top_domain"))

    return df


def get_common_domains(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        is_common_domain(
            df["domain"], "data/cloudflare-radar_top-1000000-domains.csv"
        ).alias("is_top_domain")
    )

    df = df.filter(pl.col("is_top_domain"))

    df.plot.point(x="length", y="width", color="species")

    return df


def get_periodic_connection_to_host(df: pl.DataFrame) -> pl.DataFrame:
    min_number_of_connections = 10
    df = df.with_columns(pl.col("ts").diff().dt.total_milliseconds().alias("time_diff"))

    print(df)

    df = (
        df.group_by("id.orig_h")
        .agg(
            pl.col("time_diff").mean().alias("time_diff_mean"),
            pl.col("time_diff").std().alias("time_diff_std"),
            pl.col("time_diff").median().alias("time_diff_median"),
            pl.col("time_diff").count().alias("number_of_connections"),
            (pl.col("time_diff").quantile(0.25).sub(pl.col("time_diff").quantile(0.75)))
            .abs()
            .alias("iqr"),
        )
        .filter(pl.col("number_of_connections") >= min_number_of_connections)
    )

    return df


def get_periodic_connection_from_host(df: pl.DataFrame) -> pl.DataFrame:
    min_number_of_connections = 10
    df = df.with_columns(pl.col("ts").diff().dt.total_milliseconds().alias("time_diff"))

    df = (
        df.group_by("id.resp_h")
        .agg(
            pl.col("time_diff").mean().alias("time_diff_mean"),
            pl.col("time_diff").count().alias("number_of_connections"),
            (pl.col("time_diff").quantile(0.25).sub(pl.col("time_diff").quantile(0.75)))
            .abs()
            .alias("iqr"),
        )
        .filter(pl.col("number_of_connections") >= min_number_of_connections)
    )

    return df
