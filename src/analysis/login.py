import polars as pl

from src.enrichment import ip
from polars_geodesic_distance import distance


def detect_impossible_travel(
    df: pl.DataFrame, speed_threshold: int, by: str = "id.resp_h"
) -> pl.DataFrame:
    df = ip.location_information(df)

    # process every ip
    df = df.group_by(by).map_groups(lambda ip_group: process_ip_group(ip_group))

    # distance threshold
    df = df.filter(pl.col("distance") >= speed_threshold)

    # extract
    df = df.group_by(by).agg(
        pl.col("ts").first(),
        pl.col("distance").mean().alias("mean_distance"),
        pl.col("distance").max().alias("max_distance"),
    )

    return df


def process_ip_group(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("latitude").shift().alias("prev_latitude"),
            pl.col("longitude").shift().alias("prev_longitude"),
            pl.col("ts").shift().alias("prev_ts"),
        )
        .drop_nans(["prev_latitude", "prev_longitude"])
        .with_columns(
            distance("latitude", "longitude", "prev_latitude", "prev_longitude").alias(
                "distance"
            )
        )
        # TODO: calculate theoretical speed for the distance ts -> prev_ts
    )

    return df
