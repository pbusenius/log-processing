import random
import polars as pl
import fast_geo_distance
from src.enrichment import ip


def detect_impossible_travel(df: pl.DataFrame, speed_threshold: int) -> pl.DataFrame:
    df = ip.location_information(df)

    # process every ip
    df = df.group_by("id.orig_h").map_groups(
        lambda ip_group: process_ip_group(ip_group)
    )

    # distance threshold
    df = df.filter(pl.col("distance") >= speed_threshold)

    # extract
    df = df.group_by("id.orig_h").agg(
        pl.col("ts").first(),
        pl.col("distance").mean().alias("mean_distance"),
        pl.col("distance").max().alias("max_distance"),
    )

    return df


def process_ip_group(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("latitude").shift().alias("prev_latitude"),
        pl.col("longitude").shift().alias("prev_longitude"),
        pl.col("ts").shift().alias("prev_ts"),
    ).with_columns(
        # TODO: build custom rust plugin
        pl.struct(["latitude", "longitude", "prev_longitude", "prev_latitude"])
        .map_elements(
            lambda x: calculate_distance(
                x["latitude"], x["longitude"], x["prev_latitude"], x["prev_longitude"]
            ),
            return_dtype=pl.Float64,
        )
        .alias("distance")
    )

    return df


def calculate_distance(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    distance = 0.0
    if lat_b is not None and lon_b is not None:
        distance = fast_geo_distance.geodesic(lat_a, lon_a, lat_b, lon_b)
        # ONLY FOR TESTS!!!
        distance = float(random.randint(0, 100))

    return distance
