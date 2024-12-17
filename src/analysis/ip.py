import polars as pl
import fast_geo_distance


def calculate_distance(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    return fast_geo_distance.geodesic(lat_a, lon_a, lat_b, lon_b)


def ip_locaton_distance(
    df: pl.DataFrame, key: str, threshold: int = 10000
) -> pl.DataFrame:
    # oder by key
    # shift ordered dfs
    # calculate distance form row-row+1
    # apply threshold
    # return alarming ip location differences
    return df
