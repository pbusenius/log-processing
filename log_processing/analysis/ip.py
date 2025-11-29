import polars as pl


def ip_locaton_distance(
    df: pl.DataFrame, key: str, threshold: int = 10000
) -> pl.DataFrame:
    # oder by key
    # shift ordered dfs
    # calculate distance form row-row+1
    # apply threshold
    # return alarming ip location differences
    return df
