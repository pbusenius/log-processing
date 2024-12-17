import os
import polars as pl

from zat.log_to_dataframe import LogToDataFrame


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("id.orig_h").cast(pl.String),
        pl.col("id.resp_h").cast(pl.String),
        pl.col("id.resp_p").cast(pl.String),
        pl.col("auth_success").cast(pl.Categorical),
        pl.col("client").cast(pl.Categorical),
        pl.col("server").cast(pl.Categorical),
        pl.col("cipher_alg").cast(pl.Categorical),
        pl.col("mac_alg").cast(pl.Categorical),
        pl.col("compression_alg").cast(pl.Categorical),
        pl.col("kex_alg").cast(pl.Categorical),
        pl.col("host_key_alg").cast(pl.Categorical),
        pl.col("host_key").cast(pl.Categorical),
    )


def open_log(file: str) -> pl.DataFrame:
    log_to_df = LogToDataFrame()
    df = None
    if os.path.exists(file):
        df = log_to_df.create_dataframe(file).reset_index()

        df = pl.from_pandas(df)

        df = df.sort("ts")
        df = df.drop_nulls("auth_success")

        df = cast_columns(df)

        # TODO: apply schema

    return df
