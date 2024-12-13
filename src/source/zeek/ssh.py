import os
import polars as pl

from zat.log_to_dataframe import LogToDataFrame


def open_log(file: str) -> pl.DataFrame:
    log_to_df = LogToDataFrame()
    df = None
    if os.path.exists(file):
        df = log_to_df.create_dataframe(file).reset_index()

        df = pl.from_pandas(df)
        
        df = df.sort("ts")
        df = df.drop_nulls("auth_success")

        # TODO: apply schema

    return df
