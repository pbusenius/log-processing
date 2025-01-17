import os
import polars as pl

from zat.log_to_dataframe import LogToDataFrame


def open_files_log(file: str) -> pl.DataFrame:
    df = None
    log_to_df = LogToDataFrame()
    if os.path.exists(file):
        df = log_to_df.create_dataframe(file).reset_index()

        df = pl.from_pandas(df)

        df = df.sort("ts")

        # TODO: apply schema

    return df
