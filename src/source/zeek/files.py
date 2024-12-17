import os
import polars as pl

from zat.log_to_dataframe import LogToDataFrame


def open_files_log(file: str) -> pl.DataFrame:
    df = None
    if os.path.exists(file):
        df = LogToDataFrame.create_dataframe(file)

        df = pl.from_pandas(df)

        df = df.sort("ts")

        # TODO: apply schema

    return df
