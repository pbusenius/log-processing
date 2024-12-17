import polars as pl


def open_log(file: str) -> pl.DataFrame:
    with open(file, "r") as file:
        lines = file.readlines()
