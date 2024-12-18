import polars as pl


def as_json(df: pl.DataFrame, file: str):
    # add message
    # add timestamp
    # add timestamp_desc
    data = df.write_json().strip("[]")
    with open(file, "w") as output:
        output.write(data.replace("},{", "}\n{"))


def as_csv(df: pl.DataFrame, file: str):
    # add message
    # add timestamp
    # add timestamp_desc
    data = df.write_csv(file)
