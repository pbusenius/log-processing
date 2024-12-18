import polars as pl


def as_json(df: pl.DataFrame, file: str):
    # add message
    # add timestamp
    # add timestamp_desc
    data = df.write_json().strip("[]").split("},")
    with open(file, "w") as output:
        for line in data:
            if not "}" in line:
                output.write(f"{line}}}\n")
            else:
                output.write(f"{line}\n")


def as_csv(df: pl.DataFrame, file: str):
    # add message
    # add timestamp
    # add timestamp_desc
    pass

