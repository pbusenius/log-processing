import re
import polars as pl

http_log_regex = re.compile(
    r"(?P<ip>(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})|(\S*)) (?P<unknown>\S*) (?P<unknown2>\S*) \[(?P<date>\d{1,2}/\S*/\d{4}:\d{2}:\d{2}:\d{2} \+\d*)] \"(?P<method>\S*) (?P<uri>\S*) (?P<verson>\S*)\" (?P<status_code>\d*) (?P<request_size>\S*) \"?(?P<host>\S*)\"? \"(?P<user_agent>.*)\"?"
)


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%d/%b/%Y:%H:%M:%S %z"),
    )


def open_log(file: str) -> pl.DataFrame:
    data = {
        "ts": [],
        "id.orig_h": [],
    }

    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = http_log_regex.match(line)
            if x is None:
                print(line)
            else:   
                data["id.orig_h"].append(x["ip"])
                data["ts"].append(x["date"])

    df = pl.DataFrame(data)
    df = cast_columns(df)

    return df
