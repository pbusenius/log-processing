import re
import polars as pl

auth_log_regex = re.compile(
    "(?P<date>\S*\s*\d*\s\d{2}:\d{2}:\d{2})\s(?P<user>\S*)\s(?P<service>\S*\s)(Accepted (?P<type>password|publickey) for )(?P<remote_user>\S*) from (?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) port (?P<port>\d*) ssh2(: RSA SHA256:(?P<rsa>\S*))?"
)


def open_log(file: str) -> pl.DataFrame:
    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)
            if x is not None:
                if x["type"] == "password":
                    print(
                        f"{x["date"]},{x["user"]},{x["type"]},{x["remote_user"]},{x["service"].split(":")[0]},{x["ip"]},{x["port"]}"
                    )
                else:
                    print(
                        f"{x["date"]},{x["user"]},{x["type"]},{x["remote_user"]},{x["service"].split(":")[0]},{x["ip"]},{x["port"]},{x["rsa"]}"
                    )
