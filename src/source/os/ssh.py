import re
import polars as pl

auth_log_regex = re.compile(
    # "(?P<date>\\S*\\s*\\d*\\s\\d{2}:\\d{2}:\\d{2})\\s(?P<user>\\S*)\\s(?P<service>\\S*\\s)(Accepted (?P<type>password|publickey) for )(?P<remote_user>\\S*) from (?P<ip>\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}) port (?P<port>\\d*) ssh2(: RSA SHA256:(?P<rsa>\\S*))?"
    "(?P<date>\\S*\\s*\\d*\\s\\d{2}:\\d{2}:\\d{2})\\s(?P<user>\\S*)\\s(?P<service>\\S*\\s)(?P<type>Disconnected from invalid user (?P<user_name>\\S* )|Invalid user (?P<user_name_2>\\S*) from |Received disconnect from |Accepted publickey for (?P<user_name_3>\\S*) from )(?P<ip>\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}) port (?P<port>\\d*)( ssh2(: RSA (?P<rsa>SHA256:\\S*))?)?"
)


def open_log(file: str) -> pl.DataFrame:
    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)
            if x is not None:
                if "Accepted" in x["type"]:
                    print(
                        f"{x["date"]},{x["user"]},T,{x["service"].split(":")[0]},{x["ip"]},{x["port"]},{x["rsa"]}"
                    )
                else:
                    print(
                        f"{x["date"]},{x["user"]},F,{x["service"].split(":")[0]},{x["ip"]},{x["port"]}"
                    )
