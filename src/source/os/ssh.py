import re
import polars as pl

auth_log_regex = re.compile(
    "(?P<date>\\S*\\s*\\d*\\s\\d{2}:\\d{2}:\\d{2})\\s(?P<user>\\S*)\\s(?P<service>\\S*\\s)(?P<type>Disconnected from invalid user (?P<disconnected_remote_user>\\S* )|Invalid user (?P<invalid_remote_user>\\S*) from |Received disconnect from |Accepted publickey for (?P<accedpted_public_key_remote_user>\\S*) from )(?P<ip>\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}) port (?P<port>\\d*)( ssh2(: RSA (?P<rsa>SHA256:\\S*))?)?"
)


def open_log(file: str) -> pl.DataFrame:
    data = {
        "ts": [],
        "id.orig_h": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "auth_success": [],
        "client": [],
        "server": [],
        "cipher_alg": [],
        "mac_alg": [],
        "compression_alg": [],
        "kex_alg": [],
        "host_key_alg": [],
        "host_key": [],
    }

    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)
            if x is not None:
                if "Accepted" in x["type"]:
                    data["auth_success"].append("T")
                else:
                    data["auth_success"].append("F")

                data["id.resp_h"].append(x["ip"])
                data["id.orig_h"].append(x["user"])
                data["ts"].append("2024 " + x["date"])
                data["id.resp_p"].append("XXXX")
                data["client"].append("XXXX")
                data["server"].append("XXXX")
                data["cipher_alg"].append("XXXX")
                data["mac_alg"].append("XXXX")
                data["compression_alg"].append("XXXX")
                data["kex_alg"].append("XXXX")
                data["host_key_alg"].append("XXXX")
                data["host_key"].append("XXXX")

    df = pl.DataFrame(data)

    df = df.with_columns(
        pl.col("ts").str.to_datetime("%Y %b  %d %H:%M:%S"),
        pl.col("id.orig_h").cast(pl.Categorical),
        pl.col("id.resp_h").cast(pl.Categorical),
        pl.col("id.resp_p").cast(pl.Categorical),
        pl.col("auth_success").cast(pl.Categorical),
        pl.col("client").cast(pl.Categorical),
        pl.col("server").cast(pl.Categorical),
        pl.col("cipher_alg").cast(pl.Categorical),
        pl.col("mac_alg").cast(pl.Categorical),
        pl.col("compression_alg").cast(pl.Categorical),
        pl.col("kex_alg").cast(pl.Categorical),
        pl.col("host_key_alg").cast(pl.Categorical),
        pl.col("host_key").cast(pl.Categorical)
    )

    return df
