import re
import polars as pl

http_log_regex = re.compile(r"(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) (?P<unknown>\S*) (?P<unknown2>\S*) \[(?P<date>\d{1,2}/\S*/\d{4}:\d{2}:\d{2}:\d{2} \+\d*)] \"(?P<method>\S*) (?P<uri>\S*) (?P<verson>\S*)\" (?P<status_code>\d*) (?P<request_size>\d*) \"(?P<host>\S*)\" \"(?P<user_agent>.*)\"")

def open_log(file: str) -> pl.DataFrame:
    data = {
        "ts": [],
        "id.orig_h": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "method": "GET",
        "host": "testmyids.com",
        "uri": "/",
        "version": "1.1",
        "user_agent": "curl/7.47.0",
        "request_body_len": 0,
        "response_body_len": 39,
        "status_code": 200,
        "status_msg": "OK",
        "tags": [],
        "resp_fuids": [
            "FEEsZS1w0Z0VJIb5x4"
        ],
        "resp_mime_types": [
            "text/plain"
        ]
    }
        
    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)

    df = pl.DataFrame(data)

    return df