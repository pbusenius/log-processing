import polars as pl


def brute_force_detection(
    df: pl.DataFrame, timeout: int = 30, limit: int = 30
) -> pl.DataFrame:
    df_brute_force = (
        df.group_by_dynamic("ts", group_by="id.orig_h", every=f"{timeout}m")
        .agg(
            pl.col("auth_success").count().alias("number_of_attempts"),
            pl.col(
                "id.resp_h",
                "id.resp_p",
                "client",
                "server",
                "cipher_alg",
                "mac_alg",
                "compression_alg",
                "kex_alg",
                "host_key_alg",
                "host_key",
            ).first(),
        )
        .filter(pl.col("number_of_attempts") >= limit)
        .group_by("id.orig_h")
        .agg(
            pl.col("number_of_attempts").sum(),
            pl.col(
                "id.resp_h",
                "id.resp_p",
                "client",
                "server",
                "cipher_alg",
                "mac_alg",
                "compression_alg",
                "kex_alg",
                "host_key_alg",
                "host_key",
            ).first(),
        )
    )

    return df_brute_force
