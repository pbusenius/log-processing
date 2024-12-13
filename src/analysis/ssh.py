import polars as pl

# ts	uid	id.orig_h	id.orig_p	version	auth_success		host_key_alg	host_key


def brute_force_detection(
    df: pl.DataFrame, timeout: int = 30, limit: int = 30
) -> pl.DataFrame:
    df_brute_force = (
        df.group_by_dynamic("ts", group_by="id.orig_h", every="30m")
        .agg(
            pl.col("uid").count().alias("number_of_attempts"),
            pl.col("auth_success").n_unique(),
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
