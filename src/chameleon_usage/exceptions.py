import logging

import polars as pl


class RawTableLoadError(Exception):
    pass


class RawTableMissingError(RawTableLoadError):
    pass


class RawTableAuthError(RawTableLoadError):
    pass


class RawTableRemoteError(RawTableLoadError):
    pass


def classify_raw_table_load_error(table_path: str, exc: Exception) -> RawTableLoadError:
    message = str(exc).lower()

    if isinstance(exc, FileNotFoundError):
        return RawTableMissingError(f"Missing parquet: {table_path}")

    if isinstance(exc, (OSError, pl.exceptions.ComputeError)):
        if any(
            marker in message
            for marker in (
                "not found",
                "no such file",
                "nosuchkey",
                "nosuchbucket",
                "404",
            )
        ):
            return RawTableMissingError(f"Missing parquet: {table_path}")
        if any(
            marker in message
            for marker in (
                "accessdenied",
                "invalidaccesskeyid",
                "signaturedoesnotmatch",
                "latest/api/token",
            )
        ):
            return RawTableAuthError(f"Object-store auth error for {table_path}: {exc}")
        return RawTableRemoteError(f"Object-store error for {table_path}: {exc}")

    return RawTableLoadError(f"Failed loading {table_path}: {exc}")


def log_raw_table_load_error(
    logger: logging.Logger, site_key: str, exc: RawTableLoadError
) -> None:
    if isinstance(exc, RawTableMissingError):
        logger.warning("[%s] missing required parquet/object: %s", site_key, exc)
        return
    if isinstance(exc, RawTableAuthError):
        logger.error("[%s] object-store auth/credentials error: %s", site_key, exc)
        return
    if isinstance(exc, RawTableRemoteError):
        logger.error("[%s] object-store/network error: %s", site_key, exc)
        return
    logger.error("[%s] raw table load error: %s", site_key, exc)
