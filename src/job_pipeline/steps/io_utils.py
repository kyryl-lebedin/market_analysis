from __future__ import annotations

from datetime import datetime
import pandas as pd
from pathlib import Path
from typing import Union
import io

from job_pipeline.config import Settings
from job_pipeline.logging_conf import get_logger

log = get_logger(__name__)


def _save_dataframe(
    df: pd.DataFrame, file_path: Union[Path, str], settings: Settings
) -> None:
    """Save DataFrame to either local file or S3"""
    if settings.USE_S3:
        # Save to S3
        s3_client = settings.s3_client
        s3_config = settings.s3_config

        # Convert DataFrame to parquet bytes
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload to S3
        s3_client.put_object(
            Bucket=s3_config.bucket_name,
            Key=str(file_path),
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
    else:
        # Save locally
        df.to_parquet(file_path)


def _read_dataframe(file_path: Union[Path, str], settings: Settings) -> pd.DataFrame:
    """Read DataFrame from either local file or S3"""
    if settings.USE_S3:
        # Read from S3
        s3_client = settings.s3_client
        s3_config = settings.s3_config

        # Get object from S3
        response = s3_client.get_object(
            Bucket=s3_config.bucket_name, Key=str(file_path)
        )

        # Read parquet from bytes
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
    else:
        # Read locally
        return pd.read_parquet(file_path)


# creates unique name for dataset that would be used later as main part of naming
def adzuna_save_raw_bronze(df: pd.DataFrame, name: str, settings: Settings) -> str:
    """name - unique identifier"""

    file_name = f"adzuna_{name}_{datetime.now().strftime('%H%M%S')}"

    if settings.USE_S3:
        s3_key = f"bronze/{file_name}_raw.parquet"
        _save_dataframe(df, s3_key, settings)
        log.info(
            f"{len(df)} jobs are fetched from adzuna and saved to S3: s3://{settings.s3_config.bucket_name}/{s3_key}"
        )
    else:
        output = settings.paths.bronze / (file_name + "_raw.parquet")
        _save_dataframe(df, output, settings)
        log.info(f"{len(df)} jobs are fetched from adzuna and saved in {output}")

    return file_name


def adzuna_read_raw_bronze(file_name: str, settings: Settings) -> pd.DataFrame:

    if settings.USE_S3:
        s3_key = f"bronze/{file_name}_raw.parquet"
        df = _read_dataframe(s3_key, settings)
        log.info(
            f"Read {len(df)} jobs from S3: s3://{settings.s3_config.bucket_name}/{s3_key}"
        )
    else:
        path = settings.paths.bronze / (file_name + "_raw.parquet")
        df = _read_dataframe(path, settings)
        log.info(f"Read {len(df)} jobs from {path}")

    return df


def adzuna_save_home_url_silver(
    df: pd.DataFrame, file_name: str, settings: Settings
) -> None:
    """filename - passed from prev"""

    if settings.USE_S3:
        s3_key = f"silver/{file_name}_home_url.parquet"
        _save_dataframe(df, s3_key, settings)
        log.info(
            f"Saved {len(df)} jobs with home URLs to S3: s3://{settings.s3_config.bucket_name}/{s3_key}"
        )
    else:
        output = settings.paths.silver / (file_name + "_home_url.parquet")
        _save_dataframe(df, output, settings)
        log.info(f"Saved {len(df)} jobs with home URLs to {output}")

    return None


def adzuna_read_home_url_silver(file_name: str, settings: Settings) -> pd.DataFrame:
    """filename - passed from prev"""

    if settings.USE_S3:
        s3_key = f"silver/{file_name}_home_url.parquet"
        df = _read_dataframe(s3_key, settings)
        log.info(
            f"Read {len(df)} jobs with full URLs from S3: s3://{settings.s3_config.bucket_name}/{s3_key}"
        )
    else:
        path = settings.paths.silver / (file_name + "_home_url.parquet")
        df = _read_dataframe(path, settings)
        log.info(f"Read {len(df)} jobs with full URLs from {path}")

    return df


def adzuna_save_full_description_gold(
    df: pd.DataFrame, file_name: str, settings: Settings
) -> None:
    """filename - passed from prev"""

    if settings.USE_S3:
        s3_key = f"gold/{file_name}_full_description.parquet"
        _save_dataframe(df, s3_key, settings)
        log.info(
            f"Saved {len(df)} jobs with full descriptions to S3: s3://{settings.s3_config.bucket_name}/{s3_key}"
        )
    else:
        output = settings.paths.gold / (file_name + "_full_description.parquet")
        _save_dataframe(df, output, settings)
        log.info(f"Saved {len(df)} jobs with full descriptions to {output}")

    return None
