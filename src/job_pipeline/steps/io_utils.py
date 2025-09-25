from __future__ import annotations


from datetime import datetime
import pandas as pd


from job_pipeline.config import Settings
from job_pipeline.logging_conf import get_logger


log = get_logger(__name__)


# creates unique name for dataset that would be used later as main part of naming
def adzuna_save_raw_bronze(df: pd.DataFrame, name: str, settings: Settings) -> str:
    """name - unique identifier"""

    file_name = f"adzuna_{name}_{datetime.now().strftime('%H%M%S')}"

    output = settings.paths.bronze / (file_name + "_raw.parquet")

    df.to_parquet(output)
    log.info(f"{len(df)} jobs are fetched from adzuna and saved in {output}")
    return file_name


def adzuna_read_raw_bronze(file_name: str, settings: Settings) -> pd.DataFrame:

    path = settings.paths.bronze / (file_name + "_raw.parquet")

    df = pd.read_parquet(path)

    log.info(f"Read {len(df)} jobs from {path}")
    return df


def adzuna_save_home_url_silver(
    df: pd.DataFrame, file_name: str, settings: Settings
) -> None:
    """filename - passed from prev"""

    output = settings.paths.silver / (file_name + "_home_url.parquet")

    df.to_parquet(output)

    log.info(f"Saved {len(df)} jobs with home URLs to {output}")
    return None


def adzuna_read_home_url_silver(file_name: str, settings: Settings) -> pd.DataFrame:
    """filename - passed from prev"""

    path = settings.paths.silver / (file_name + "_home_url.parquet")

    df = pd.read_parquet(path)

    log.info(f"Read {len(df)} jobs with full URLs from {path}")
    return df


def adzuna_save_full_description_gold(
    df: pd.DataFrame, file_name: str, settings: Settings
) -> None:
    """filename - passed from prev"""

    output = settings.paths.gold / (file_name + "_full_description.parquet")

    df.to_parquet(output)

    log.info(f"Saved {len(df)} jobs with full descriptions to {output}")
    return None
