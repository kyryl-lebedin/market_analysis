import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import sys


from job_pipeline.config import GOLD_DIR, BRONZE_DIR, SILVER_DIR, LOGS_DIR


from job_pipeline.logging_conf import get_logger

# Use the same logger name as the main app - no setup_logging needed here
log = get_logger("pipeline")  # testing only, will dance with it later


# creates unique name for dataset that would be used later as main part of naming
def adzuna_save_raw_bronze(df: pd.DataFrame, name: str) -> str:
    """name - unique identifier"""

    file_name = f"adzuna_{name}_{datetime.now().strftime('%H%M%S')}"

    output = BRONZE_DIR / (file_name + "_raw.parquet")

    df.to_parquet(output)
    log.info(f"{len(df)} jobs are fetched from adzuna and saved in {output}")
    return file_name


def adzuna_read_raw_bronze(file_name: str) -> pd.DataFrame:

    path = BRONZE_DIR / (file_name + "_raw.parquet")

    df = pd.read_parquet(path)

    log.info(f"Read {len(df)} jobs from {path}")
    return df


def adzuna_save_home_url_silver(df: pd.DataFrame, file_name: str) -> None:
    """filename - passed from prev"""

    output = SILVER_DIR / (file_name + "_home_url.parquet")

    df.to_parquet(output)

    log.info(f"Saved {len(df)} jobs with home URLs to {output}")
    return None


def adzuna_read_home_url_silver(file_name: str) -> pd.DataFrame:
    """filename - passed from prev"""

    path = SILVER_DIR / (file_name + "_home_url.parquet")

    df = pd.read_parquet(path)

    log.info(f"Read {len(df)} jobs with full URLs from {path}")
    return df


def adzuna_save_full_description_gold(df: pd.DataFrame, file_name: str) -> None:
    """filename - passed from prev"""

    output = GOLD_DIR / (file_name + "_full_description.parquet")

    df.to_parquet(output)

    log.info(f"Saved {len(df)} jobs with full descriptions to {output}")
    return None
