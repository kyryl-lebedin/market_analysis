import logging
from pathlib import Path
import datetime
from job_pipeline.steps.ingest.adzuna import DATA_DIR
import pandas as pd
import sys


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.logging_conf import setup_logging, get_logger

log = get_logger(__name__)


DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)


def adzuna_save_raw_bronze(df: pd.DataFrame, what_and: str) -> str:

    output = (
        BRONZE_DIR
        / f"adzuna_{what_and}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_raw.parquet"
    )

    df.to_parquet(output)

    log.info(f"{len(df)} jobs are fetched from adzuna and saved in {output}")

    return output
