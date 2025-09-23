from pathlib import Path
import sys


######################## LOGGING ana IMPORTING #################################

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LOGS_DIR = PROJECT_ROOT / "logs"

from src.logging_conf import setup_logging, get_logger

log = get_logger(__name__)

if __name__ == "__main__":
    setup_logging(level="INFO", log_dir=LOGS_DIR)
    log = get_logger("pipeline")
    log.info("Starting pipeline execution...")


# pipeline components
from src.job_pipeline.steps.ingest.adzuna import AdzunaAPI
from src.job_pipeline.steps.transform.add_home_url import HomeUrlProcessor
from src.job_pipeline.steps.transform.add_full_description import (
    FullDescriptionProcessor,
)
from src.job_pipeline.steps import io_utils

################################################################################


# need to get input from somewhere
def run_pipeline():

    try:
        api_client = AdzunaAPI()  # needs inputs

        jobs = api_client.search_jobs_robust()  # needs inputs
        jobs = api_client.to_df(jobs)

        io_utils.adzuna_save_raw_bronze(jobs, what_and)

    except Exception as e:
        log.error(f"Error in pipeline: {e}")
        raise e
