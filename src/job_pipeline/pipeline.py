from pathlib import Path
import sys


from job_pipeline.logging_conf import get_logger, setup_logging
from job_pipeline.config import LOGS_DIR

# adzuna api import
from job_pipeline.config import ADZUNA_API_PRESETS, ADZUNA_ID, ADZUNA_KEY

# bright data proxy import
from job_pipeline.config import (
    LOGS_DIR,
    BD_HOST,
    BD_PASSWORD,
    BD_PORT,
    BD_USERNAME_BASE,
    BD_COUNTRY,
)

log = get_logger(__name__)

if __name__ == "__main__":
    setup_logging(level="INFO", log_dir=LOGS_DIR)
    log = get_logger("pipeline")
    log.info("Starting pipeline execution...")


# pipeline components
from job_pipeline.steps.ingest.adzuna import AdzunaAPI
from job_pipeline.steps.transform.add_home_url import HomeUrlProcessor
from job_pipeline.steps.transform.add_full_description import (
    FullDescriptionProcessor,
)
from job_pipeline.steps import io_utils

################################################################################


# need to get input from somewhere
def run_pipeline():

    try:

        # STAGE 1: INGEST RAW DATA, SAVE TO BRONZE

        name = "test"
        config = ADZUNA_API_PRESETS["test_page_list"]

        api_client = AdzunaAPI(ADZUNA_ID, ADZUNA_KEY)

        jobs, error_list = api_client.search_jobs_robust(**config)
        log.info(f"Error list: {error_list}")
        jobs = api_client.to_df(jobs)

        file_name = io_utils.adzuna_save_raw_bronze(jobs, name)

        # STAGE 2: ADD HOME URL, SAVE TO SILVER

        url_processor = HomeUrlProcessor(
            BD_HOST=BD_HOST,
            BD_PORT=int(BD_PORT),
            BD_USERNAME_BASE=BD_USERNAME_BASE,
            BD_PASSWORD=BD_PASSWORD,
            BD_COUNTRY=BD_COUNTRY,
        )

        url_jobs = url_processor.add_home_urls_robust(jobs, 100, 0.01, 10, True)
        url_jobs = url_processor.clean_urls(
            url_jobs, specific_domains=["www.adzuna.co.uk", "www.linkedin.com"]
        )

        # save
        io_utils.adzuna_save_home_url_silver(url_jobs, file_name)

        # STAGE 3: ADD FULL DESCRIPTION, SAVE TO GOLD

        description_processor = FullDescriptionProcessor(
            BD_HOST=BD_HOST,
            BD_PORT=int(BD_PORT),
            BD_USERNAME_BASE=BD_USERNAME_BASE,
            BD_PASSWORD=BD_PASSWORD,
            BD_COUNTRY=BD_COUNTRY,
        )

        file_name = "adzuna_test_page_list_191306"

        descriptions = description_processor.add_full_descriptions_robust(
            url_jobs,
            max_workers=100,
            acceptable_fault_rate=0.01,
            max_tries=5,
            initial_process=True,
        )  # Add copy=True here

        io_utils.adzuna_save_full_description_gold(descriptions, file_name)

        log.info("Pipeline completed successfully")

    except Exception as e:
        log.error(f"Error in pipeline: {e}")
        raise e


if __name__ == "__main__":
    run_pipeline()
