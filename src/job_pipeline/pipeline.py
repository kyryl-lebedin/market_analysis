from __future__ import annotations

from typing import Tuple, Mapping, Any

from job_pipeline.config import Settings
from job_pipeline.logging_conf import get_logger


# adzuna api import


# pipeline components
from job_pipeline.steps.ingest.adzuna import AdzunaAPI
from job_pipeline.steps.transform.add_home_url import HomeUrlProcessor
from job_pipeline.steps.transform.add_full_description import (
    FullDescriptionProcessor,
)
from job_pipeline.steps import io_utils

################################################################################

log = get_logger(__name__)


# need to get input from somewhere
def run_pipeline(
    settings: Settings,
    preset_name: str | None = "test_page_list",
    run_name: str = "test1",
    search_kwargs: Mapping[str, Any] | None = None,
):
    # Examples:
    # job_pipeline --preset data_scientist_gb --name ds_search
    # job_pipeline --kwargs '{"country":"gb","what_and":"python","scope":"single_page","page":1}' --name python_jobs

    log.info(
        f"Starting pipeline… run_name={run_name} preset={preset_name} uses_raw_kwargs={bool(search_kwargs)}"
    )

    #######################

    if search_kwargs:
        adzuna_kwargs = dict(search_kwargs)
    else:
        # if no explicit arguments, run with preset
        presets = settings.adzuna_presets
        if not preset_name or preset_name not in presets:
            raise KeyError(
                f"Unknown preset '{preset_name}'. Available: {', '.join(presets.keys())}"
            )
        preset = presets[preset_name]
        adzuna_kwargs = preset.model_dump(by_alias=True, exclude_none=True)

    for k in ("what_or", "what_and"):
        if isinstance(adzuna_kwargs.get(k), list):
            adzuna_kwargs[k] = ",".join(adzuna_kwargs[k])
            log.info(f"Mistakenly provided {k} as list, converted to string")

    ######################################

    # STAGE 1: INGEST RAW DATA, SAVE TO BRONZE

    api_client = AdzunaAPI(settings.ADZUNA_ID, settings.ADZUNA_KEY)
    log.debug("Adzuna search kwargs: %s", adzuna_kwargs)

    jobs, error_list = api_client.search_jobs_robust(**adzuna_kwargs)
    if error_list:
        log.info(f"Error list: {error_list}")
    jobs = api_client.to_df(jobs)

    file_name = io_utils.adzuna_save_raw_bronze(jobs, run_name, settings)

    # STAGE 2: ADD HOME URL, SAVE TO SILVER

    url_processor = HomeUrlProcessor(
        BD_HOST=settings.BD_HOST,
        BD_PORT=int(settings.BD_PORT),
        BD_USERNAME_BASE=settings.BD_USERNAME_BASE,
        BD_PASSWORD=settings.BD_PASSWORD,
        BD_COUNTRY=settings.BD_COUNTRY,
    )

    url_jobs = url_processor.add_home_urls_robust(jobs, 100, 0.01, 10, True)
    url_jobs = url_processor.clean_urls(
        url_jobs, specific_domains=["www.adzuna.co.uk", "www.linkedin.com"]
    )

    # save
    io_utils.adzuna_save_home_url_silver(url_jobs, file_name, settings)

    # STAGE 3: ADD FULL DESCRIPTION, SAVE TO GOLD

    description_processor = FullDescriptionProcessor(
        BD_HOST=settings.BD_HOST,
        BD_PORT=int(settings.BD_PORT),
        BD_USERNAME_BASE=settings.BD_USERNAME_BASE,
        BD_PASSWORD=settings.BD_PASSWORD,
        BD_COUNTRY=settings.BD_COUNTRY,
    )

    descriptions = description_processor.add_full_descriptions_robust(
        url_jobs,
        max_workers=100,
        acceptable_fault_rate=0.01,
        max_tries=5,
        initial_process=True,
    )  # Add copy=True here

    io_utils.adzuna_save_full_description_gold(descriptions, file_name, settings)

    log.info("Pipeline completed successfully")
