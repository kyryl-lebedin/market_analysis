from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Mapping, Optional

from dotenv import load_dotenv

from job_pipeline.config import get_settings
from job_pipeline.logging_conf import setup_logging
from job_pipeline.pipeline import run_pipeline
from job_pipeline.secrets import load_secrets_to_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="job-pipeline",
        description="Run the job pipeline using a preset or ad-hoc search kwargs.",
    )
    parser.add_argument(
        "--preset",
        default=None,  # Will be set from env or default later
        help="Preset key to run (ignored if --kwargs is provided). Can also be set via PIPELINE_PRESET env var.",
    )
    parser.add_argument(
        "--name",
        default=None,  # Will be set from env or default later
        help="Run name used in the saved filenames (bronze/silver/gold). Can also be set via PIPELINE_RUN_NAME env var.",
    )
    parser.add_argument(
        "--concurrency",
        default=None,
        type=int,
        help="Max workers for concurrency (overrides preset max_workers). Can also be set via PIPELINE_CONCURRENCY env var.",
    )
    parser.add_argument(
        "--kwargs",
        default=None,
        help=(
            "Optional JSON dict of ad-hoc search params for Adzuna, e.g. "
            '\'{"country":"gb","formatted":true,"scope":"single_page","page":6,"what_and":"data_scientist"}\'. '
            "If provided, --preset is ignored. Can also be set via PIPELINE_SEARCH_KWARGS env var."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Override LOG_LEVEL for this run (e.g., DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


def main() -> None:
    # 1) Load .env into process env
    load_dotenv()

    # 1.5) Load secrets from AWS
    load_secrets_to_env()

    # 2) Build settings (validated by Pydantic)
    settings = get_settings()

    # 3) Ensure directories exist (safety)
    settings.paths.ensure_dirs()

    # 4) Parse CLI args
    args = parse_args()

    # 5) Configure logging using your existing helper
    level = args.log_level or settings.logging.level
    logger = setup_logging(
        app_name=settings.logging.app_name,
        log_dir=settings.paths.logs,
        level=level,
        disable_file_logging=settings.logging.disable_file_logging,
    )
    logger.info("CLI started")

    # 6) Resolve parameters with environment variable fallbacks
    # Preset: CLI arg -> env var -> default
    preset_name = args.preset or settings.PIPELINE_PRESET or "test_page_list"

    # Run name: CLI arg -> env var -> default
    run_name = args.name or settings.PIPELINE_RUN_NAME or "test1"

    # Concurrency: CLI arg -> env var -> None (will use preset default)
    concurrency = args.concurrency or settings.PIPELINE_CONCURRENCY

    # 7) Optional ad-hoc kwargs (JSON). If present, ignore preset.
    search_kwargs: Optional[Mapping[str, Any]] = None

    # Check CLI args first, then environment variable
    kwargs_source = args.kwargs or settings.PIPELINE_SEARCH_KWARGS

    if kwargs_source:
        try:
            search_kwargs = json.loads(kwargs_source)
            if not isinstance(search_kwargs, dict):
                raise ValueError("kwargs must parse to a JSON object")
        except Exception as exc:
            logger.error("Failed to parse search kwargs JSON: %s", exc)
            sys.exit(2)

    # 8) Run the pipeline
    try:
        run_pipeline(
            settings=settings,
            preset_name=None if search_kwargs else preset_name,
            run_name=run_name,
            search_kwargs=search_kwargs,
            concurrency=concurrency,
        )
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()
