from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Mapping, Optional

from dotenv import load_dotenv

from job_pipeline.config import get_settings
from job_pipeline.logging_conf import setup_logging
from job_pipeline.pipeline import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="job-pipeline",
        description="Run the job pipeline using a preset or ad-hoc search kwargs.",
    )
    parser.add_argument(
        "--preset",
        default="test_page_list",
        help="Preset key to run (ignored if --kwargs is provided).",
    )
    parser.add_argument(
        "--name",
        default="test1",
        help="Run name used in the saved filenames (bronze/silver/gold).",
    )
    parser.add_argument(
        "--kwargs",
        default=None,
        help=(
            "Optional JSON dict of ad-hoc search params for Adzuna, e.g. "
            '\'{"country":"gb","formatted":true,"scope":"single_page","page":6,"what_and":"data_scientist"}\'. '
            "If provided, --preset is ignored."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Override LOG_LEVEL for this run (e.g., DEBUG, INFO, WARNING).",
    )
    return parser.parse_args()


def main() -> None:
    # 1) Load .env into process env (single boundary)
    load_dotenv()

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
    )
    logger.info("CLI started")

    # 6) Optional ad-hoc kwargs (JSON). If present, ignore preset.
    search_kwargs: Optional[Mapping[str, Any]] = None
    if args.kwargs:
        try:
            search_kwargs = json.loads(args.kwargs)
            if not isinstance(search_kwargs, dict):
                raise ValueError("kwargs must parse to a JSON object")
        except Exception as exc:
            logger.error("Failed to parse --kwargs JSON: %s", exc)
            sys.exit(2)

    # 7) Run the pipeline
    try:
        run_pipeline(
            settings=settings,
            preset_name=None if search_kwargs else args.preset,
            run_name=args.name,
            search_kwargs=search_kwargs,
        )
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()
