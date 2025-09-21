import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import logging.handlers

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
_DEFAULT_APP_NAME = os.getenv("LOG_APP_NAME", "pipeline")  # not sure
_DEFAULT_LOG_LEVEL = os.getenv(
    "LOG_LEVEL", "INFO"
).upper()  # for debugging in production
_DEFAULT_LOG_DIR = Path(
    os.getenv("LOG_DIR", PROJECT_ROOT / "logs")
)  # might be usefull for deployment


def setup_logging(
    app_name: str = "pipeline",
    log_dir: Path = None,
    level: str = None,
    keep_days: int = 7,
):
    name = app_name or _DEFAULT_APP_NAME
    lvl = getattr(
        logging, (level or _DEFAULT_LOG_LEVEL).upper(), logging.INFO
    )  # retrieve log numbers
    dir_ = log_dir or _DEFAULT_LOG_DIR

    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured; still align level if caller passed a different one
        logger.setLevel(lvl)
        return logger

    dir_.mkdir(parents=True, exist_ok=True)
    logger.setLevel(lvl)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler: rotate daily, keep N days
    fh = logging.handlers.TimedRotatingFileHandler(
        filename=dir_ / f"{name}.log",
        when="D",
        interval=1,
        backupCount=keep_days,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # Optional: quiet noisy libs a bit
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    # Allow child loggers to propagate to this logger so console output works
    logger.propagate = True

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a module-level logger that inherits from the configured app logger.
    Call setup_logging() once in your __main__ before doing work.
    """
    return logging.getLogger(name)
