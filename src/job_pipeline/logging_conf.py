from pathlib import Path
from dotenv import load_dotenv
import logging
import logging.handlers
from job_pipeline.config import LOGS_DIR, LOG_LEVEL, LOG_APP_NAME


def setup_logging(
    app_name: str = None,
    log_dir: Path = None,
    level: str = None,
    keep_days: int = 7,
):
    name = app_name or LOG_APP_NAME
    lvl = getattr(logging, (level or LOG_LEVEL).upper(), logging.INFO)
    dir_ = log_dir or LOGS_DIR

    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured; still align level if caller passed a different one
        logger.setLevel(lvl)
        return logger

    dir_.mkdir(parents=True, exist_ok=True)  # leave for protection

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

    # quiet noisy libs a bit
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a module-level logger that inherits from the configured app logger.
    Call setup_logging() once in your __main__ before doing work.
    """
    return logging.getLogger(name)
