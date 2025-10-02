from __future__ import annotations

from pathlib import Path
import logging
import logging.handlers


def setup_logging(
    log_dir: Path,
    app_name: str | None = None,
    level: str | int | None = None,
    keep_days: int = 7,  # This parameter is now unused but kept for compatibility
    disable_file_logging: bool = False,
) -> logging.Logger:
    """
    Configure file + console logging.
    You MUST pass app_name/log_dir/level from your runtime settings (CLI or app init).
    If you pass None, sensible fallbacks are used.

    Args:
        disable_file_logging: If True, only console logging will be enabled (useful for ECS/CloudWatch)
    """
    name = app_name or "job_pipeline"
    # accept "INFO" or logging.INFO
    if isinstance(level, int):
        lvl = level
    else:
        lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)

    dir_ = Path(log_dir)

    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured; align level if caller changed it
        logger.setLevel(lvl)
        return logger

    # Only create log directory if file logging is enabled
    if not disable_file_logging:
        dir_.mkdir(parents=True, exist_ok=True)

    logger.setLevel(lvl)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler: only add if file logging is not disabled
    if not disable_file_logging:
        fh = logging.FileHandler(
            filename=dir_ / f"{name}.log",
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    # Console handler (always enabled)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # quiet noisy libs a bit
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a module-level logger that inherits from the configured app logger.
    Call setup_logging() once in your CLI (__main__) before doing work.
    """
    return logging.getLogger(name)
