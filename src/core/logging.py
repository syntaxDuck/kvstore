import logging
from pathlib import Path
import sys

from .config import settings


def setup_logging() -> None:
    log_dir = Path(settings.LOGS_DIR)
    log_dir.mkdir(exist_ok=True)

    if settings.LOG_FORMAT == "simple":
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
    else:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

    handlers = []

    if settings.LOG_TO_FILE:
        app_handler = logging.FileHandler(
            log_dir / "app.log", mode="a", encoding="utf-8"
        )
        app_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(app_handler)

        error_handler = logging.FileHandler(
            filename=log_dir / "errors.log", mode="a", encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(error_handler)

    if settings.LOG_TO_CONSOLE:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(console_handler)

    logging.basicConfig(
        level=getattr(logging, settings.LOGGING_LEVEL.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True,
    )

    configure_component_loggers()

    logging.info("Application logging initilized")


def configure_component_loggers() -> None:
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
