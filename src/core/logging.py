import logging
from pathlib import Path
import sys

from .config import settings


def setup_logging():
    log_dir = Path(settings.LOGS_DIR)
    log_dir.mkdir(exist_ok=True)

    # File handler - gets all levels (DEBUG+)
    if settings.LOG_TO_FILE:
        file_handler = logging.FileHandler(log_dir / "app.log")
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        file_handler.setLevel(logging.DEBUG)
        logging.root.addHandler(file_handler)

    # Console handler - gets INFO+ only
    if settings.LOG_TO_CONSOLE:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        console_handler.setLevel(logging.INFO)
        logging.root.addHandler(console_handler)

    # Root logger level
    logging.root.setLevel(logging.DEBUG)


def get_logger(name):
    return logging.getLogger(name)
