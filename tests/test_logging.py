import pytest
import logging
import logging.config
import tempfile
import os
from pathlib import Path
from unittest.mock import patch

from src.core import logging as logging_module


class TestLogging:
    def test_get_logger_returns_logger(self):
        logger = logging_module.get_logger("test.module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_get_logger_same_name_returns_same_logger(self):
        logger1 = logging_module.get_logger("shared")
        logger2 = logging_module.get_logger("shared")
        assert logger1 is logger2

    def test_configure_component_loggers(self):
        logging_module.configure_component_loggers()
        asyncio_logger = logging.getLogger("asyncio")
        assert asyncio_logger.level == logging.WARNING

    def test_setup_logging_with_console(self, monkeypatch, tmp_path, capsys):
        log_dir = tmp_path / "test_logs"
        monkeypatch.setenv("LOGS_DIR", str(log_dir))
        monkeypatch.setenv("LOG_TO_FILE", "false")
        monkeypatch.setenv("LOG_TO_CONSOLE", "true")

        with patch.object(logging_module, "settings") as mock_settings:
            mock_settings.LOGS_DIR = str(log_dir)
            mock_settings.LOG_TO_FILE = False
            mock_settings.LOG_TO_CONSOLE = True
            mock_settings.LOG_FORMAT = "simple"
            mock_settings.LOGGING_LEVEL = "DEBUG"

            logging_module.setup_logging()

            logger = logging_module.get_logger("console_test")
            logger.info("test message")
            captured = capsys.readouterr()
            assert "test message" in captured.out

    def test_setup_logging_creates_files(self, monkeypatch, tmp_path):
        log_dir = tmp_path / "test_logs"
        log_dir.mkdir()

        with patch.object(logging_module, "settings") as mock_settings:
            mock_settings.LOGS_DIR = str(log_dir)
            mock_settings.LOG_TO_FILE = True
            mock_settings.LOG_TO_CONSOLE = False
            mock_settings.LOG_FORMAT = "simple"
            mock_settings.LOGGING_LEVEL = "DEBUG"

            logging_module.setup_logging()

            assert (log_dir / "app.log").exists()
            assert (log_dir / "errors.log").exists()

    def test_logger_integration(self, tmp_path):
        log_dir = tmp_path / "test_logs"
        log_dir.mkdir()

        with patch.object(logging_module, "settings") as mock_settings:
            mock_settings.LOGS_DIR = str(log_dir)
            mock_settings.LOG_TO_FILE = True
            mock_settings.LOG_TO_CONSOLE = False
            mock_settings.LOG_FORMAT = "simple"
            mock_settings.LOGGING_LEVEL = "DEBUG"

            logging_module.setup_logging()

            logger = logging_module.get_logger("integration_test")
            logger.debug("debug message")
            logger.info("info message")
            logger.warning("warning message")
            logger.error("error message")

            app_log = (log_dir / "app.log").read_text()
            assert "debug message" in app_log
            assert "info message" in app_log
            assert "warning message" in app_log
            assert "error message" in app_log

            error_log = (log_dir / "errors.log").read_text()
            assert "error message" in error_log
