from src.core import logging as logging_module


class TestLogging:
    def test_get_logger_returns_logger(self):
        logger = logging_module.get_logger("test.module")
        assert logger is not None
        assert hasattr(logger, "info")
        assert hasattr(logger, "debug")
        assert hasattr(logger, "warning")
        assert hasattr(logger, "error")

    def test_get_logger_returns_same_type(self):
        logger1 = logging_module.get_logger("shared")
        logger2 = logging_module.get_logger("shared")
        assert isinstance(logger1, type(logger2))

    def test_setup_logging(self, monkeypatch, tmp_path):
        log_dir = tmp_path / "test_logs"
        monkeypatch.setenv("LOGS_DIR", str(log_dir))

        logging_module.setup_logging()
        logger = logging_module.get_logger("test")
        assert logger is not None

    def test_logger_outputs_to_console(self, tmp_path):
        log_dir = tmp_path / "test_logs"
        log_dir.mkdir()

        logging_module.setup_logging()

        logger = logging_module.get_logger("console_output_test")
        logger.info("hello world")

    def test_logger_can_log_different_levels(self, tmp_path):
        log_dir = tmp_path / "test_logs"
        log_dir.mkdir()

        logging_module.setup_logging()

        logger = logging_module.get_logger("level_test")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
