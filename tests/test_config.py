from src.core.config import Settings


def test_config_init(monkeypatch):
    monkeypatch.setenv("LOGGING_LEVEL", "Test")
    monkeypatch.setenv("LOGS_DIR", "Test")
    monkeypatch.setenv("LOG_FORMAT", "Test")
    monkeypatch.setenv("LOG_TO_FILE", "false")
    monkeypatch.setenv("LOG_TO_CONSOLE", "false")
    settings = Settings(_env_file=None)

    assert settings.LOGGING_LEVEL == "Test"
    assert settings.LOGS_DIR == "Test"
    assert settings.LOG_FORMAT == "Test"
    assert settings.LOG_TO_CONSOLE is False
    assert settings.LOG_TO_FILE is False
