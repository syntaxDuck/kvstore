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


def test_default_values(monkeypatch):
    monkeypatch.delenv("LOGGING_LEVEL", raising=False)
    monkeypatch.delenv("LOGS_DIR", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.delenv("LOG_TO_FILE", raising=False)
    monkeypatch.delenv("LOG_TO_CONSOLE", raising=False)
    settings = Settings(_env_file=None)

    assert settings.LOGGING_LEVEL == "DEBUG"
    assert settings.LOGS_DIR == "logs"
    assert settings.LOG_FORMAT == "simple"
    assert settings.LOG_TO_FILE is True
    assert settings.LOG_TO_CONSOLE is True


def test_env_file_loading(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("LOGGING_LEVEL=INFO\nLOGS_DIR=/tmp/test\n")
    settings = Settings(_env_file=str(env_file))

    assert settings.LOGGING_LEVEL == "INFO"
    assert settings.LOGS_DIR == "/tmp/test"
