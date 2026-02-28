from src.core.config import Settings


def test_config_init(monkeypatch):
    monkeypatch.setenv("LOGS_DIR", "Test")
    monkeypatch.setenv("LOG_TO_FILE", "false")
    monkeypatch.setenv("LOG_TO_CONSOLE", "false")
    settings = Settings(_env_file=None)

    assert settings.LOGS_DIR == "Test"
    assert settings.LOG_TO_CONSOLE is False
    assert settings.LOG_TO_FILE is False


def test_default_values(monkeypatch):
    monkeypatch.delenv("LOGS_DIR", raising=False)
    monkeypatch.delenv("LOG_TO_FILE", raising=False)
    monkeypatch.delenv("LOG_TO_CONSOLE", raising=False)
    settings = Settings(_env_file=None)

    assert settings.LOGS_DIR == "logs"
    assert settings.LOG_TO_FILE is True
    assert settings.LOG_TO_CONSOLE is True


def test_env_file_loading(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("LOGS_DIR=/tmp/test\nLOG_TO_CONSOLE=false\n")
    settings = Settings(_env_file=str(env_file))

    assert settings.LOGS_DIR == "/tmp/test"
    assert settings.LOG_TO_CONSOLE is False
