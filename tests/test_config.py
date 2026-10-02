"""Tests for execution-plane configuration."""

import pytest

from execution_plane.config import EPSettings, ScriptExecutorSettings, to_asyncpg_url


def test_to_asyncpg_url_normalizes_postgresql_driver_variants() -> None:
    """Listener URLs use the plain PostgreSQL scheme for every input variant."""
    assert (
        to_asyncpg_url("postgresql+asyncpg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql+psycopg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql://user:password@localhost/syntara") == "postgresql://user:password@localhost/syntara"
    )


class TestScriptExecutorSettingsDefaults:
    """Default values match the hardcoded literals they replaced."""

    def test_script_cleanup_terminate_timeout_default(self) -> None:
        assert ScriptExecutorSettings().script_cleanup_terminate_timeout == 1.0

    def test_script_cleanup_kill_timeout_default(self) -> None:
        assert ScriptExecutorSettings().script_cleanup_kill_timeout == 0.5

    def test_max_env_var_length_default(self) -> None:
        assert ScriptExecutorSettings().max_env_var_length == 32768

    def test_max_completion_result_bytes_default(self) -> None:
        assert ScriptExecutorSettings().max_completion_result_bytes == 1_887_436


class TestScriptExecutorSettingsEnvVars:
    """Script execution settings are overridable via environment variables."""

    def test_script_cleanup_terminate_timeout_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SCRIPT_CLEANUP_TERMINATE_TIMEOUT", "5.0")
        assert ScriptExecutorSettings().script_cleanup_terminate_timeout == 5.0

    def test_script_cleanup_kill_timeout_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SCRIPT_CLEANUP_KILL_TIMEOUT", "2.0")
        assert ScriptExecutorSettings().script_cleanup_kill_timeout == 2.0

    def test_max_env_var_length_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAX_ENV_VAR_LENGTH", "65536")
        assert ScriptExecutorSettings().max_env_var_length == 65536

    def test_max_completion_result_bytes_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EP_MAX_COMPLETION_RESULT_BYTES", "4194304")
        assert ScriptExecutorSettings().max_completion_result_bytes == 4_194_304


class TestEPSettingsInheritsScriptSettings:
    """EPSettings exposes the same script fields via inheritance."""

    def test_ep_settings_has_script_cleanup_terminate_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EP_DATABASE_URL", "postgresql+asyncpg://ep:ep@localhost/execution_plane")
        monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        settings = EPSettings()
        assert settings.script_cleanup_terminate_timeout == 1.0

    def test_ep_settings_exposes_completion_result_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EP_DATABASE_URL", "postgresql+asyncpg://ep:ep@localhost/execution_plane")
        monkeypatch.setenv("EP_CREDENTIAL_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        settings = EPSettings()
        assert settings.max_completion_result_bytes == 1_887_436
