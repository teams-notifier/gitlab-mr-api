"""Guards for the env-driven outbound timeouts.

A typo in an env var name would leave the shipped defaults in place with nothing to show for it,
so the wiring itself is what these assert, not just the helper arithmetic.
"""

import importlib

import config as config_module


def _reloaded_config():
    importlib.reload(config_module)
    return config_module.DefaultConfig()


def test_timeouts_fall_back_to_shipped_defaults(monkeypatch):
    monkeypatch.delenv("ACTIVITY_API_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("GITLAB_API_TIMEOUT_SECONDS", raising=False)

    try:
        cfg = _reloaded_config()
        assert cfg.activity_api_timeout().read == 10.0
        assert cfg.activity_api_timeout().connect == 5.0
        assert cfg.gitlab_api_timeout().read == 5.0
        assert cfg.gitlab_api_timeout().connect == 2.5
    finally:
        importlib.reload(config_module)


def test_timeouts_read_their_env_vars(monkeypatch):
    monkeypatch.setenv("ACTIVITY_API_TIMEOUT_SECONDS", "42.0")
    monkeypatch.setenv("GITLAB_API_TIMEOUT_SECONDS", "7.0")

    try:
        cfg = _reloaded_config()
        assert cfg.activity_api_timeout().read == 42.0
        assert cfg.activity_api_timeout().connect == 21.0
        assert cfg.gitlab_api_timeout().read == 7.0
        assert cfg.gitlab_api_timeout().connect == 3.5
    finally:
        importlib.reload(config_module)
