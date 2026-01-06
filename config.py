#!/usr/bin/env python3
import json
import os

from dataclasses import dataclass

import dotenv


dotenv.load_dotenv()

__all__ = ["DefaultConfig", "config", "GitLabApiToken"]


@dataclass
class GitLabApiToken:
    name: str
    url: str
    token: str


class DefaultConfig:
    PORT = int(os.environ.get("PORT", "3980"))
    ACTIVITY_API = os.environ.get("ACTIVITY_API", "")
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    DATABASE_POOL_MIN_SIZE = int(os.environ.get("DATABASE_POOL_MIN_SIZE", "1"))
    DATABASE_POOL_MAX_SIZE = int(os.environ.get("DATABASE_POOL_MAX_SIZE", "10"))
    LOG_QUERIES = os.environ.get("LOG_QUERIES", "")
    VALID_X_GITLAB_TOKEN = os.environ.get("VALID_X_GITLAB_TOKEN", "")
    MESSAGE_DELETE_DELAY_SECONDS = int(os.environ.get("MESSAGE_DELETE_DELAY_SECONDS", "30"))
    NOTE_DEBOUNCE_SECONDS = float(os.environ.get("NOTE_DEBOUNCE_SECONDS", "5.0"))
    EMOJI_DEBOUNCE_SECONDS = float(os.environ.get("EMOJI_DEBOUNCE_SECONDS", "5.0"))
    _valid_tokens: list[str]
    _gitlab_api_tokens: dict[str, GitLabApiToken]

    def __init__(self):
        self._valid_tokens = [t.strip() for t in self.VALID_X_GITLAB_TOKEN.lower().split(",")]
        self.log_queries = False
        if len(self.LOG_QUERIES) and self.LOG_QUERIES[0].lower() in ("y", "t", "1"):
            self.log_queries = True

        self._gitlab_api_tokens = {}
        raw = os.environ.get("GITLAB_API_TOKENS", "")
        if raw:
            try:
                parsed = json.loads(raw)
                for name, cfg in parsed.items():
                    self._gitlab_api_tokens[name] = GitLabApiToken(
                        name=name, url=cfg["url"], token=cfg["token"]
                    )
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

    def is_valid_token(self, token: str) -> bool:
        return token.lower() in self._valid_tokens

    def get_gitlab_api_token(self, project_url: str) -> GitLabApiToken | None:
        """Find a matching GitLab API token for a project URL."""
        for api_token in self._gitlab_api_tokens.values():
            if project_url.startswith(api_token.url):
                return api_token
        return None


config = DefaultConfig()
