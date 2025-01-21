#!/usr/bin/env python3
import os

import dotenv

dotenv.load_dotenv()

__all__ = ["DefaultConfig", "config"]


class DefaultConfig:
    PORT = int(os.environ.get("PORT", "3980"))
    ACTIVITY_API = os.environ.get("ACTIVITY_API", "")
    DATABASE_URL = os.environ.get("DATABASE_URL", "")
    DATABASE_POOL_MIN_SIZE = int(os.environ.get("DATABASE_POOL_MIN_SIZE", "1"))
    DATABASE_POOL_MAX_SIZE = int(os.environ.get("DATABASE_POOL_MAX_SIZE", "10"))
    LOG_QUERIES = os.environ.get("LOG_QUERIES", "")
    VALID_X_GITLAB_TOKEN = os.environ.get("VALID_X_GITLAB_TOKEN", "")
    _valid_tokens: list[str]

    def __init__(self):
        self._valid_tokens = list([t.strip() for t in self.VALID_X_GITLAB_TOKEN.lower().split(",")])
        self.log_queries = False
        if len(self.LOG_QUERIES) and self.LOG_QUERIES[0].lower() in ("y", "t", "1"):
            self.log_queries = True

    def is_valid_token(self, token: str) -> bool:
        return token.lower() in self._valid_tokens


config = DefaultConfig()
