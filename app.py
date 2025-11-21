#!/usr/bin/env python3
import asyncio
import logging
import os
import sys
import traceback
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
import fastapi_structured_logging
import httpx
from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse

import webhook
from config import DefaultConfig
from db import database
from gitlab_model import EmojiPayload
from gitlab_model import MergeRequestPayload
from gitlab_model import PipelinePayload
from periodic_cleanup import periodic_cleanup

config = DefaultConfig()

# Configure structured logging
log_format = os.getenv("LOG_FORMAT", "auto").lower()
log_level = os.getenv("LOG_LEVEL", "INFO").upper()

if log_format == "json":
    fastapi_structured_logging.setup_logging(json_logs=True, log_level=log_level)
elif log_format == "line":
    fastapi_structured_logging.setup_logging(json_logs=False, log_level=log_level)
else:
    fastapi_structured_logging.setup_logging(log_level=log_level)

logging.getLogger("uvicorn.error").disabled = True

# Suppress traceback printing to stderr
traceback.print_exception = lambda *args, **kwargs: None

logger = fastapi_structured_logging.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting app version %s", app.version)
    await database.connect()
    task = asyncio.create_task(periodic_cleanup(config, database))

    yield

    task.cancel()
    await database.disconnect()


app: FastAPI = FastAPI(
    title="Teams Notifier gitlab-mr-api",
    version=os.environ.get("VERSION", "v0.0.0-dev"),
    lifespan=lifespan,
)

app.add_middleware(fastapi_structured_logging.AccessLogMiddleware)


@app.exception_handler(asyncpg.UniqueViolationError)
async def database_uniqueviolation_handler(request: Request, exc: asyncpg.UniqueViolationError):
    logger.error(
        "database unique violation error",
        error_type=type(exc).__name__,
        error_detail=str(exc),
        constraint=getattr(exc, "constraint_name", None),
        path=request.url.path,
        method=request.method,
    )
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"detail": "Resource already exists", "error": str(exc)},
    )


@app.exception_handler(asyncpg.PostgresError)
async def database_exception_handler(request: Request, exc: asyncpg.PostgresError):
    logger.error(
        "database error",
        error_type=type(exc).__name__,
        error_detail=str(exc),
        sqlstate=getattr(exc, "sqlstate", None),
        path=request.url.path,
        method=request.method,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Database error occurred"},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()

    logger.error(
        "unhandled exception",
        error_type=type(exc).__name__,
        error_detail=str(exc),
        path=request.url.path,
        method=request.method,
        traceback="".join(traceback.format_exception(exc_type, exc_value, exc_traceback)),
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


@app.get("/", response_class=RedirectResponse, status_code=302)
async def root():
    return "/docs"


def validate_gitlab_token(token: str) -> None:
    if not config.is_valid_token(token):
        raise HTTPException(status_code=403, detail=f"Invalid gitlab token {token}")


def validate_uuid(val: str) -> str | None:
    try:
        return str(uuid.UUID(str(val)))
    except ValueError:
        return None


@app.post("/api/v1/gitlab-webhook")
async def handle_webhook(
    payload: MergeRequestPayload | PipelinePayload | EmojiPayload,
    x_conversation_token: Annotated[str, Header()],
    x_gitlab_token: Annotated[str, Header()],
    filter_on_participant_ids: str | None = None,
    new_commits_revoke_approvals: bool = True,
):
    validate_gitlab_token(x_gitlab_token)
    conversation_tokens = list(
        filter(
            None,
            [validate_uuid(ct.strip()) for ct in x_conversation_token.split(",")],
        )
    )

    try:
        if isinstance(payload, MergeRequestPayload):
            participant_ids_filter: list[int] = []
            if filter_on_participant_ids:
                try:
                    participant_ids_filter = [int(entry) for entry in filter_on_participant_ids.split(",")]
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="filter_on_participant_ids must be a list of comma separated integers",
                    )

            await webhook.merge_request(
                payload,
                conversation_tokens,
                participant_ids_filter,
                new_commits_revoke_approvals,
            )
        if isinstance(payload, PipelinePayload):
            await webhook.pipeline(payload, conversation_tokens)
        if isinstance(payload, EmojiPayload):
            await webhook.emoji(payload, conversation_tokens)
        return {"status": "ok"}
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=exc.response.json(),
        )


@app.get("/healthz", include_in_schema=False)
async def healthcheck():
    try:
        connection: asyncpg.pool.PoolConnectionProxy
        async with await database.acquire() as connection:
            result = await connection.fetchval("SELECT true FROM merge_request_ref")
            return {"ok": result}
    except Exception as e:
        logger.error(
            "health check failed",
            error_type=type(e).__name__,
            error_detail=str(e),
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=f"{type(e)}: {e}")


if __name__ == "__main__":
    # fmt: off
    print(
        "use fastapi cli to run this app\n"
        "- fastapi run # for prod\n"
        "- fastapi dev # for dev :)\n"
    )
    # fmt: on

    # for debug entry point
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
