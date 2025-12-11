#!/usr/bin/env python3
import datetime
import uuid

from typing import Any

import asyncpg
import fastapi_structured_logging
import httpx

from pydantic import BaseModel

from config import config
from db import MergeRequestInfos
from db import database


logger = fastapi_structured_logging.get_logger()


class MRMessRef(BaseModel):
    merge_request_message_ref_id: int
    conversation_token: uuid.UUID
    message_id: uuid.UUID | None
    last_processed_fingerprint: str | None = None
    last_processed_updated_at: datetime.datetime | None = None


async def get_or_create_message_refs(
    merge_request_ref_id: int,
    conv_tokens: list[str],
) -> dict[str, MRMessRef]:
    convtoken_to_msgrefs: dict[str, MRMessRef] = {}

    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                """SELECT 1 FROM merge_request_ref
                   WHERE merge_request_ref_id = $1
                   FOR UPDATE""",
                merge_request_ref_id,
            )

            resset = await connection.fetch(
                """
                SELECT
                    merge_request_message_ref_id,
                    conversation_token,
                    message_id,
                    last_processed_fingerprint,
                    last_processed_updated_at
                FROM
                    merge_request_message_ref
                WHERE
                    merge_request_ref_id = $1
                """,
                merge_request_ref_id,
            )

            for row in resset:
                convtoken_to_msgrefs[str(row["conversation_token"])] = MRMessRef(**row)

            for conv_token in conv_tokens:
                if conv_token in convtoken_to_msgrefs:
                    continue
                row = await connection.fetchrow(
                    """
                    INSERT INTO merge_request_message_ref (
                        merge_request_ref_id, conversation_token
                    ) VALUES (
                        $1, $2
                    ) ON CONFLICT (merge_request_ref_id, conversation_token) DO UPDATE
                        SET merge_request_ref_id = EXCLUDED.merge_request_ref_id
                    RETURNING merge_request_message_ref_id,
                                conversation_token,
                                message_id
                    """,
                    merge_request_ref_id,
                    conv_token,
                )
                assert row is not None
                convtoken_to_msgrefs[str(row["conversation_token"])] = MRMessRef(**row)

    return convtoken_to_msgrefs


async def get_all_message_refs(merge_request_ref_id: int) -> list[MRMessRef]:
    """
    Get ALL message refs for an MR, regardless of conversation tokens.

    Used for update/approve operations to process all messages including orphaned ones.
    """
    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                """SELECT 1 FROM merge_request_ref
                   WHERE merge_request_ref_id = $1
                   FOR UPDATE""",
                merge_request_ref_id,
            )

            resset = await connection.fetch(
                """
                SELECT
                    merge_request_message_ref_id,
                    conversation_token,
                    message_id,
                    last_processed_fingerprint,
                    last_processed_updated_at
                FROM
                    merge_request_message_ref
                WHERE
                    merge_request_ref_id = $1
                """,
                merge_request_ref_id,
            )

            return [MRMessRef(**row) for row in resset]


async def create_or_update_message(
    client: httpx.AsyncClient,
    mrmsgref: MRMessRef,
    *,
    message_text: str | None = None,
    card: dict[str, Any] | None = None,
    summary: str | None = None,
    update_only: bool = False,
) -> uuid.UUID | None:
    payload: dict[str, Any]
    if message_text:
        payload = {"text": message_text}
    if card:
        payload = {"card": card}
        if summary:
            payload["summary"] = summary

    if mrmsgref.message_id is None:
        if update_only is True:
            return None

        payload["conversation_token"] = str(mrmsgref.conversation_token)
        try:
            res = await client.request(
                "POST",
                config.ACTIVITY_API + "api/v1/message",
                json=payload,
            )
            res.raise_for_status()
            response = res.json()
        except Exception:
            logger.error(
                "failed to create message",
                method="POST",
                url=config.ACTIVITY_API + "api/v1/message",
                conversation_token=str(mrmsgref.conversation_token),
                status_code=res.status_code if "res" in locals() else None,
                exc_info=True,
            )
            raise

        connection: asyncpg.Connection
        async with await database.acquire() as connection:
            result = await connection.fetchrow(
                """UPDATE merge_request_message_ref
                    SET message_id = $1
                    WHERE merge_request_message_ref_id = $2
                        AND message_id IS NULL
                    RETURNING merge_request_message_ref_id
                """,
                response.get("message_id"),
                mrmsgref.merge_request_message_ref_id,
            )
        if result is None or len(result) == 0:
            try:
                await client.request(
                    "DELETE",
                    config.ACTIVITY_API + "api/v1/message",
                    json={
                        "message_id": str(response.get("message_id")),
                    },
                )
            except Exception:
                logger.exception("Failed to delete duplicate message %s", response.get("message_id"))
    else:
        payload["message_id"] = str(mrmsgref.message_id)
        try:
            res = await client.request(
                "PATCH",
                config.ACTIVITY_API + "api/v1/message",
                json=payload,
            )
            res.raise_for_status()
            response = res.json()
        except Exception:
            logger.error(
                "failed to update message",
                method="PATCH",
                url=config.ACTIVITY_API + "api/v1/message",
                message_id=str(mrmsgref.message_id),
                status_code=res.status_code if "res" in locals() else None,
                exc_info=True,
            )
            raise
    return uuid.UUID(response.get("message_id"))


async def update_message_with_fingerprint(
    client: httpx.AsyncClient,
    mrmsgref: MRMessRef,
    card: dict[str, Any],
    summary: str | None,
    payload_fingerprint: str,
    payload_updated_at: datetime.datetime,
) -> None:
    """
    Update an existing message via Teams API and store the fingerprint.

    Assumes message_id is not None.
    """
    if mrmsgref.message_id is None:
        logger.warning(
            "update_message_with_fingerprint called with NULL message_id - skipping",
            merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
        )
        return

    payload = {
        "message_id": str(mrmsgref.message_id),
        "card": card,
    }
    if summary:
        payload["summary"] = summary

    try:
        res = await client.request(
            "PATCH",
            config.ACTIVITY_API + "api/v1/message",
            json=payload,
        )
        res.raise_for_status()

        connection: asyncpg.Connection
        async with await database.acquire() as connection:
            # Conditional update: only if new timestamp is actually newer (handles races)
            result = await connection.execute(
                """UPDATE merge_request_message_ref
                   SET last_processed_fingerprint = $1, last_processed_updated_at = $2
                   WHERE merge_request_message_ref_id = $3
                     AND (last_processed_updated_at IS NULL OR last_processed_updated_at < $2)""",
                payload_fingerprint,
                payload_updated_at,
                mrmsgref.merge_request_message_ref_id,
            )

        if result == "UPDATE 0":
            logger.warning(
                "message update skipped - newer timestamp already stored (race)",
                message_id=str(mrmsgref.message_id),
                payload_updated_at=payload_updated_at.isoformat(),
            )
        else:
            logger.debug(
                "updated message with fingerprint",
                message_id=str(mrmsgref.message_id),
                fingerprint=payload_fingerprint,
                updated_at=payload_updated_at.isoformat(),
            )
    except Exception:
        logger.error(
            "failed to update message",
            method="PATCH",
            url=config.ACTIVITY_API + "api/v1/message",
            message_id=str(mrmsgref.message_id),
            status_code=res.status_code if "res" in locals() else None,
            exc_info=True,
        )
        raise


async def update_all_messages_transactional(
    mri: MergeRequestInfos,
    card: dict[str, Any],
    summary: str,
    payload_fingerprint: str,
    payload_updated_at: datetime.datetime | None,
    action_name: str,
    schedule_deletion: bool = False,
    deletion_delay: datetime.timedelta | None = None,
) -> int:
    """
    Update all messages for an MR within a transaction with row locking.

    For non-deletion operations, checks per-message fingerprints and skips
    messages already processed with the same payload.

    Returns the number of messages updated.
    """
    logger.info(
        f"{action_name} action - locking and updating all messages",
        mr_ref_id=mri.merge_request_ref_id,
        fingerprint=payload_fingerprint,
    )

    connection: asyncpg.Connection
    timeout = httpx.Timeout(10.0, connect=5.0)
    message_count = 0

    async with await database.acquire() as connection:
        async with connection.transaction():
            locked_messages = await connection.fetch(
                """SELECT merge_request_message_ref_id, conversation_token,
                          message_id, last_processed_fingerprint,
                          last_processed_updated_at
                    FROM merge_request_message_ref
                    WHERE merge_request_ref_id = $1
                    FOR UPDATE""",
                mri.merge_request_ref_id,
            )

            if len(locked_messages) > 0:
                logger.info(
                    f"locked messages for {action_name} update",
                    mr_ref_id=mri.merge_request_ref_id,
                    message_count=len(locked_messages),
                )

                async with httpx.AsyncClient(timeout=timeout) as client:
                    for row in locked_messages:
                        message_id = row.get("message_id")
                        ref_id = row.get("merge_request_message_ref_id")

                        if not schedule_deletion and payload_updated_at is not None:
                            stored_updated_at = row.get("last_processed_updated_at")
                            if stored_updated_at is not None:
                                if payload_updated_at < stored_updated_at:
                                    logger.warning(
                                        f"skipping {action_name} - out of order event",
                                        merge_request_message_ref_id=ref_id,
                                        payload_updated_at=payload_updated_at.isoformat(),
                                        stored_updated_at=stored_updated_at.isoformat(),
                                    )
                                    continue
                                if (
                                    payload_updated_at == stored_updated_at
                                    and row.get("last_processed_fingerprint") == payload_fingerprint
                                ):
                                    logger.debug(
                                        f"skipping {action_name} - same timestamp and fingerprint",
                                        merge_request_message_ref_id=ref_id,
                                        fingerprint=payload_fingerprint,
                                    )
                                    continue

                        if message_id is not None:
                            try:
                                payload = {
                                    "message_id": str(message_id),
                                    "card": card,
                                }
                                if summary:
                                    payload["summary"] = summary

                                res = await client.request(
                                    "PATCH",
                                    config.ACTIVITY_API + "api/v1/message",
                                    json=payload,
                                )
                                res.raise_for_status()
                                message_count += 1

                                if not schedule_deletion:
                                    await connection.execute(
                                        """UPDATE merge_request_message_ref
                                           SET last_processed_fingerprint = $1, last_processed_updated_at = $2
                                           WHERE merge_request_message_ref_id = $3""",
                                        payload_fingerprint,
                                        payload_updated_at,
                                        ref_id,
                                    )

                                logger.debug(
                                    f"updated message for {action_name}",
                                    message_id=str(message_id),
                                    mr_ref_id=mri.merge_request_ref_id,
                                )
                            except Exception:
                                logger.error(
                                    f"failed to update message during {action_name} action",
                                    method="PATCH",
                                    url=config.ACTIVITY_API + "api/v1/message",
                                    message_id=str(message_id),
                                    mr_ref_id=mri.merge_request_ref_id,
                                    status_code=res.status_code if "res" in locals() else None,
                                    exc_info=True,
                                )

                if schedule_deletion:
                    if deletion_delay is None:
                        deletion_delay = datetime.timedelta(seconds=0)

                    for row in locked_messages:
                        message_id = row.get("message_id")
                        if message_id is not None:
                            await connection.execute(
                                """INSERT INTO msg_to_delete
                                    (message_id, expire_at)
                                VALUES
                                    ($1, now()+$2::INTERVAL)""",
                                str(message_id),
                                deletion_delay,
                            )
                        await connection.execute(
                            """DELETE FROM merge_request_message_ref
                                    WHERE merge_request_message_ref_id = $1""",
                            row.get("merge_request_message_ref_id"),
                        )

                logger.info(
                    f"{action_name} update completed",
                    mr_ref_id=mri.merge_request_ref_id,
                    message_count=len(locked_messages),
                    fingerprint=payload_fingerprint,
                )

    return message_count
