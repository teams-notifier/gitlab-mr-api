#!/usr/bin/env python3
import datetime
import hashlib
import uuid
from typing import Any

import asyncpg
import fastapi_structured_logging
import httpx
from pydantic import BaseModel

import periodic_cleanup
from cards.render import render
from config import config
from db import database
from db import dbh
from gitlab_model import MergeRequestPayload

logger = fastapi_structured_logging.get_logger()


class MRMessRef(BaseModel):
    merge_request_message_ref_id: int
    conversation_token: uuid.UUID
    message_id: uuid.UUID | None


async def get_or_create_message_refs(
    merge_request_ref_id: int,
    conv_tokens: list[str],
) -> dict[str, MRMessRef]:
    convtoken_to_msgrefs: dict[str, MRMessRef] = {}

    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        async with connection.transaction():
            # Lock the MR ref to prevent concurrent modifications
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
                    message_id
                FROM
                    merge_request_message_ref
                WHERE
                    merge_request_ref_id = $1
                --  AND conversation_token = ANY($2::uuid[])
                """,
                merge_request_ref_id,
                # conv_tokens,
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


async def merge_request(
    mr: MergeRequestPayload,
    conversation_tokens: list[str],
    participant_ids_filter: list[int],
    new_commits_revoke_approvals: bool,
):
    payload_fingerprint = hashlib.sha256(mr.model_dump_json().encode("utf8")).hexdigest()
    logger.debug("payload fingerprint: %s", payload_fingerprint)

    is_closing_action = mr.object_attributes.action in ("merge", "close") or mr.object_attributes.state in (
        "closed",
        "merged",
    )

    mri = await dbh.get_merge_request_ref_infos(mr)

    need_cleanup_reschedule = False

    participant_found = True
    if participant_ids_filter:
        participant_found = False
        participant_found |= mri.merge_request_extra_state.opener.id in participant_ids_filter
        participant_found |= any(
            [
                user.id in participant_ids_filter
                for userlist in (mri.merge_request_payload.assignees, mri.merge_request_payload.reviewers)
                for user in userlist
            ]
        )

    connection: asyncpg.Connection

    if mr.object_attributes.action in ("update"):
        async with await database.acquire() as connection:
            row = await connection.fetchrow(
                """UPDATE merge_request_ref
                    SET
                        head_pipeline_id = $1
                    WHERE merge_request_ref_id = $2
                    RETURNING merge_request_extra_state""",
                mr.object_attributes.head_pipeline_id,
                mri.merge_request_ref_id,
            )
            if row is not None:
                mri.merge_request_extra_state = row["merge_request_extra_state"]

            if new_commits_revoke_approvals and mr.object_attributes.oldrev:
                row = await connection.fetchrow(
                    """UPDATE merge_request_ref
                        SET merge_request_extra_state = jsonb_set(merge_request_extra_state, $1, $2::jsonb)
                        WHERE merge_request_ref_id = $3
                        RETURNING merge_request_extra_state""",
                    ["approvers"],
                    {},
                    mri.merge_request_ref_id,
                )
                print(mri.merge_request_ref_id)
                if row is not None:
                    mri.merge_request_extra_state = row["merge_request_extra_state"]

            if mr.changes and "draft" in mr.changes and not mr.object_attributes.draft:
                logger.info(
                    "draft to ready transition detected - locking and updating all messages",
                    mr_ref_id=mri.merge_request_ref_id,
                    fingerprint=payload_fingerprint,
                )

                message_expiration = datetime.timedelta(seconds=0)
                timeout = httpx.Timeout(10.0, connect=5.0)

                async with connection.transaction():
                    locked_messages = await connection.fetch(
                        """SELECT merge_request_message_ref_id, conversation_token, message_id
                            FROM merge_request_message_ref
                            WHERE merge_request_ref_id = $1
                            FOR UPDATE""",
                        mri.merge_request_ref_id,
                    )

                    if len(locked_messages) > 0:
                        logger.info(
                            "locked messages for draft-to-ready update",
                            mr_ref_id=mri.merge_request_ref_id,
                            message_count=len(locked_messages),
                        )

                        temp_mri = await dbh.get_merge_request_ref_infos(mr)
                        temp_card = render(
                            temp_mri,
                            collapsed=False,
                            show_collapsible=False,
                        )
                        temp_summary = (
                            f"MR {temp_mri.merge_request_payload.object_attributes.state}:"
                            f" {temp_mri.merge_request_payload.object_attributes.title}\n"
                            f"on {temp_mri.merge_request_payload.project.path_with_namespace}"
                        )

                        async with httpx.AsyncClient(timeout=timeout) as client:
                            for row in locked_messages:
                                message_id = row.get("message_id")
                                if message_id is not None:
                                    try:
                                        payload = {
                                            "message_id": str(message_id),
                                            "card": temp_card,
                                        }
                                        if temp_summary:
                                            payload["summary"] = temp_summary

                                        res = await client.request(
                                            "PATCH",
                                            config.ACTIVITY_API + "api/v1/message",
                                            json=payload,
                                        )
                                        res.raise_for_status()
                                        logger.debug(
                                            "updated message for draft-to-ready",
                                            message_id=str(message_id),
                                            mr_ref_id=mri.merge_request_ref_id,
                                        )
                                    except Exception:
                                        logger.error(
                                            "failed to update message during draft-to-ready",
                                            method="PATCH",
                                            url=config.ACTIVITY_API + "api/v1/message",
                                            message_id=str(message_id),
                                            mr_ref_id=mri.merge_request_ref_id,
                                            status_code=res.status_code if "res" in locals() else None,
                                            exc_info=True,
                                        )

                        for row in locked_messages:
                            message_id = row.get("message_id")
                            if message_id is not None:
                                await connection.execute(
                                    """INSERT INTO msg_to_delete
                                        (message_id, expire_at)
                                    VALUES
                                        ($1, now()+$2::INTERVAL)""",
                                    str(message_id),
                                    message_expiration,
                                )
                            await connection.execute(
                                """DELETE FROM merge_request_message_ref
                                        WHERE merge_request_message_ref_id = $1""",
                                row.get("merge_request_message_ref_id"),
                            )

                        await connection.execute(
                            """INSERT INTO webhook_fingerprint (fingerprint, processed_at)
                                VALUES ($1, now())
                                ON CONFLICT (fingerprint) DO NOTHING""",
                            payload_fingerprint,
                        )

                        logger.info(
                            "marked all draft messages for deletion",
                            mr_ref_id=mri.merge_request_ref_id,
                            message_count=len(locked_messages),
                            fingerprint=payload_fingerprint,
                        )
                        need_cleanup_reschedule = True

    if mr.object_attributes.action in ("approved", "unapproved"):
        v = mr.user.model_dump()
        v["status"] = mr.object_attributes.action

        async with await database.acquire() as connection:
            await connection.fetchrow(
                """UPDATE merge_request_ref
                    SET merge_request_extra_state = jsonb_set(merge_request_extra_state, $1, $2::jsonb)
                    WHERE merge_request_ref_id = $3
                    RETURNING merge_request_extra_state""",
                ["approvers", str(v["id"])],
                v,
                mri.merge_request_ref_id,
            )

    mri = await dbh.get_merge_request_ref_infos(mr)
    should_be_collapsed: bool = (
        mr.object_attributes.draft
        or mr.object_attributes.work_in_progress
        or mr.object_attributes.state
        in (
            "closed",
            "merged",
        )
    )
    card = render(
        mri,
        collapsed=should_be_collapsed,
        show_collapsible=should_be_collapsed,
    )
    summary = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}\n"
        f"on {mri.merge_request_payload.project.path_with_namespace}"
    )

    if is_closing_action:
        logger.info(
            "close/merge action detected - locking and updating all messages",
            mr_ref_id=mri.merge_request_ref_id,
            action=mr.object_attributes.action,
            state=mr.object_attributes.state,
            fingerprint=payload_fingerprint,
        )

        message_expiration = datetime.timedelta(seconds=config.MESSAGE_DELETE_DELAY_SECONDS)
        timeout = httpx.Timeout(10.0, connect=5.0)

        async with await database.acquire() as connection:
            async with connection.transaction():
                locked_messages = await connection.fetch(
                    """SELECT merge_request_message_ref_id, conversation_token, message_id
                        FROM merge_request_message_ref
                        WHERE merge_request_ref_id = $1
                        FOR UPDATE""",
                    mri.merge_request_ref_id,
                )

                if len(locked_messages) > 0:
                    logger.info(
                        "locked messages for update",
                        mr_ref_id=mri.merge_request_ref_id,
                        message_count=len(locked_messages),
                    )

                    async with httpx.AsyncClient(timeout=timeout) as client:
                        for row in locked_messages:
                            message_id = row.get("message_id")
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
                                    logger.debug(
                                        "updated message",
                                        message_id=str(message_id),
                                        mr_ref_id=mri.merge_request_ref_id,
                                    )
                                except Exception:
                                    logger.error(
                                        "failed to update message during close/merge",
                                        method="PATCH",
                                        url=config.ACTIVITY_API + "api/v1/message",
                                        message_id=str(message_id),
                                        mr_ref_id=mri.merge_request_ref_id,
                                        status_code=res.status_code if "res" in locals() else None,
                                        exc_info=True,
                                    )

                    for row in locked_messages:
                        message_id = row.get("message_id")
                        if message_id is not None:
                            await connection.execute(
                                """INSERT INTO msg_to_delete
                                    (message_id, expire_at)
                                VALUES
                                    ($1, now()+$2::INTERVAL)""",
                                str(message_id),
                                message_expiration,
                            )
                        await connection.execute(
                            "DELETE FROM merge_request_message_ref WHERE merge_request_message_ref_id = $1",
                            row.get("merge_request_message_ref_id"),
                        )

                    await connection.execute(
                        """INSERT INTO webhook_fingerprint (fingerprint, processed_at)
                            VALUES ($1, now())
                            ON CONFLICT (fingerprint) DO NOTHING""",
                        payload_fingerprint,
                    )

                    logger.info(
                        "marked all messages for deletion",
                        mr_ref_id=mri.merge_request_ref_id,
                        message_count=len(locked_messages),
                        fingerprint=payload_fingerprint,
                    )
                    need_cleanup_reschedule = True
    else:
        convtoken_to_msgrefs = await get_or_create_message_refs(
            mri.merge_request_ref_id,
            conversation_tokens,
        )

        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for ct in conversation_tokens:
                mrmsgref = convtoken_to_msgrefs[ct]
                original_message_id = mrmsgref.message_id

                mrmsgref.message_id = await create_or_update_message(
                    client,
                    mrmsgref,
                    card=card,
                    summary=summary,
                    update_only=(
                        (
                            mr.object_attributes.action not in ("open", "reopen")
                            and (mr.object_attributes.draft or mr.object_attributes.work_in_progress)
                        )
                        or not participant_found
                    ),
                )

                if original_message_id is None and mrmsgref.message_id is not None:
                    async with await database.acquire() as conn:
                        await conn.execute(
                            """UPDATE merge_request_message_ref
                               SET message_id = $1
                               WHERE merge_request_message_ref_id = $2""",
                            mrmsgref.message_id,
                            mrmsgref.merge_request_message_ref_id,
                        )

    if need_cleanup_reschedule:
        periodic_cleanup.reschedule()

    return {
        "merge_request_infos": mri,
    }
