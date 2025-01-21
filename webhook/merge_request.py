#!/usr/bin/env python3
import datetime
import hashlib
import logging
import uuid
from typing import Any

import asyncpg
import dateutil.parser
import httpx
from pydantic import BaseModel

import periodic_cleanup
from cards.render import render
from config import config
from db import database
from db import dbh
from gitlab_model import MergeRequestPayload

logger = logging.getLogger(__name__)


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
                ) RETURNING merge_request_message_ref_id,
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
        res = await client.request(
            "POST",
            config.ACTIVITY_API + "api/v1/message",
            json=payload,
        )
        response = res.json()

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
            # This case is a race condition so cleanup the second message :)
            await client.request(
                "DELETE",
                config.ACTIVITY_API + "api/v1/message",
                json={
                    "message_id": str(response.get("message_id")),
                },
            )
    else:
        payload["message_id"] = str(mrmsgref.message_id)
        res = await client.request(
            "PATCH",
            config.ACTIVITY_API + "api/v1/message",
            json=payload,
        )
        response = res.json()
    res.raise_for_status()
    return uuid.UUID(response.get("message_id"))


async def merge_request(
    mr: MergeRequestPayload,
    conversation_tokens: list[str],
    participant_ids_filter: list[int],
    new_commits_revoke_approvals: bool,
):
    payload_fingerprint = hashlib.sha256(mr.model_dump_json().encode("utf8")).hexdigest()
    logger.debug("payload fingerprint: %s", payload_fingerprint)
    mri = await dbh.get_merge_request_ref_infos(mr)

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
        # Update MR info (head_pipeline_id)
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

            # If update and oldrev field is set => new commit in MR
            # Approvals must be reset
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

            # if it's a transition from draft to ready
            # - Delete all messages related to this MR prior to the current event update
            # then create_or_update_message will re-post new message
            #  to have cards being the most recent in the feeds.
            # Rows are used as lock to avoid race condition when multiple instances can receive hooks
            # for the same MR (multiple webhook same project, multiple instances [kube?])
            if mr.changes and "draft" in mr.changes and not mr.object_attributes.draft:
                assert mr.object_attributes.updated_at is not None
                update_ref_datetime = dateutil.parser.parse(mr.object_attributes.updated_at)
                message_expiration = datetime.timedelta(seconds=0)
                async with connection.transaction():
                    res = await connection.fetch(
                        """SELECT merge_request_message_ref_id, message_id
                            FROM merge_request_message_ref
                            WHERE merge_request_ref_id = $1 AND created_at < $2
                            FOR UPDATE""",
                        mri.merge_request_ref_id,
                        update_ref_datetime,
                    )
                    for row in res:
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
                periodic_cleanup.reschedule()

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
    should_be_collapsed: bool = mr.object_attributes.draft or mr.object_attributes.work_in_progress
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

    convtoken_to_msgrefs = await get_or_create_message_refs(
        mri.merge_request_ref_id,
        conversation_tokens,
    )

    if mr.object_attributes.action in ("open", "reopen") or True:
        async with httpx.AsyncClient() as client:
            for ct in conversation_tokens:
                mrmsgref = convtoken_to_msgrefs[ct]
                mrmsgref.message_id = await create_or_update_message(
                    client,
                    mrmsgref,
                    card=card,
                    summary=summary,
                    update_only=mr.object_attributes.state
                    in (
                        "closed",
                        "merged",
                    )
                    or mr.object_attributes.draft
                    or mr.object_attributes.work_in_progress
                    or not participant_found,
                )

    if mr.object_attributes.action in (
        "merge",
        "close",
    ) or mr.object_attributes.state in (
        "closed",
        "merged",
    ):
        message_expiration = datetime.timedelta(seconds=30)
        async with await database.acquire() as connection:
            res = await connection.fetch(
                """SELECT merge_request_message_ref_id, message_id
                    FROM merge_request_message_ref
                    WHERE merge_request_ref_id = $1""",
                mri.merge_request_ref_id,
            )
            for row in res:
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
            if len(res):
                await connection.execute(
                    "DELETE FROM merge_request_ref WHERE merge_request_ref_id = $1",
                    mri.merge_request_ref_id,
                )
            periodic_cleanup.reschedule()

    return {
        "merge_request_infos": mri,
    }
