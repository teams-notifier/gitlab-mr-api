#!/usr/bin/env python3
import uuid
from typing import Any

import asyncpg
import httpx
from pydantic import BaseModel

from cards.render import render
from config import config
from db import database
from db import dbh
from gitlab_model import MergeRequestPayload


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
):
    mri = await dbh.get_merge_request_ref_infos(mr)
    convtoken_to_msgrefs = await get_or_create_message_refs(
        mri.merge_request_ref_id,
        conversation_tokens,
    )

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
        ...  # Update MR info (head_pipeline_id)
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
    card = render(mri)
    summary = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}\n"
        f"on {mri.merge_request_payload.project.path_with_namespace}"
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

    if mr.object_attributes.action in ("merge", "close") or mr.object_attributes.state in (
        "closed",
        "merged",
    ):

        async with await database.acquire() as connection:
            for ct in conversation_tokens:
                mrmsgref = convtoken_to_msgrefs[ct]
                if mrmsgref.message_id is None:
                    continue
                await connection.execute(
                    """INSERT INTO msg_to_delete
                        (message_id, expire_at)
                    VALUES
                        ($1, now()+'30 seconds'::INTERVAL)""",
                    str(mrmsgref.message_id),
                )
                await connection.execute(
                    "DELETE FROM merge_request_message_ref WHERE merge_request_message_ref_id = $1",
                    mrmsgref.merge_request_message_ref_id,
                )
            await connection.execute(
                "DELETE FROM merge_request_ref WHERE merge_request_ref_id = $1",
                mri.merge_request_ref_id,
            )

    return {
        "merge_request_infos": mri,
    }
