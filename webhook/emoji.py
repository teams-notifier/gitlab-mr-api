#!/usr/bin/env python3
import asyncpg
import httpx

from cards.render import render
from db import database
from db import dbh
from db import EmojiEntry
from db import GitlabUser
from db import MergeRequestInfos
from gitlab_model import EmojiPayload
from webhook.merge_request import create_or_update_message
from webhook.merge_request import MRMessRef


async def emoji(
    emoji: EmojiPayload,
    conversation_tokens: list[str],
) -> MergeRequestInfos | None:
    if emoji.object_attributes.awardable_type != "MergeRequest":
        return None

    mri = await dbh.get_mri_from_url_pid_mriid(
        url=emoji.object_attributes.awarded_on_url,
        project_id=emoji.merge_request.target_project_id,
        mr_iid=emoji.merge_request.iid,
    )
    if mri is None:
        return None
    key = f"{emoji.object_attributes.name}:{emoji.object_attributes.user_id}"
    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        res = await connection.fetchrow(
            """UPDATE merge_request_ref
                SET merge_request_extra_state = jsonb_set(merge_request_extra_state, $1, $2::jsonb)
                WHERE merge_request_ref_id = $3
                RETURNING merge_request_ref_id, merge_request_payload,
                            merge_request_extra_state, head_pipeline_id""",
            ["emojis", key],
            EmojiEntry(
                event_type=emoji.event_type,
                object_attributes=emoji.object_attributes,
                object_kind=emoji.object_kind,
                user=GitlabUser(
                    id=emoji.user.id,
                    name=emoji.user.name,
                    username=emoji.user.username,
                ),
            ).model_dump(),
            mri.merge_request_ref_id,
        )
        if res is not None:
            mri = MergeRequestInfos(**res)
            await update_message(mri, conversation_tokens)
            return mri
    return None


async def update_message(mri: MergeRequestInfos, conversation_tokens: list[str]):
    card = render(mri)
    summary = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}\n"
        f"on {mri.merge_request_payload.project.path_with_namespace}"
    )

    connection: asyncpg.Connection
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with await database.acquire() as connection, httpx.AsyncClient(timeout=timeout) as client:
        res = await connection.fetch(
            """
            SELECT merge_request_message_ref_id, conversation_token, message_id
            FROM merge_request_message_ref
            WHERE merge_request_ref_id = $1
            """,
            mri.merge_request_ref_id,
        )
        for row in res:
            if row["message_id"] is None:
                continue
            await create_or_update_message(
                client,
                MRMessRef(**row),
                #  message_text=message_text,
                card=card,
                summary=summary,
                update_only=True,
            )
