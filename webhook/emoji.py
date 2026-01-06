#!/usr/bin/env python3
import asyncpg
import fastapi_structured_logging

import periodic_cleanup

from config import config
from db import EmojiEntry
from db import GitlabUser
from db import database
from db import dbh
from gitlab_model import EmojiPayload


logger = fastapi_structured_logging.get_logger()


async def emoji(
    emoji: EmojiPayload,
    conversation_tokens: list[str],
) -> dict[str, str]:
    """Handle emoji webhook events for MRs - persists state and queues for processing."""
    if emoji.object_attributes.awardable_type != "MergeRequest":
        return {"status": "skipped", "reason": "not_mr_emoji"}

    logger.info(
        "processing emoji hook",
        project_id=emoji.merge_request.target_project_id,
        merge_request_iid=emoji.merge_request.iid,
        object_kind=emoji.object_kind,
        event_type=emoji.event_type,
        emoji_name=emoji.object_attributes.name,
    )

    mri = await dbh.get_mri_from_url_pid_mriid(
        url=emoji.object_attributes.awarded_on_url,
        project_id=emoji.merge_request.target_project_id,
        mr_iid=emoji.merge_request.iid,
    )
    if mri is None:
        logger.debug(
            "no existing MR ref found for emoji event",
            project_id=emoji.merge_request.target_project_id,
            mr_iid=emoji.merge_request.iid,
        )
        return {"status": "skipped", "reason": "no_mr_ref"}

    key = f"{emoji.object_attributes.name}:{emoji.object_attributes.user_id}"
    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        res = await connection.fetchrow(
            """UPDATE merge_request_ref
                SET merge_request_extra_state = jsonb_set(merge_request_extra_state, $1, $2::jsonb)
                WHERE merge_request_ref_id = $3
                RETURNING merge_request_ref_id""",
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
        if res is None:
            return {"status": "skipped", "reason": "update_failed"}

    await dbh.upsert_pending_mr_refresh(
        mri.merge_request_ref_id,
        payload_type="emoji",
        debounce_seconds=config.EMOJI_DEBOUNCE_SECONDS,
    )
    periodic_cleanup.reschedule()

    logger.debug(
        "emoji event queued for processing",
        project_id=emoji.merge_request.target_project_id,
        mr_iid=emoji.merge_request.iid,
        merge_request_ref_id=mri.merge_request_ref_id,
        emoji_key=key,
    )

    return {"status": "queued"}
