#!/usr/bin/env python3
import datetime
import hashlib

import asyncpg
import fastapi_structured_logging
import httpx

import periodic_cleanup
from cards.render import render
from config import config
from db import database
from db import dbh
from gitlab_model import MergeRequestPayload
from webhook.messaging import create_or_update_message
from webhook.messaging import get_all_message_refs
from webhook.messaging import get_or_create_message_refs
from webhook.messaging import update_all_messages_transactional
from webhook.messaging import update_message_with_fingerprint

logger = fastapi_structured_logging.get_logger()


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

                await update_all_messages_transactional(
                    temp_mri,
                    temp_card,
                    temp_summary,
                    payload_fingerprint,
                    "draft-to-ready",
                    schedule_deletion=True,
                    deletion_delay=datetime.timedelta(seconds=0),
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
        await update_all_messages_transactional(
            mri,
            card,
            summary,
            payload_fingerprint,
            "close/merge",
            schedule_deletion=True,
            deletion_delay=datetime.timedelta(seconds=config.MESSAGE_DELETE_DELAY_SECONDS),
        )
        need_cleanup_reschedule = True
    else:
        await get_or_create_message_refs(
            mri.merge_request_ref_id,
            conversation_tokens,
        )

        all_message_refs = await get_all_message_refs(mri.merge_request_ref_id)

        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for mrmsgref in all_message_refs:
                if mrmsgref.last_processed_fingerprint == payload_fingerprint:
                    logger.debug(
                        "message ref already processed with this fingerprint - skipping",
                        merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                        fingerprint=payload_fingerprint,
                    )
                    continue

                if mrmsgref.message_id is None:
                    conv_token_str = str(mrmsgref.conversation_token)
                    if conv_token_str not in conversation_tokens:
                        logger.debug(
                            "message ref has no message_id, conv_token not in webhook - skipping",
                            merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                            conversation_token=conv_token_str,
                        )
                        continue

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

                    if mrmsgref.message_id is not None:
                        async with await database.acquire() as conn:
                            await conn.execute(
                                """UPDATE merge_request_message_ref
                                   SET message_id = $1, last_processed_fingerprint = $2
                                   WHERE merge_request_message_ref_id = $3""",
                                mrmsgref.message_id,
                                payload_fingerprint,
                                mrmsgref.merge_request_message_ref_id,
                            )
                else:
                    await update_message_with_fingerprint(
                        client,
                        mrmsgref,
                        card,
                        summary,
                        payload_fingerprint,
                    )

    if need_cleanup_reschedule:
        periodic_cleanup.reschedule()

    return {
        "merge_request_infos": mri,
    }
