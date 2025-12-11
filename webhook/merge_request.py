#!/usr/bin/env python3
import datetime
import hashlib

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


class PartialMessageUpdateError(Exception):
    """Raised when some but not all messages were updated successfully."""

    def __init__(self, total: int, failed: int, message: str = ""):
        self.total = total
        self.failed = failed
        self.succeeded = total - failed
        super().__init__(message or f"{failed}/{total} message updates failed")


async def merge_request(
    mr: MergeRequestPayload,
    conversation_tokens: list[str],
    participant_ids_filter: list[int],
    new_commits_revoke_approvals: bool,
):
    payload_fingerprint = hashlib.sha256(mr.model_dump_json().encode("utf8")).hexdigest()
    if mr.object_attributes.updated_at:
        payload_updated_at = datetime.datetime.fromisoformat(
            mr.object_attributes.updated_at.replace(" UTC", "+00:00")
        )
    else:
        payload_updated_at = datetime.datetime.now(datetime.UTC)
    logger.info(
        "processing merge_request hook",
        project_id=mr.object_attributes.target_project_id,
        merge_request_iid=mr.object_attributes.iid,
        object_kind=mr.object_kind,
        fingerprint=payload_fingerprint,
        updated_at=payload_updated_at.isoformat(),
    )

    is_closing_action = mr.object_attributes.action in ("merge", "close") or mr.object_attributes.state in (
        "closed",
        "merged",
    )

    # Phase 1: Get/create MR ref ID WITHOUT updating payload
    # This prevents payload corruption from OOO events
    merge_request_ref_id = await dbh.get_or_create_merge_request_ref_id(mr)

    # Early out-of-order check
    # Reopen bypasses OOO: recreates messages after close, or updates existing if close arrives late
    # Closing actions check OOO: late close must not delete a reopened MR's messages
    is_reopen_action = mr.object_attributes.action == "reopen"
    if not is_reopen_action:
        async with await database.acquire() as connection:
            row = await connection.fetchrow(
                """SELECT MAX(last_processed_updated_at) as max_updated_at
                   FROM merge_request_message_ref
                   WHERE merge_request_ref_id = $1""",
                merge_request_ref_id,
            )
            if row and row.get("max_updated_at") is not None:
                if payload_updated_at < row["max_updated_at"]:
                    logger.warning(
                        "skipping entire out of order event",
                        merge_request_ref_id=merge_request_ref_id,
                        payload_updated_at=payload_updated_at.isoformat(),
                        stored_max_updated_at=row["max_updated_at"].isoformat(),
                    )
                    return
                # Same timestamp implies same fingerprint (fingerprint includes updated_at)
                # so duplicate detection is already handled by fingerprint check later

    # Phase 2: OOO check passed - now safe to update payload
    mri = await dbh.update_merge_request_ref_payload(merge_request_ref_id, mr)

    need_cleanup_reschedule = False

    participant_found = True
    if participant_ids_filter:
        participant_found = False
        participant_found |= mri.merge_request_extra_state.opener.id in participant_ids_filter
        participant_found |= any(
            user.id in participant_ids_filter
            for userlist in (mri.merge_request_payload.assignees, mri.merge_request_payload.reviewers)
            for user in userlist
        )

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
                    payload_updated_at,
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
            payload_updated_at,
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

        messages_processed = 0
        messages_failed = 0

        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for mrmsgref in all_message_refs:
                if mrmsgref.last_processed_updated_at is not None:
                    if payload_updated_at < mrmsgref.last_processed_updated_at:
                        logger.warning(
                            "skipping out of order event",
                            merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                            payload_updated_at=payload_updated_at.isoformat(),
                            stored_updated_at=mrmsgref.last_processed_updated_at.isoformat(),
                        )
                        continue
                    if (
                        payload_updated_at == mrmsgref.last_processed_updated_at
                        and mrmsgref.last_processed_fingerprint == payload_fingerprint
                    ):
                        logger.debug(
                            "message ref already processed - same timestamp and fingerprint",
                            merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                            fingerprint=payload_fingerprint,
                        )
                        continue

                try:
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
                                       SET message_id = $1, last_processed_fingerprint = $2,
                                           last_processed_updated_at = $3
                                       WHERE merge_request_message_ref_id = $4
                                         AND (last_processed_updated_at IS NULL
                                              OR last_processed_updated_at < $3)""",
                                    mrmsgref.message_id,
                                    payload_fingerprint,
                                    payload_updated_at,
                                    mrmsgref.merge_request_message_ref_id,
                                )
                    else:
                        await update_message_with_fingerprint(
                            client,
                            mrmsgref,
                            card,
                            summary,
                            payload_fingerprint,
                            payload_updated_at,
                        )
                    messages_processed += 1
                except Exception:
                    messages_failed += 1
                    logger.error(
                        "failed to process message, continuing to next",
                        merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                        conversation_token=str(mrmsgref.conversation_token),
                        exc_info=True,
                    )

        if messages_failed > 0:
            raise PartialMessageUpdateError(
                total=messages_processed + messages_failed,
                failed=messages_failed,
            )

    if need_cleanup_reschedule:
        periodic_cleanup.reschedule()

    return {
        "merge_request_infos": mri,
    }
