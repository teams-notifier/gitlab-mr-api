#!/usr/bin/env python3
import fastapi_structured_logging
import httpx

from cards.render import render
from db import dbh
from gitlab_api import fetch_and_persist_discussion_stats
from gitlab_model import NotePayload
from webhook.messaging import create_or_update_message
from webhook.messaging import get_all_message_refs


logger = fastapi_structured_logging.get_logger()

NOTE_DEBOUNCE_SECONDS = 2.0


async def note(payload: NotePayload):
    """Handle note/comment webhook events for MRs."""
    if payload.merge_request is None:
        logger.debug("note event not for merge request, skipping")
        return {"status": "skipped", "reason": "not_mr_note"}

    if payload.object_attributes.noteable_type != "MergeRequest":
        logger.debug(
            "note event not for merge request type",
            noteable_type=payload.object_attributes.noteable_type,
        )
        return {"status": "skipped", "reason": "not_mr_note"}

    mri = await dbh.get_mri_from_url_pid_mriid(
        url=payload.merge_request.url,
        project_id=payload.project.id,
        mr_iid=payload.merge_request.iid,
    )
    if mri is None:
        logger.debug(
            "no existing MR ref found for note event",
            project_id=payload.project.id,
            mr_iid=payload.merge_request.iid,
        )
        return {"status": "skipped", "reason": "no_mr_ref"}

    is_first_event = await dbh.upsert_pending_mr_refresh(
        mri.merge_request_ref_id,
        payload_type="note",
        debounce_seconds=NOTE_DEBOUNCE_SECONDS,
    )

    if not is_first_event:
        logger.debug(
            "note event debounced - will process in catch-up",
            project_id=payload.project.id,
            mr_iid=payload.merge_request.iid,
        )
        return {"status": "debounced", "reason": "pending_catchup"}

    updated_extra_state = await fetch_and_persist_discussion_stats(
        merge_request_ref_id=mri.merge_request_ref_id,
        project_url=payload.project.web_url,
        project_id=payload.project.id,
        mr_iid=payload.merge_request.iid,
    )
    if updated_extra_state is not None:
        mri.merge_request_extra_state = updated_extra_state

    should_be_collapsed: bool = (
        mri.merge_request_payload.object_attributes.draft
        or mri.merge_request_payload.object_attributes.work_in_progress
        or mri.merge_request_payload.object_attributes.state in ("closed", "merged")
    )
    card = render(mri, collapsed=should_be_collapsed, show_collapsible=should_be_collapsed)
    summary = (
        f"MR {mri.merge_request_payload.object_attributes.state}:"
        f" {mri.merge_request_payload.object_attributes.title}\n"
        f"on {mri.merge_request_payload.project.path_with_namespace}"
    )

    all_message_refs = await get_all_message_refs(mri.merge_request_ref_id)
    messages_updated = 0

    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for mrmsgref in all_message_refs:
            if mrmsgref.message_id is None:
                continue
            try:
                await create_or_update_message(client, mrmsgref, card=card, summary=summary)
                messages_updated += 1
            except Exception:
                logger.warning(
                    "failed to update message on note event",
                    merge_request_message_ref_id=mrmsgref.merge_request_message_ref_id,
                    exc_info=True,
                )

    ds = mri.merge_request_extra_state.discussion_stats
    logger.info(
        "note event processed",
        project_id=payload.project.id,
        mr_iid=payload.merge_request.iid,
        messages_updated=messages_updated,
        threads_total=ds.threads_total if ds else 0,
        threads_resolved=ds.threads_resolved if ds else 0,
        threads_unresolved=ds.threads_unresolved if ds else 0,
    )

    return {
        "status": "ok",
        "messages_updated": messages_updated,
    }
