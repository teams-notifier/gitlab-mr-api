#!/usr/bin/env python3
import fastapi_structured_logging

import periodic_cleanup

from config import config
from db import dbh
from gitlab_model import NotePayload


logger = fastapi_structured_logging.get_logger()


async def note(payload: NotePayload):
    """Handle note/comment webhook events for MRs - queues for processing."""
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

    await dbh.upsert_pending_mr_refresh(
        mri.merge_request_ref_id,
        payload_type="note",
        debounce_seconds=config.NOTE_DEBOUNCE_SECONDS,
    )
    periodic_cleanup.reschedule()

    logger.debug(
        "note event queued for processing",
        project_id=payload.project.id,
        mr_iid=payload.merge_request.iid,
        merge_request_ref_id=mri.merge_request_ref_id,
    )

    return {"status": "queued"}
