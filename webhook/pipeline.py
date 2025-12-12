#!/usr/bin/env python3
import hashlib

import asyncpg
import fastapi_structured_logging

from cards.render import render
from db import MergeRequestInfos
from db import database
from gitlab_model import PipelinePayload
from webhook.messaging import update_all_messages_transactional


logger = fastapi_structured_logging.get_logger()


async def pipeline(
    pipeline: PipelinePayload,
    conversation_tokens: list[str],
) -> MergeRequestInfos | None:
    payload_fingerprint = hashlib.sha256(pipeline.model_dump_json().encode("utf8")).hexdigest()
    logger.info(
        "processing pipeline hook",
        project_id=pipeline.project.id,
        pipeline_id=pipeline.object_attributes.id,
        object_kind=pipeline.object_kind,
        fingerprint=payload_fingerprint,
    )

    connection: asyncpg.Connection
    async with await database.acquire() as connection:
        res = await connection.fetchrow(
            """UPDATE merge_request_ref
                SET merge_request_extra_state = jsonb_set(merge_request_extra_state, $1, $2::jsonb)
                WHERE head_pipeline_id = $3
                RETURNING merge_request_ref_id, merge_request_payload,
                            merge_request_extra_state, head_pipeline_id""",
            ["pipeline_statuses", str(pipeline.object_attributes.id)],
            pipeline.model_dump(),
            pipeline.object_attributes.id,
        )
        if res is not None:
            mri = MergeRequestInfos(**res)
            card = render(mri)
            summary = (
                f"MR {mri.merge_request_payload.object_attributes.state}:"
                f" {mri.merge_request_payload.object_attributes.title}\n"
                f"on {mri.merge_request_payload.project.path_with_namespace}"
            )
            await update_all_messages_transactional(
                mri,
                card,
                summary,
                payload_fingerprint,
                None,
                "pipeline",
            )
            return mri
    return None
