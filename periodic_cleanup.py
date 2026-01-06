#!/usr/bin/env python3
import asyncio
import datetime

import asyncpg
import fastapi_structured_logging
import httpx

from cards.render import render
from config import DefaultConfig
from db import DatabaseLifecycleHandler
from db import MergeRequestInfos
from db import compute_mri_fingerprint
from db import dbh
from gitlab_api import fetch_and_persist_discussion_stats
from webhook.messaging import update_all_messages_transactional


logger = fastapi_structured_logging.get_logger()

signal = asyncio.Event()
MAX_WAIT = 300


def reschedule():
    signal.set()


async def periodic_cleanup(config: DefaultConfig, database: DatabaseLifecycleHandler):
    return await _log_exception(_cleanup_task(config, database))


async def _process_pending_refreshes() -> int:
    """Process pending MR refreshes that have passed their debounce window."""
    pending = await dbh.get_pending_refreshes(limit=50)
    processed = 0

    for row in pending:
        try:
            mri = MergeRequestInfos(
                merge_request_ref_id=row["merge_request_ref_id"],
                merge_request_payload=row["merge_request_payload"],
                merge_request_extra_state=row["merge_request_extra_state"],
                head_pipeline_id=row["head_pipeline_id"],
            )

            updated_extra_state = await fetch_and_persist_discussion_stats(
                merge_request_ref_id=mri.merge_request_ref_id,
                project_url=mri.merge_request_payload.project.web_url,
                project_id=mri.merge_request_payload.object_attributes.target_project_id,
                mr_iid=mri.merge_request_payload.object_attributes.iid,
            )
            if updated_extra_state is not None:
                mri.merge_request_extra_state = updated_extra_state

            should_be_collapsed: bool = (
                mri.merge_request_payload.object_attributes.draft
                or mri.merge_request_payload.object_attributes.work_in_progress
                or mri.merge_request_payload.object_attributes.state in ("closed", "merged")
            )
            card = render(mri, collapsed=should_be_collapsed, show_collapsible=should_be_collapsed)
            datasource_fingerprint = compute_mri_fingerprint(mri)
            summary = (
                f"MR {mri.merge_request_payload.object_attributes.state}:"
                f" {mri.merge_request_payload.object_attributes.title}\n"
                f"on {mri.merge_request_payload.project.path_with_namespace}"
            )

            event_updated_at: datetime.datetime = row["last_event_at"]

            messages_updated = await update_all_messages_transactional(
                mri,
                card,
                summary,
                datasource_fingerprint,
                event_updated_at,
                row["payload_type"],
            )

            await dbh.delete_pending_refresh(mri.merge_request_ref_id)
            processed += 1

            logger.info(
                "pending refresh processed",
                merge_request_ref_id=mri.merge_request_ref_id,
                payload_type=row["payload_type"],
                messages_updated=messages_updated,
                fingerprint=datasource_fingerprint[:16],
            )
        except Exception as e:
            logger.error(
                "error processing pending refresh",
                merge_request_ref_id=row["merge_request_ref_id"],
                error_type=type(e).__name__,
                error_detail=str(e),
                exc_info=True,
            )

    return processed


async def _cleanup_task(config: DefaultConfig, database: DatabaseLifecycleHandler):
    timeout = httpx.Timeout(10.0, connect=5.0)
    client = httpx.AsyncClient(timeout=timeout)
    while True:
        wait_sec: float = MAX_WAIT
        try:
            refreshes_processed = await _process_pending_refreshes()
            if refreshes_processed > 0:
                logger.debug("processed pending refreshes", count=refreshes_processed)

            connection: asyncpg.Connection
            async with await database.acquire() as connection:
                stmt = await connection.prepare(
                    """SELECT msg_to_delete_id, message_id
                        FROM msg_to_delete
                        WHERE expire_at < NOW()
                        FOR UPDATE"""
                )
                async with connection.transaction():
                    async for record in stmt.cursor():
                        try:
                            res = await client.request(
                                "DELETE",
                                config.ACTIVITY_API + "api/v1/message",
                                json={
                                    "message_id": str(record["message_id"]),
                                },
                            )
                            if res.status_code not in (410, 200):
                                res.raise_for_status()
                            await connection.execute(
                                "DELETE FROM msg_to_delete WHERE msg_to_delete_id = $1",
                                record["msg_to_delete_id"],
                            )
                            logger.info("deleted message %s", record["msg_to_delete_id"])
                        except Exception as e:
                            logger.error(
                                "error processing deletion record",
                                msg_to_delete_id=record["msg_to_delete_id"],
                                error_type=type(e).__name__,
                                error_detail=str(e),
                                exc_info=True,
                            )

                min_delete = await connection.fetchval("SELECT min(expire_at) FROM msg_to_delete")
                min_refresh = await connection.fetchval("SELECT min(process_after) FROM pending_mr_refresh")

                if min_delete is not None:
                    wait_sec = min(
                        wait_sec, (min_delete - datetime.datetime.now(tz=datetime.UTC)).total_seconds()
                    )
                if min_refresh is not None:
                    wait_sec = min(
                        wait_sec, (min_refresh - datetime.datetime.now(tz=datetime.UTC)).total_seconds()
                    )
                wait_sec = max(0.1, wait_sec)
        except Exception as e:
            logger.error(
                "cleanup task error",
                error_type=type(e).__name__,
                error_detail=str(e),
                exc_info=True,
            )
        try:
            logger.debug(f"wait for signal or {wait_sec}s")
            await asyncio.wait_for(signal.wait(), wait_sec)
            signal.clear()
            logger.debug("signal received")
        except TimeoutError:
            logger.debug("wait_for timeout")


async def _log_exception(awaitable):
    try:
        return await awaitable
    except Exception as e:
        logger.error(
            "periodic cleanup unhandled exception",
            error_type=type(e).__name__,
            error_detail=str(e),
            exc_info=True,
        )
