#!/usr/bin/env python3
import asyncio
import datetime

import asyncpg
import fastapi_structured_logging
import httpx

from cards.render import render
from config import DefaultConfig
from config import config
from db import DatabaseLifecycleHandler
from db import MergeRequestInfos
from db import compute_mri_fingerprint
from db import dbh
from db import has_unresolved_threads
from db import make_mr_summary
from gitlab_api import fetch_and_persist_discussion_stats
from gitlab_api import fetch_and_refresh_mr_status
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

            # Emoji events trigger a full MR status refresh via API
            # to sync state that may have been lost due to missed webhooks.
            # We do NOT short-circuit when stored state is already terminal:
            # the merge_request handler updates payload state and schedules
            # deletion in separate transactions, so a partial rollback can
            # leave state="merged" with refs still present. Letting the API
            # refresh + api-refresh-close branch run is the recovery for that.
            api_refreshed = False
            if row["payload_type"] == "emoji":
                refreshed_mri = await fetch_and_refresh_mr_status(
                    merge_request_ref_id=mri.merge_request_ref_id,
                    project_url=mri.merge_request_payload.project.web_url,
                    project_id=mri.merge_request_payload.object_attributes.target_project_id,
                    mr_iid=mri.merge_request_payload.object_attributes.iid,
                )
                if refreshed_mri is not None:
                    mri = refreshed_mri
                    api_refreshed = True
                    if mri.merge_request_payload.object_attributes.state in ("closed", "merged"):
                        logger.info(
                            "api refresh detected terminal state",
                            merge_request_ref_id=mri.merge_request_ref_id,
                            state=mri.merge_request_payload.object_attributes.state,
                        )
                else:
                    # None = couldn't verify (no token / HTTP error). Distinct
                    # from "API confirmed still open"; recurring emoji events
                    # on a closed MR will keep landing here until API succeeds.
                    logger.warning(
                        "api refresh unavailable for emoji refresh",
                        merge_request_ref_id=mri.merge_request_ref_id,
                        project_id=mri.merge_request_payload.object_attributes.target_project_id,
                        mr_iid=mri.merge_request_payload.object_attributes.iid,
                    )

            had_unresolved_threads = has_unresolved_threads(mri.merge_request_extra_state)

            updated_extra_state = await fetch_and_persist_discussion_stats(
                merge_request_ref_id=mri.merge_request_ref_id,
                project_url=mri.merge_request_payload.project.web_url,
                project_id=mri.merge_request_payload.object_attributes.target_project_id,
                mr_iid=mri.merge_request_payload.object_attributes.iid,
            )
            if updated_extra_state is not None:
                mri.merge_request_extra_state = updated_extra_state

            now_has_unresolved_threads = has_unresolved_threads(mri.merge_request_extra_state)

            is_closing_state = mri.merge_request_payload.object_attributes.state in ("closed", "merged")

            # Use GitLab's updated_at from stored payload (not local last_event_at)
            # to keep timestamps comparable for OOO detection
            payload_updated_at = datetime.datetime.fromisoformat(
                mri.merge_request_payload.object_attributes.updated_at.replace(" UTC", "+00:00")
            )

            if had_unresolved_threads and not now_has_unresolved_threads and not is_closing_state:
                datasource_fingerprint = compute_mri_fingerprint(mri)
                temp_card = render(mri, collapsed=False, show_collapsible=False)
                await update_all_messages_transactional(
                    mri,
                    temp_card,
                    make_mr_summary(mri),
                    datasource_fingerprint,
                    payload_updated_at,
                    "threads-resolved",
                    schedule_deletion=True,
                    deletion_delay=datetime.timedelta(seconds=0),
                )
                await dbh.delete_pending_refresh(mri.merge_request_ref_id)
                processed += 1
                logger.info(
                    "pending refresh processed - threads resolved, message re-emitted",
                    merge_request_ref_id=mri.merge_request_ref_id,
                    payload_type=row["payload_type"],
                    fingerprint=datasource_fingerprint[:16],
                )
                continue

            # API refresh detected closed/merged MR — schedule message deletion to clean up
            if api_refreshed and is_closing_state:
                datasource_fingerprint = compute_mri_fingerprint(mri)
                card = render(mri, collapsed=True, show_collapsible=True)
                await update_all_messages_transactional(
                    mri,
                    card,
                    make_mr_summary(mri),
                    datasource_fingerprint,
                    payload_updated_at,
                    "api-refresh-close",
                    schedule_deletion=True,
                    deletion_delay=datetime.timedelta(seconds=config.MESSAGE_DELETE_DELAY_SECONDS),
                )
                await dbh.delete_pending_refresh(mri.merge_request_ref_id)
                processed += 1
                logger.info(
                    "api refresh detected closed/merged MR, scheduled message deletion",
                    merge_request_ref_id=mri.merge_request_ref_id,
                    state=mri.merge_request_payload.object_attributes.state,
                )
                continue

            should_be_collapsed: bool = (
                mri.merge_request_payload.object_attributes.draft
                or mri.merge_request_payload.object_attributes.work_in_progress
                or is_closing_state
                or now_has_unresolved_threads
            )
            card = render(mri, collapsed=should_be_collapsed, show_collapsible=should_be_collapsed)
            datasource_fingerprint = compute_mri_fingerprint(mri)

            messages_updated = await update_all_messages_transactional(
                mri,
                card,
                make_mr_summary(mri),
                datasource_fingerprint,
                payload_updated_at,
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
