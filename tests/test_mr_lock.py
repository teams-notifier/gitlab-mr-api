#!/usr/bin/env python3
"""The per-MR lock: it polls without pinning a connection, and it degrades instead of failing."""

import asyncio

from unittest.mock import AsyncMock
from unittest.mock import patch

from webhook.merge_request import mr_lock


async def test_waits_by_polling_and_releases_the_connection_between_tries(mock_database):
    mock_db = mock_database
    mock_conn = mock_db.connection
    mock_conn.fetchval = AsyncMock(side_effect=[False, False, True])
    acquires = 0
    real_acquire = mock_db.acquire

    async def counting_acquire():
        nonlocal acquires
        acquires += 1
        return await real_acquire()

    mock_db.acquire = counting_acquire
    ran = False

    with patch("webhook.merge_request.database", mock_db):
        async with mr_lock(42, poll=0.001):
            ran = True

    assert ran
    assert mock_conn.fetchval.await_count == 3
    # one fresh acquire per attempt: a waiter never holds a connection while it sleeps
    assert acquires == 3
    assert mock_conn.fetchval.await_args_list[0].args == ("SELECT pg_try_advisory_xact_lock(1, $1::int)", 42)


async def test_runs_unlocked_after_the_deadline_instead_of_failing(mock_database):
    mock_db = mock_database
    mock_conn = mock_db.connection
    mock_conn.fetchval = AsyncMock(return_value=False)
    ran = False

    with (
        patch("webhook.merge_request.database", mock_db),
        patch("webhook.merge_request.logger") as mock_logger,
    ):
        async with mr_lock(42, timeout=0.05, poll=0.01):
            ran = True

    assert ran
    mock_logger.warning.assert_called_once()
    assert mock_logger.warning.call_args.args[0] == "merge request lock not acquired, proceeding unlocked"
    assert mock_logger.warning.call_args.kwargs["merge_request_ref_id"] == 42


async def test_holders_are_capped_by_the_handler_slots(mock_database):
    """More holders than slots must queue, not pile onto the pool."""
    mock_db = mock_database
    mock_db.handler_slots = asyncio.Semaphore(1)
    inside = 0
    peak = 0

    async def hold():
        nonlocal inside, peak
        async with mr_lock(7):
            inside += 1
            peak = max(peak, inside)
            await asyncio.sleep(0.01)
            inside -= 1

    # one patch around all three: concurrent patches of one attribute unpatch each other
    with patch("webhook.merge_request.database", mock_db):
        await asyncio.gather(hold(), hold(), hold())
    assert peak == 1
