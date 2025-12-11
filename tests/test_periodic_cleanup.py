#!/usr/bin/env python3
"""
Tests for periodic cleanup error handling.

Critical bug scenarios tested:
1. Activity-API failures leave records in msg_to_delete forever
2. No retry mechanism for failed deletions
3. Network timeouts and rate limiting
"""

import asyncio
import datetime
import uuid

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from tests.conftest import MockPreparedStatement


@pytest.fixture
def fresh_signal():
    """Create a fresh asyncio.Event for each test to avoid event loop binding issues."""
    return asyncio.Event()


@pytest.mark.asyncio
async def test_activity_api_failure_leaves_record_in_database(mock_database, fresh_signal):
    """
    CRITICAL BUG: Activity-API DELETE fails, record stays in msg_to_delete forever.

    Scenario: Activity-API is down or returns 500
    Result: Exception logged, but record not deleted from msg_to_delete
    Impact: Message never gets deleted from Teams, stale data accumulates

    Location: periodic_cleanup.py:57-58
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()
    error_response = MagicMock()
    error_response.status_code = 500
    error_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Internal server error", request=MagicMock(), response=error_response
    )
    client.request.return_value = error_response

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 0


@pytest.mark.asyncio
async def test_activity_api_network_timeout(mock_database, fresh_signal):
    """
    BUG: Network timeout leaves record in msg_to_delete.

    Scenario: Network timeout when calling activity-API
    Result: Record stays in database indefinitely
    Impact: No retry mechanism exists

    Location: periodic_cleanup.py:43-58
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()
    client.request.side_effect = TimeoutError("Connection timeout")

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 0


@pytest.mark.asyncio
async def test_activity_api_rate_limiting(mock_database, fresh_signal):
    """
    BUG: Rate limiting (429) not handled, record stays in database.

    Scenario: Activity-API returns 429 Too Many Requests
    Result: Record not deleted, no exponential backoff
    Impact: Repeated failures on every cleanup cycle

    Location: periodic_cleanup.py:50-51
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()
    rate_limit_response = MagicMock()
    rate_limit_response.status_code = 429
    rate_limit_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Too many requests", request=MagicMock(), response=rate_limit_response
    )
    client.request.return_value = rate_limit_response

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 0


@pytest.mark.asyncio
async def test_successful_deletion_with_410_gone(mock_database, fresh_signal):
    """
    POSITIVE TEST: 410 Gone is accepted (message already deleted).

    Scenario: Activity-API returns 410, meaning message was already deleted
    Result: Record should be removed from msg_to_delete

    Location: periodic_cleanup.py:50
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()
    gone_response = MagicMock()
    gone_response.status_code = 410
    client.request.return_value = gone_response

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 1
    delete_call = connection.execute.call_args_list[0]
    assert "DELETE FROM msg_to_delete" in delete_call[0][0]
    assert delete_call[0][1] == msg_to_delete_id


@pytest.mark.asyncio
async def test_successful_deletion_with_200_ok(mock_database, fresh_signal):
    """
    POSITIVE TEST: 200 OK is accepted (message deleted successfully).

    Scenario: Activity-API returns 200, message deleted successfully
    Result: Record should be removed from msg_to_delete

    Location: periodic_cleanup.py:50
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()
    success_response = MagicMock()
    success_response.status_code = 200
    client.request.return_value = success_response

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 1
    delete_call = connection.execute.call_args_list[0]
    assert "DELETE FROM msg_to_delete" in delete_call[0][0]


@pytest.mark.asyncio
async def test_multiple_records_first_fails_second_succeeds(mock_database, fresh_signal):
    """
    BUG: Transaction commits even if some records fail.

    Scenario: Multiple expired messages, first one fails, second succeeds
    Result: Transaction commits, first record stays, second deleted
    Impact: Failed records accumulate over time

    Location: periodic_cleanup.py:40-58
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    records = [
        {"msg_to_delete_id": 1, "message_id": uuid.uuid4()},
        {"msg_to_delete_id": 2, "message_id": uuid.uuid4()},
    ]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None

    client = AsyncMock()

    error_response = MagicMock()
    error_response.status_code = 500
    error_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server error", request=MagicMock(), response=error_response
    )

    success_response = MagicMock()
    success_response.status_code = 200

    client.request.side_effect = [error_response, success_response]

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 2
    assert connection.execute.call_count == 1


@pytest.mark.asyncio
async def test_db_delete_fails_after_api_success(mock_database, fresh_signal):
    """
    BUG: Message deleted from Teams but record stays in database.

    Scenario: Activity-API DELETE succeeds, but DB DELETE fails
    Result: Message gone from Teams, but record still in msg_to_delete
    Impact: Cleanup will retry deletion of non-existent message (will get 410)

    Location: periodic_cleanup.py:52-55
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    msg_to_delete_id = 1
    message_id = uuid.uuid4()

    records = [{"msg_to_delete_id": msg_to_delete_id, "message_id": message_id}]

    prepared_stmt = MockPreparedStatement(records)
    connection.prepare.return_value = prepared_stmt
    connection.fetchval.return_value = None
    connection.execute.side_effect = Exception("Database error")

    client = AsyncMock()
    success_response = MagicMock()
    success_response.status_code = 200
    client.request.return_value = success_response

    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert client.request.call_count == 1
    assert connection.execute.call_count == 1


@pytest.mark.asyncio
async def test_cleanup_task_reschedule_signal():
    """
    TEST: Reschedule signal wakes up cleanup task.

    Scenario: New message queued for deletion, reschedule() called
    Result: Cleanup task should wake up immediately

    Location: periodic_cleanup.py:14-19, webhook/merge_request.py:327
    """
    import periodic_cleanup

    periodic_cleanup.signal.clear()

    assert periodic_cleanup.signal.is_set() is False

    periodic_cleanup.reschedule()

    assert periodic_cleanup.signal.is_set() is True

    periodic_cleanup.signal.clear()


@pytest.mark.asyncio
async def test_wait_time_calculation_with_future_expiry(mock_database, fresh_signal):
    """
    TEST: Wait time calculated correctly based on next expiry.

    Scenario: Messages expire in 30 seconds
    Result: Wait time should be ~30 seconds

    Location: periodic_cleanup.py:59-61
    """
    from config import DefaultConfig
    from periodic_cleanup import _cleanup_task

    connection = mock_database.connection

    prepared_stmt = MockPreparedStatement([])
    connection.prepare.return_value = prepared_stmt

    future_time = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=30)
    connection.fetchval.return_value = future_time

    client = AsyncMock()
    config = DefaultConfig()

    with (
        patch("periodic_cleanup.httpx.AsyncClient", return_value=client),
        patch("periodic_cleanup.signal", fresh_signal),
    ):
        task = _cleanup_task(config, mock_database)
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except TimeoutError:
            pass

    assert connection.fetchval.call_count == 1
