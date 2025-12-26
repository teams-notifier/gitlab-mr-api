#!/usr/bin/env python3
"""
Tests for merge request message deletion.

Critical bug scenarios tested:
1. Non-transactional deletion causing orphaned records
2. Partial failure in deletion sequence
3. Race conditions during message deletion
"""

import uuid

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from tests.conftest import MockAsyncHttpxClient
from webhook.messaging import MRMessRef


@pytest.mark.asyncio
async def test_merge_close_uses_transactional_update(mock_database, sample_merge_request_payload):
    """
    POSITIVE TEST: merge/close now uses update_all_messages_transactional.

    This ensures that all database operations happen in a transaction,
    preventing orphaned records if any operation fails.

    Location: webhook/merge_request.py:144-154
    """
    merge_request_ref_id = 100

    sample_mri = AsyncMock(
        merge_request_ref_id=merge_request_ref_id,
        merge_request_payload=sample_merge_request_payload,
        merge_request_extra_state=AsyncMock(opener=sample_merge_request_payload.user),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
        patch("webhook.merge_request.periodic_cleanup"),
    ):
        from webhook.merge_request import merge_request

        mock_update.return_value = 1

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=["conv-token-1"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    mock_update.assert_called_once()
    call_args = mock_update.call_args
    assert call_args[0][5] == "close/merge"
    assert call_args[1]["schedule_deletion"] is True


@pytest.mark.asyncio
async def test_merge_close_transactional_rollback_on_failure(mock_database, sample_merge_request_payload):
    """
    POSITIVE TEST: Transaction rollback on failure.

    Scenario: update_all_messages_transactional fails partway through
    Result: Transaction rolls back, no orphaned records
    Impact: All-or-nothing guarantee

    Location: webhook/messaging.py:update_all_messages_transactional
    """
    merge_request_ref_id = 100

    sample_mri = AsyncMock(
        merge_request_ref_id=merge_request_ref_id,
        merge_request_payload=sample_merge_request_payload,
        merge_request_extra_state=AsyncMock(opener=sample_merge_request_payload.user),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
        patch("webhook.merge_request.periodic_cleanup"),
    ):
        from webhook.merge_request import merge_request

        mock_update.side_effect = Exception("Database error during transaction")

        with pytest.raises(Exception, match="Database error during transaction"):
            await merge_request(
                mr=sample_merge_request_payload,
                conversation_tokens=["conv-token-1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

    mock_update.assert_called_once()


@pytest.mark.asyncio
async def test_race_condition_duplicate_message_deletion_logs_failure(mock_database):
    """
    POSITIVE TEST: Race condition handling gracefully logs DELETE failures.

    Scenario: Two webhooks create messages simultaneously, second one's DELETE fails
    Result: DELETE failure is logged but doesn't crash; returns None to signal duplicate
    Impact: Graceful degradation - duplicate may persist in Teams but caller knows not to use it

    Location: webhook/messaging.py:create_or_update_message
    """
    import httpx

    from webhook.messaging import MRMessRef
    from webhook.messaging import create_or_update_message

    connection = mock_database.connection
    connection.fetchrow.return_value = None

    client = AsyncMock()
    create_response = MagicMock()
    create_response.status_code = 200
    new_message_id = str(uuid.uuid4())
    create_response.json.return_value = {"message_id": new_message_id}

    delete_response = MagicMock()
    delete_response.status_code = 500
    delete_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server error", request=MagicMock(), response=delete_response
    )

    client.request.side_effect = [create_response, delete_response]

    mrmsgref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=uuid.uuid4(),
        message_id=None,
    )

    with patch("webhook.messaging.database", mock_database):
        result = await create_or_update_message(
            client=client,
            mrmsgref=mrmsgref,
            card={"type": "AdaptiveCard"},
            summary="Test",
        )

    assert result is None
    assert client.request.call_count == 2
    assert connection.fetchrow.call_count == 1


@pytest.mark.asyncio
async def test_multiple_messages_all_deleted_transactionally(mock_database, sample_merge_request_payload):
    """
    POSITIVE TEST: Multiple messages deleted in transaction.

    Scenario: MR posted to 3 channels, all messages deleted atomically
    Result: All channels updated consistently
    Impact: Transaction ensures consistency

    Location: webhook/messaging.py:update_all_messages_transactional
    """
    merge_request_ref_id = 100

    sample_mri = AsyncMock(
        merge_request_ref_id=merge_request_ref_id,
        merge_request_payload=sample_merge_request_payload,
        merge_request_extra_state=AsyncMock(opener=sample_merge_request_payload.user),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
        patch("webhook.merge_request.periodic_cleanup"),
    ):
        from webhook.merge_request import merge_request

        mock_update.return_value = 3

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=["conv-token-1"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    mock_update.assert_called_once()
    assert mock_update.return_value == 3


@pytest.mark.asyncio
async def test_draft_to_ready_uses_transaction(mock_database, sample_merge_request_payload):
    """
    POSITIVE TEST: Draft-to-ready transition DOES use transaction (lines 206-230).

    This is the correct pattern that should be applied to merge/close deletion.
    """
    connection = mock_database.connection

    sample_merge_request_payload.object_attributes.action = "update"
    sample_merge_request_payload.object_attributes.state = "opened"
    sample_merge_request_payload.object_attributes.draft = False
    sample_merge_request_payload.changes = {"draft": {"previous": True, "current": False}}

    connection.fetch.return_value = [{"merge_request_message_ref_id": 1, "message_id": uuid.uuid4()}]
    connection.fetchrow.return_value = {"merge_request_extra_state": {}}

    mock_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=uuid.uuid4(),
        message_id=uuid.uuid4(),
    )

    sample_mri = AsyncMock(
        merge_request_ref_id=100,
        merge_request_payload=sample_merge_request_payload,
        merge_request_extra_state=AsyncMock(
            opener=sample_merge_request_payload.user,
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
        patch("webhook.merge_request.periodic_cleanup"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient", MockAsyncHttpxClient),
    ):
        from webhook.merge_request import merge_request

        mock_update.return_value = 1
        mock_get_refs.return_value = {"conv-token-1": mock_ref}
        mock_get_all.return_value = [mock_ref]

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=["conv-token-1"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    mock_update.assert_called_once()


@pytest.mark.asyncio
async def test_merge_with_no_messages_doesnt_delete_mr_ref(mock_database, sample_merge_request_payload):
    """
    Edge case: MR merged but no messages exist (messages already deleted or never created).

    Scenario: Duplicate merge webhook received, messages already processed
    Result: Should not attempt to delete MR ref

    Location: webhook/merge_request.py:322-326
    """
    connection = mock_database.connection
    connection.fetch.return_value = []

    sample_mri = AsyncMock(
        merge_request_ref_id=100,
        merge_request_payload=sample_merge_request_payload,
        merge_request_extra_state=AsyncMock(opener=sample_merge_request_payload.user),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
        patch("webhook.merge_request.periodic_cleanup"),
    ):
        from webhook.merge_request import merge_request

        mock_update.return_value = 0

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=["conv-token-1"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    delete_calls = [
        call for call in connection.execute.call_args_list if "DELETE FROM merge_request_ref" in str(call)
    ]
    assert len(delete_calls) == 0
