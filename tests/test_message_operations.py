#!/usr/bin/env python3
"""
Tests for race condition protection mechanisms.

These tests verify that protection mechanisms exist (locks, cleanup logic),
but don't test actual concurrent execution. See test_e2e_race_conditions.py
for real concurrency tests with a database.
"""
import uuid
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from webhook.messaging import MRMessRef


@pytest.mark.asyncio
async def test_merge_creates_single_deletion_record(mock_database, sample_merge_request_payload):
    """Test that a merge webhook creates exactly one deletion record per message."""
    connection = mock_database.connection

    message_id = uuid.uuid4()
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    messages = [
        {"merge_request_message_ref_id": 1, "message_id": message_id},
    ]

    connection.fetch.return_value = messages

    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=message_id,
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        from webhook.merge_request import merge_request

        mock_dbh.return_value = AsyncMock(
            merge_request_ref_id=100,
            merge_request_payload=sample_merge_request_payload,
            merge_request_extra_state=AsyncMock(opener=sample_merge_request_payload.user),
        )

        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    insert_calls = [
        call for call in connection.execute.call_args_list if "INSERT INTO msg_to_delete" in str(call)
    ]
    assert len(insert_calls) == 1


@pytest.mark.asyncio
async def test_update_existing_message_uses_patch(mock_database):
    """Test that updating an existing message uses PATCH request."""
    from webhook.messaging import create_or_update_message

    mrmsgref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=uuid.uuid4(),
        message_id=uuid.uuid4(),
    )

    client = AsyncMock()
    success_response = MagicMock()
    success_response.status_code = 200
    success_response.json.return_value = {"message_id": str(mrmsgref.message_id)}
    client.request = AsyncMock(return_value=success_response)

    with patch("webhook.merge_request.database", mock_database):
        result = await create_or_update_message(
            client=client,
            mrmsgref=mrmsgref,
            card={"type": "AdaptiveCard"},
            summary="Test",
        )

    assert result == mrmsgref.message_id
    assert client.request.call_count == 1

    call_args = client.request.call_args
    assert call_args[0][0] == "PATCH"


@pytest.mark.asyncio
async def test_get_or_create_returns_existing_and_new_refs(mock_database):
    """Test that get_or_create_message_refs returns existing refs and creates missing ones."""
    from webhook.messaging import get_or_create_message_refs

    connection = mock_database.connection

    existing_ref = {
        "merge_request_message_ref_id": 1,
        "conversation_token": uuid.UUID("11111111-1111-1111-1111-111111111111"),
        "message_id": None,
    }

    connection.fetch.return_value = [existing_ref]

    new_conv_token = str(uuid.UUID("22222222-2222-2222-2222-222222222222"))
    new_ref = {
        "merge_request_message_ref_id": 2,
        "conversation_token": uuid.UUID(new_conv_token),
        "message_id": None,
    }

    connection.fetchrow.return_value = new_ref

    with patch("webhook.messaging.database", mock_database):
        result = await get_or_create_message_refs(
            merge_request_ref_id=100,
            conv_tokens=[str(existing_ref["conversation_token"]), new_conv_token],
        )

    assert len(result) == 2
    assert str(existing_ref["conversation_token"]) in result
    assert new_conv_token in result


@pytest.mark.asyncio
async def test_duplicate_message_cleanup_on_db_conflict(mock_database):
    """Test that duplicate message is deleted when DB update returns NULL (conflict)."""
    from webhook.messaging import create_or_update_message

    connection = mock_database.connection
    connection.fetchrow.return_value = None

    create_response = MagicMock()
    create_response.status_code = 200
    create_response.json.return_value = {"message_id": str(uuid.uuid4())}

    delete_response = MagicMock()
    delete_response.status_code = 200

    client = AsyncMock()
    client.request = AsyncMock(side_effect=[create_response, delete_response])

    mrmsgref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=uuid.uuid4(),
        message_id=None,
    )

    with patch("webhook.messaging.database", mock_database):
        await create_or_update_message(
            client=client,
            mrmsgref=mrmsgref,
            card={"type": "AdaptiveCard"},
            summary="Test",
        )

    assert client.request.call_count == 2

    first_call = client.request.call_args_list[0]
    assert first_call[0][0] == "POST"

    second_call = client.request.call_args_list[1]
    assert second_call[0][0] == "DELETE"


@pytest.mark.asyncio
async def test_draft_transition_uses_for_update_lock(mock_database, sample_merge_request_payload):
    """Test that draft-to-ready transition uses FOR UPDATE to serialize access."""
    connection = mock_database.connection

    sample_merge_request_payload.object_attributes.action = "update"
    sample_merge_request_payload.object_attributes.state = "opened"
    sample_merge_request_payload.object_attributes.draft = False
    sample_merge_request_payload.changes = {"draft": {"previous": True, "current": False}}

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    message_id = uuid.uuid4()
    messages = [
        {"merge_request_message_ref_id": 1, "message_id": message_id},
    ]

    connection.fetch.return_value = messages
    connection.fetchrow.return_value = {"merge_request_extra_state": {}}

    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=message_id,
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        from webhook.merge_request import merge_request

        mock_dbh.return_value = AsyncMock(
            merge_request_ref_id=100,
            merge_request_payload=sample_merge_request_payload,
            merge_request_extra_state=AsyncMock(
                opener=sample_merge_request_payload.user,
                approvers={},
            ),
        )

        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    fetch_call = connection.fetch.call_args
    query = fetch_call[0][0]
    assert "FOR UPDATE" in query


@pytest.mark.asyncio
async def test_approval_updates_extra_state(mock_database, sample_merge_request_payload):
    """Test that approval action updates merge_request_extra_state."""
    connection = mock_database.connection

    sample_merge_request_payload.object_attributes.action = "approved"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    message_id = uuid.uuid4()

    connection.fetchrow.return_value = {
        "merge_request_extra_state": {
            "approvers": {"1": {"id": 1, "username": "user1", "status": "approved"}}
        }
    }

    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=message_id,
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        from webhook.merge_request import merge_request

        mock_dbh.return_value = AsyncMock(
            merge_request_ref_id=100,
            merge_request_payload=sample_merge_request_payload,
            merge_request_extra_state=AsyncMock(
                opener=sample_merge_request_payload.user,
                approvers={"1": {"id": 1, "username": "user1", "status": "approved"}},
            ),
        )

        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        await merge_request(
            mr=sample_merge_request_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    update_calls = [
        call for call in connection.fetchrow.call_args_list if "UPDATE merge_request_ref" in str(call)
    ]
    assert len(update_calls) > 0
