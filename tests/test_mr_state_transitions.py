#!/usr/bin/env python3
"""
Tests for different MR states and transitions.

Tests various MR lifecycle events:
- open, reopen, update, approve, unapproved, merge, close
- draft transitions
- pipeline updates
- approval resets on new commits
"""

import uuid

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from webhook.messaging import MRMessRef


@pytest.fixture
def base_mr_payload(sample_merge_request_payload):
    """Base MR payload for state tests."""
    return sample_merge_request_payload


def make_mock_mri(payload, ref_id=1):
    """Create a mock MergeRequestInfos object."""
    mri = MagicMock()
    mri.merge_request_ref_id = ref_id
    mri.merge_request_payload = payload
    mri.merge_request_extra_state = MagicMock()
    mri.merge_request_extra_state.opener = payload.user
    mri.merge_request_extra_state.approvers = {}
    mri.merge_request_extra_state.discussion_stats = None
    return mri


@pytest.mark.asyncio
async def test_mr_state_open_creates_message(base_mr_payload, mock_database):
    """Test that opening MR creates a new message."""
    base_mr_payload.object_attributes.action = "open"
    base_mr_payload.object_attributes.state = "opened"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "POST"


@pytest.mark.asyncio
async def test_mr_state_draft_renders_collapsed(base_mr_payload, mock_database):
    """Test that draft MR is rendered collapsed."""
    base_mr_payload.object_attributes.action = "open"
    base_mr_payload.object_attributes.state = "opened"
    base_mr_payload.object_attributes.draft = True

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        render_call = mock_render.call_args
        assert render_call[1]["collapsed"] is True
        assert render_call[1]["show_collapsible"] is True


@pytest.mark.asyncio
async def test_mr_state_merged_renders_collapsed(base_mr_payload, mock_database):
    """Test that merged MR is rendered collapsed."""
    base_mr_payload.object_attributes.action = "merge"
    base_mr_payload.object_attributes.state = "merged"

    connection = mock_database.connection
    connection.fetch.return_value = [
        {"merge_request_message_ref_id": 1, "conversation_token": uuid.uuid4(), "message_id": uuid.uuid4()}
    ]

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.periodic_cleanup"),
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=["11111111-1111-1111-1111-111111111111"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        render_call = mock_render.call_args
        assert render_call[1]["collapsed"] is True
        assert render_call[1]["show_collapsible"] is True


@pytest.mark.asyncio
async def test_mr_state_closed_triggers_deletion(base_mr_payload, mock_database):
    """Test that closing MR triggers deletion flow."""
    base_mr_payload.object_attributes.action = "close"
    base_mr_payload.object_attributes.state = "closed"

    connection = mock_database.connection
    message_id = uuid.uuid4()
    connection.fetch.return_value = [
        {"merge_request_message_ref_id": 1, "conversation_token": uuid.uuid4(), "message_id": message_id}
    ]

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=["11111111-1111-1111-1111-111111111111"],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        insert_calls = [
            call for call in connection.execute.call_args_list if "INSERT INTO msg_to_delete" in str(call)
        ]
        assert len(insert_calls) == 1

        assert mock_cleanup.reschedule.called


@pytest.mark.asyncio
async def test_mr_state_approved_updates_approvers(base_mr_payload, mock_database):
    """Test that approval updates approvers in extra_state."""
    base_mr_payload.object_attributes.action = "approved"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {
        "merge_request_extra_state": MagicMock(
            approvers={"1": {"id": 1, "status": "approved"}}, discussion_stats=None
        )
    }

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        update_calls = [
            call for call in connection.fetchrow.call_args_list if "UPDATE merge_request_ref" in str(call)
        ]
        assert len(update_calls) > 0


@pytest.mark.asyncio
async def test_mr_state_unapproved_updates_approvers(base_mr_payload, mock_database):
    """Test that unapproval updates approvers in extra_state."""
    base_mr_payload.object_attributes.action = "unapproved"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {
        "merge_request_extra_state": MagicMock(
            approvers={"1": {"id": 1, "status": "unapproved"}}, discussion_stats=None
        )
    }

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        update_calls = [
            call for call in connection.fetchrow.call_args_list if "UPDATE merge_request_ref" in str(call)
        ]
        assert len(update_calls) > 0


@pytest.mark.asyncio
async def test_mr_update_with_new_commits_resets_approvals(base_mr_payload, mock_database):
    """Test that new commits reset approvals when flag is enabled."""
    base_mr_payload.object_attributes.action = "update"
    base_mr_payload.object_attributes.oldrev = "abc123"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {
        "merge_request_extra_state": MagicMock(approvers={}, discussion_stats=None)
    }

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=True,
        )

        update_calls = [
            call
            for call in connection.fetchrow.call_args_list
            if "jsonb_set" in str(call) and "approvers" in str(call)
        ]
        assert len(update_calls) > 0


@pytest.mark.asyncio
async def test_mr_draft_to_ready_deletes_old_messages(base_mr_payload, mock_database):
    """Test that draft-to-ready transition deletes old messages."""
    base_mr_payload.object_attributes.action = "update"
    base_mr_payload.object_attributes.state = "opened"
    base_mr_payload.object_attributes.draft = False
    base_mr_payload.object_attributes.updated_at = "2025-01-01T12:00:00Z"
    base_mr_payload.changes = {"draft": {"previous": True, "current": False}}

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    connection = mock_database.connection
    connection.fetch.return_value = [
        {
            "merge_request_message_ref_id": 1,
            "conversation_token": conv_token,
            "message_id": msg_ref.message_id,
        }
    ]
    connection.fetchrow.return_value = {"merge_request_extra_state": MagicMock(discussion_stats=None)}

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        fetch_calls = [call for call in connection.fetch.call_args_list if "FOR UPDATE" in str(call)]
        assert len(fetch_calls) == 1

        insert_calls = [
            call for call in connection.execute.call_args_list if "INSERT INTO msg_to_delete" in str(call)
        ]
        assert len(insert_calls) == 1

        assert mock_cleanup.reschedule.called


@pytest.mark.asyncio
async def test_mr_reopen_creates_message(base_mr_payload, mock_database):
    """Test that reopening MR creates a new message."""
    base_mr_payload.object_attributes.action = "reopen"
    base_mr_payload.object_attributes.state = "opened"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "POST"


@pytest.mark.asyncio
async def test_mr_update_existing_message_uses_patch(base_mr_payload, mock_database):
    """Test that updating existing MR uses PATCH instead of POST."""
    base_mr_payload.object_attributes.action = "update"
    base_mr_payload.object_attributes.state = "opened"

    existing_message_id = uuid.uuid4()
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=existing_message_id,
    )

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(existing_message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count == 1
        call_args = mock_client.request.call_args
        assert call_args[0][0] == "PATCH"


@pytest.mark.asyncio
async def test_threads_resolved_triggers_reemission(base_mr_payload, mock_database):
    """Test that resolving all threads triggers message re-emission."""
    base_mr_payload.object_attributes.action = "update"
    base_mr_payload.object_attributes.state = "opened"
    base_mr_payload.object_attributes.draft = False
    base_mr_payload.object_attributes.updated_at = "2025-01-01T12:00:00Z"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    connection = mock_database.connection
    connection.fetch.return_value = [
        {
            "merge_request_message_ref_id": 1,
            "conversation_token": conv_token,
            "message_id": msg_ref.message_id,
        }
    ]

    sample_mri = make_mock_mri(base_mr_payload)

    call_count = [0]

    def mock_has_unresolved(extra_state):
        call_count[0] += 1
        return call_count[0] == 1

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.dbh.any_message_needs_update", return_value=True),
        patch("webhook.merge_request.fetch_and_persist_discussion_stats", return_value=None),
        patch("webhook.merge_request.has_unresolved_threads", side_effect=mock_has_unresolved),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update_transactional,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_update_transactional.return_value = 1
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        assert mock_update_transactional.called
        call_kwargs = mock_update_transactional.call_args[1]
        assert call_kwargs["schedule_deletion"] is True
        assert call_kwargs["deletion_delay"].total_seconds() == 0
        assert "threads-resolved" in str(mock_update_transactional.call_args)

        assert mock_cleanup.reschedule.called


@pytest.mark.asyncio
async def test_threads_not_resolved_no_reemission(base_mr_payload, mock_database):
    """Test that keeping unresolved threads does not trigger re-emission."""
    base_mr_payload.object_attributes.action = "update"
    base_mr_payload.object_attributes.state = "opened"
    base_mr_payload.object_attributes.draft = False
    base_mr_payload.object_attributes.updated_at = "2025-01-01T12:00:00Z"

    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=uuid.uuid4(),
    )

    sample_mri = make_mock_mri(base_mr_payload)

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.dbh.any_message_needs_update", return_value=False),
        patch("webhook.merge_request.has_unresolved_threads", return_value=True),
        patch("webhook.merge_request.render"),
        patch("webhook.merge_request.compute_mri_fingerprint", return_value="test-fp"),
        patch("webhook.merge_request.update_all_messages_transactional") as mock_update_transactional,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(msg_ref.message_id)}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        from webhook.merge_request import merge_request

        await merge_request(
            mr=base_mr_payload,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Transactional update may or may not be called, but never for "threads-resolved"
        if mock_update_transactional.called:
            assert "threads-resolved" not in str(mock_update_transactional.call_args)
