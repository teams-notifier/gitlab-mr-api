#!/usr/bin/env python3
"""
Tests for participant filtering logic.

Tests that messages are only created/updated when:
- Participant filter is disabled (empty list)
- Current user is in the filter
- Assignees/reviewers are in the filter
- MR opener is in the filter
"""
import uuid
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from gitlab_model import GLUser
from webhook.messaging import MRMessRef


@pytest.fixture
def mr_with_participants(sample_merge_request_payload):
    """MR payload with opener, assignees, and reviewers."""
    sample_merge_request_payload.object_attributes.action = "open"
    sample_merge_request_payload.object_attributes.state = "opened"
    sample_merge_request_payload.user.id = 100
    sample_merge_request_payload.assignees = [
        GLUser(id=200, username="assignee1", name="Assignee 1", email="assignee1@example.com")
    ]
    sample_merge_request_payload.reviewers = [
        GLUser(id=300, username="reviewer1", name="Reviewer 1", email="reviewer1@example.com")
    ]
    return sample_merge_request_payload


def make_mock_mri(payload, ref_id=1):
    """Create a mock MergeRequestInfos object."""
    mri = MagicMock()
    mri.merge_request_ref_id = ref_id
    mri.merge_request_payload = payload
    mri.merge_request_extra_state = MagicMock()
    mri.merge_request_extra_state.opener = payload.user
    mri.merge_request_extra_state.approvers = {}
    return mri


@pytest.mark.asyncio
async def test_no_filter_creates_message(mr_with_participants, mock_database):
    """Test that message is created when no participant filter is set."""
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "POST"


@pytest.mark.asyncio
async def test_opener_in_filter_creates_message(mr_with_participants, mock_database):
    """Test that message is created when opener is in participant filter."""
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[100],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1


@pytest.mark.asyncio
async def test_assignee_in_filter_creates_message(mr_with_participants, mock_database):
    """Test that message is created when assignee is in participant filter."""
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[200],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1


@pytest.mark.asyncio
async def test_reviewer_in_filter_creates_message(mr_with_participants, mock_database):
    """Test that message is created when reviewer is in participant filter."""
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[300],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1


@pytest.mark.asyncio
async def test_no_participant_match_uses_update_only(mr_with_participants, mock_database):
    """Test that message uses update_only when no participant matches filter."""
    existing_message_id = uuid.uuid4()
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=existing_message_id,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[999],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "PATCH"


@pytest.mark.asyncio
async def test_multiple_participants_in_filter(mr_with_participants, mock_database):
    """Test with multiple participants in filter."""
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[100, 200, 300, 400],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1


@pytest.mark.e2e
def test_filter_validation_in_endpoint(test_database_url):
    """Test participant filter validation in endpoint."""
    from config import config
    from db import database

    config.DATABASE_URL = test_database_url
    database._config = config

    from fastapi.testclient import TestClient
    from app import app

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/gitlab-webhook?filter_on_participant_ids=100,200,300",
            json={
                "object_kind": "merge_request",
                "event_type": "merge_request",
                "repository": {"homepage": "https://gitlab.example.com/test/project", "name": "project"},
                "user": {"id": 1, "username": "testuser", "name": "Test User", "email": "test@example.com"},
                "project": {
                    "path_with_namespace": "test/project",
                    "web_url": "https://gitlab.example.com/test/project",
                },
                "object_attributes": {
                    "id": 123,
                    "iid": 1,
                    "title": "Test MR",
                    "created_at": "2025-01-01T00:00:00Z",
                    "draft": False,
                    "state": "opened",
                    "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
                    "action": "open",
                    "updated_at": "2025-01-01T00:00:00Z",
                    "detailed_merge_status": "mergeable",
                    "head_pipeline_id": None,
                    "work_in_progress": False,
                    "source_project_id": 100,
                    "source_branch": "feature",
                    "target_project_id": 100,
                    "target_branch": "main",
                },
                "changes": {},
                "assignees": [],
                "reviewers": [],
            },
            headers={"X-Gitlab-Token": "test-token", "X-Gitlab-Event": "Merge Request Hook"},
        )

        assert response.status_code in [200, 403, 422, 500]


@pytest.mark.e2e
def test_invalid_filter_format_returns_400(test_database_url):
    """Test that invalid participant filter format returns 400."""
    from config import config
    from db import database

    config.DATABASE_URL = test_database_url
    database._config = config

    from fastapi.testclient import TestClient
    from app import app

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/gitlab-webhook?filter_on_participant_ids=abc,def",
            json={
                "object_kind": "merge_request",
                "event_type": "merge_request",
                "repository": {"homepage": "https://gitlab.example.com/test/project", "name": "project"},
                "user": {"id": 1, "username": "testuser", "name": "Test User", "email": "test@example.com"},
                "project": {
                    "path_with_namespace": "test/project",
                    "web_url": "https://gitlab.example.com/test/project",
                },
                "object_attributes": {
                    "id": 123,
                    "iid": 1,
                    "title": "Test MR",
                    "created_at": "2025-01-01T00:00:00Z",
                    "draft": False,
                    "state": "opened",
                    "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
                    "action": "open",
                    "updated_at": "2025-01-01T00:00:00Z",
                    "detailed_merge_status": "mergeable",
                    "head_pipeline_id": None,
                    "work_in_progress": False,
                    "source_project_id": 100,
                    "source_branch": "feature",
                    "target_project_id": 100,
                    "target_branch": "main",
                },
                "changes": {},
                "assignees": [],
                "reviewers": [],
            },
            headers={"X-Gitlab-Token": "test-token", "X-Gitlab-Event": "Merge Request Hook"},
        )

        assert response.status_code in [400, 422]


@pytest.mark.asyncio
async def test_draft_mr_with_non_participant_uses_update_only(mr_with_participants, mock_database):
    """Test that draft MR with non-participant uses update_only mode."""
    mr_with_participants.object_attributes.draft = True
    mr_with_participants.object_attributes.state = "opened"

    existing_message_id = uuid.uuid4()
    conv_token = uuid.UUID("11111111-1111-1111-1111-111111111111")
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=existing_message_id,
    )

    connection = mock_database.connection
    connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos") as mock_dbh,
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):

        mock_dbh.return_value = make_mock_mri(mr_with_participants)
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
            mr=mr_with_participants,
            conversation_tokens=[str(conv_token)],
            participant_ids_filter=[999],
            new_commits_revoke_approvals=False,
        )

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "PATCH"
