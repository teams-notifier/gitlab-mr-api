#!/usr/bin/env python3
"""
Integration tests for complete webhook flow.

Tests the full path: FastAPI endpoint → webhook handler → database → activity-API
"""

import uuid

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from fastapi.testclient import TestClient

from webhook.messaging import MRMessRef


def dict_to_mock(d):
    """Recursively convert dict to mock object with nested attributes."""
    if not isinstance(d, dict):
        return d
    mock = MagicMock()
    for key, value in d.items():
        if isinstance(value, dict):
            setattr(mock, key, dict_to_mock(value))
        elif isinstance(value, list):
            setattr(mock, key, [dict_to_mock(item) if isinstance(item, dict) else item for item in value])
        else:
            setattr(mock, key, value)
    return mock


@pytest.fixture
def test_client():
    """Create test client with mocked dependencies."""
    with (
        patch("app.database") as mock_db,
        patch("app.periodic_cleanup"),
        patch("app.config.is_valid_token", return_value=True),
    ):
        mock_db.connect = AsyncMock()
        mock_db.disconnect = AsyncMock()
        mock_db.acquire = AsyncMock()

        from app import app

        with TestClient(app) as client:
            yield client


@pytest.fixture
def valid_headers():
    """Valid request headers."""
    return {
        "X-Gitlab-Token": "valid-token",
        "X-Conversation-Token": "11111111-1111-1111-1111-111111111111",
    }


@pytest.fixture
def merge_request_payload_dict():
    """Sample MR payload as dict."""
    return {
        "object_kind": "merge_request",
        "event_type": "merge_request",
        "repository": {
            "homepage": "https://gitlab.example.com/test/project",
            "name": "project",
        },
        "user": {
            "id": 1,
            "name": "Test User",
            "username": "testuser",
            "email": "test@example.com",
        },
        "project": {
            "id": 100,
            "web_url": "https://gitlab.example.com/test/project",
            "path_with_namespace": "test/project",
        },
        "object_attributes": {
            "id": 1,
            "iid": 1,
            "title": "Test MR",
            "created_at": "2025-01-01 00:00:00 UTC",
            "updated_at": "2025-01-01 00:00:00 UTC",
            "state": "opened",
            "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
            "action": "open",
            "draft": False,
            "work_in_progress": False,
            "head_pipeline_id": None,
            "detailed_merge_status": "mergeable",
            "source_project_id": 100,
            "source_branch": "feature",
            "target_project_id": 100,
            "target_branch": "main",
        },
        "changes": {},
        "assignees": [],
        "reviewers": [],
    }


@pytest.mark.asyncio
async def test_webhook_endpoint_merge_request_open(
    test_client, valid_headers, merge_request_payload_dict, mock_database
):
    """Test full flow for MR open webhook."""
    conv_token = uuid.UUID(valid_headers["X-Conversation-Token"])
    msg_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=conv_token,
        message_id=None,
    )

    sample_mri = MagicMock(
        merge_request_ref_id=1,
        merge_request_payload=dict_to_mock(merge_request_payload_dict),
        merge_request_extra_state=MagicMock(
            opener=MagicMock(id=1),
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_or_create,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        connection = mock_database.connection
        connection.fetchrow.return_value = {"merge_request_message_ref_id": 1}

        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_or_create.return_value = {str(conv_token): msg_ref}
        mock_get_all.return_value = [msg_ref]

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=valid_headers,
        )

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

        assert mock_client.request.call_count >= 1
        first_call_args = mock_client.request.call_args_list[0]
        assert first_call_args[0][0] == "POST"
        assert "api/v1/message" in first_call_args[0][1]


@pytest.mark.asyncio
async def test_webhook_endpoint_merge_request_merge(
    test_client, valid_headers, merge_request_payload_dict, mock_database
):
    """Test full flow for MR merge webhook - should trigger deletion."""
    merge_request_payload_dict["object_attributes"]["action"] = "merge"
    merge_request_payload_dict["object_attributes"]["state"] = "merged"

    sample_mri = MagicMock(
        merge_request_ref_id=1,
        merge_request_payload=dict_to_mock(merge_request_payload_dict),
        merge_request_extra_state=MagicMock(
            opener=MagicMock(id=1),
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        patch("httpx.AsyncClient") as mock_client_class,
    ):
        connection = mock_database.connection

        message_id = uuid.uuid4()
        connection.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "message_id": message_id,
            }
        ]

        mock_render.return_value = {"type": "AdaptiveCard"}

        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message_id": str(uuid.uuid4())}
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client_class.return_value.__aenter__.return_value = mock_client

        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=valid_headers,
        )

        assert response.status_code == 200

        insert_calls = [
            call for call in connection.execute.call_args_list if "INSERT INTO msg_to_delete" in str(call)
        ]
        assert len(insert_calls) == 1

        delete_calls = [
            call
            for call in connection.execute.call_args_list
            if "DELETE FROM merge_request_message_ref" in str(call)
        ]
        assert len(delete_calls) == 1

        assert mock_cleanup.reschedule.called


@pytest.mark.asyncio
async def test_webhook_endpoint_invalid_token(test_client, merge_request_payload_dict):
    """Test webhook with invalid GitLab token."""
    invalid_headers = {
        "X-Gitlab-Token": "invalid-token",
        "X-Conversation-Token": "11111111-1111-1111-1111-111111111111",
    }

    with patch("app.config.is_valid_token", return_value=False):
        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=invalid_headers,
        )

        assert response.status_code == 403
        assert "Invalid gitlab token" in response.json()["detail"]


@pytest.mark.asyncio
async def test_webhook_endpoint_invalid_conversation_token(
    test_client, valid_headers, merge_request_payload_dict, mock_database
):
    """Test webhook with invalid conversation token UUID."""
    invalid_headers = valid_headers.copy()
    invalid_headers["X-Conversation-Token"] = "not-a-uuid"

    sample_mri = MagicMock(
        merge_request_ref_id=1,
        merge_request_payload=dict_to_mock(merge_request_payload_dict),
        merge_request_extra_state=MagicMock(
            opener=MagicMock(id=1),
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render", return_value={"type": "AdaptiveCard"}),
    ):
        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=invalid_headers,
        )

        assert response.status_code == 200


@pytest.mark.asyncio
async def test_webhook_endpoint_multiple_conversation_tokens(
    test_client, valid_headers, merge_request_payload_dict, mock_database
):
    """Test webhook with multiple conversation tokens (multiple channels)."""
    multi_conv_headers = valid_headers.copy()
    conv_tokens = [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "33333333-3333-3333-3333-333333333333",
    ]
    multi_conv_headers["X-Conversation-Token"] = ",".join(conv_tokens)

    mock_refs = [
        MRMessRef(
            merge_request_message_ref_id=i + 1,
            conversation_token=uuid.UUID(token),
            message_id=None,
        )
        for i, token in enumerate(conv_tokens)
    ]

    sample_mri = MagicMock(
        merge_request_ref_id=1,
        merge_request_payload=dict_to_mock(merge_request_payload_dict),
        merge_request_extra_state=MagicMock(
            opener=MagicMock(id=1),
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("webhook.merge_request.create_or_update_message") as mock_create,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_refs.return_value = dict(zip(conv_tokens, mock_refs, strict=False))
        mock_get_all.return_value = mock_refs
        mock_create.return_value = uuid.uuid4()

        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=multi_conv_headers,
        )

        assert response.status_code == 200
        assert mock_create.call_count == 3


@pytest.mark.asyncio
async def test_webhook_endpoint_activity_api_error(
    test_client, valid_headers, merge_request_payload_dict, mock_database
):
    """Test webhook when activity-API returns error - now returns 502 for partial failure."""
    import httpx

    conv_token = valid_headers["X-Conversation-Token"]
    mock_ref = MRMessRef(
        merge_request_message_ref_id=1,
        conversation_token=uuid.UUID(conv_token),
        message_id=None,
    )

    sample_mri = MagicMock(
        merge_request_ref_id=1,
        merge_request_payload=dict_to_mock(merge_request_payload_dict),
        merge_request_extra_state=MagicMock(
            opener=MagicMock(id=1),
            approvers={},
        ),
    )

    with (
        patch("webhook.merge_request.database", mock_database),
        patch("webhook.messaging.database", mock_database),
        patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
        patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
        patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
        patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
        patch("webhook.merge_request.create_or_update_message") as mock_create,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}
        mock_get_refs.return_value = {conv_token: mock_ref}
        mock_get_all.return_value = [mock_ref]

        mock_create.side_effect = httpx.HTTPStatusError(
            "Server error",
            request=MagicMock(),
            response=MagicMock(status_code=500, json=lambda: {"error": "Internal server error"}),
        )

        response = test_client.post(
            "/api/v1/gitlab-webhook",
            json=merge_request_payload_dict,
            headers=valid_headers,
        )

        assert response.status_code == 502
        assert response.json()["detail"]["error"] == "partial_message_update_failure"
        assert response.json()["detail"]["failed"] == 1


@pytest.mark.asyncio
async def test_healthcheck_success(test_client, mock_database):
    """Test healthcheck endpoint success."""
    with patch("app.database", mock_database):
        connection = mock_database.connection
        connection.fetchval.return_value = True

        response = test_client.get("/healthz")

        assert response.status_code == 200
        assert response.json() == {"ok": True}


@pytest.mark.asyncio
async def test_healthcheck_database_error(test_client, mock_database):
    """Test healthcheck endpoint when database fails."""
    with patch("app.database", mock_database):
        connection = mock_database.connection
        connection.fetchval.side_effect = Exception("Database connection failed")

        response = test_client.get("/healthz")

        assert response.status_code == 500
        assert "Database connection failed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_root_redirect(test_client):
    """Test root endpoint redirects to /docs."""
    response = test_client.get("/", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"] == "/docs"
