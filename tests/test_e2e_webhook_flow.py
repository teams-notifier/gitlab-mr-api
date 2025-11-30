#!/usr/bin/env python3
"""
E2E tests for complete webhook flow with real database.

Tests the full application flow: endpoint → handler → database → activity-API
"""
import uuid
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.e2e


@pytest.fixture
def mr_payload():
    """Sample MR webhook payload."""
    return {
        "object_kind": "merge_request",
        "event_type": "merge_request",
        "user": {
            "id": 1,
            "name": "Test User",
            "username": "testuser",
            "email": "test@example.com",
            "avatar_url": "https://example.com/avatar.jpg",
        },
        "project": {
            "id": 100,
            "name": "Test Project",
            "description": "A test project",
            "web_url": "https://gitlab.example.com/test/project",
            "avatar_url": None,
            "git_ssh_url": "git@gitlab.example.com:test/project.git",
            "git_http_url": "https://gitlab.example.com/test/project.git",
            "namespace": "test",
            "visibility_level": 0,
            "path_with_namespace": "test/project",
            "default_branch": "main",
        },
        "repository": {
            "name": "Test Project",
            "url": "git@gitlab.example.com:test/project.git",
            "description": "A test project",
            "homepage": "https://gitlab.example.com/test/project",
        },
        "object_attributes": {
            "id": 1000,
            "target_branch": "main",
            "source_branch": "feature",
            "source_project_id": 100,
            "author_id": 1,
            "assignee_id": None,
            "title": "Test MR",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "state": "opened",
            "merge_status": "can_be_merged",
            "detailed_merge_status": "not_open",
            "target_project_id": 100,
            "iid": 1,
            "description": "Test description",
            "source": {},
            "target": {},
            "last_commit": {},
            "work_in_progress": False,
            "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
            "action": "open",
            "assignee": None,
            "draft": False,
            "head_pipeline_id": None,
            "oldrev": None,
        },
        "labels": [],
        "assignees": [],
        "reviewers": [],
        "changes": {},
    }


@pytest.mark.asyncio
async def test_e2e_mr_open_creates_database_records(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test MR open creates GitLab instance, MR ref, and message ref."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())
        payload = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    gitlab_instances = await db_connection.fetch("SELECT * FROM gitlab_mr_api.gitlab_instance")
    assert len(gitlab_instances) == 1
    assert gitlab_instances[0]["hostname"] == "gitlab.example.com"

    mr_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    assert len(mr_refs) == 1
    assert mr_refs[0]["gitlab_project_id"] == 100
    assert mr_refs[0]["gitlab_merge_request_iid"] == 1

    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs) == 1
    assert msg_refs[0]["message_id"] is not None

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_mr_merge_creates_deletion_record(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test MR merge creates deletion record in msg_to_delete."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup"),
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())
        payload = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        msg_id_before_merge = await db_connection.fetchval(
            """
            SELECT message_id FROM gitlab_mr_api.merge_request_message_ref
            ORDER BY merge_request_message_ref_id DESC LIMIT 1
            """
        )

        mr_payload["object_attributes"]["action"] = "merge"
        mr_payload["object_attributes"]["state"] = "merged"
        payload_merge = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_merge,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    assert len(deletion_records) == 1
    assert deletion_records[0]["message_id"] == str(msg_id_before_merge)

    msg_refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs_after) == 0

    # MR ref persists for historical reference
    mr_refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    assert len(mr_refs_after) == 1

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_multiple_conversation_tokens(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test MR with multiple conversation tokens creates multiple message refs."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_tokens = [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())]
        payload = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload,
            conversation_tokens=conv_tokens,
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs) == 3

    for msg_ref in msg_refs:
        assert str(msg_ref["conversation_token"]) in conv_tokens
        assert msg_ref["message_id"] is not None

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_approval_updates_extra_state(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test approval webhook updates merge_request_extra_state."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())
        payload = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        mr_payload["object_attributes"]["action"] = "approved"
        payload_approved = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_approved,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    mr_ref = await db_connection.fetchrow(
        "SELECT merge_request_extra_state FROM gitlab_mr_api.merge_request_ref"
    )

    import json

    extra_state = (
        json.loads(mr_ref["merge_request_extra_state"])
        if isinstance(mr_ref["merge_request_extra_state"], str)
        else mr_ref["merge_request_extra_state"]
    )
    approvers = extra_state["approvers"]
    assert "1" in approvers
    assert approvers["1"]["status"] == "approved"

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_draft_to_ready_deletes_old_messages(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test draft-to-ready transition deletes old messages."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup"),
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())

        mr_payload["object_attributes"]["draft"] = True
        payload_draft = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_draft,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        msg_refs_before = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
        assert len(msg_refs_before) == 1

        import time

        time.sleep(0.1)

        mr_payload["object_attributes"]["draft"] = False
        mr_payload["object_attributes"]["action"] = "update"
        mr_payload["object_attributes"]["updated_at"] = "2025-01-01T12:00:00Z"
        mr_payload["changes"] = {"draft": {"previous": True, "current": False}}
        payload_ready = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_ready,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    assert len(deletion_records) == 1

    msg_refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs_after) == 1
    assert (
        msg_refs_after[0]["merge_request_message_ref_id"]
        != msg_refs_before[0]["merge_request_message_ref_id"]
    )
    assert msg_refs_after[0]["message_id"] is not None

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_idempotent_webhook_processing(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test processing same webhook multiple times is idempotent."""
    from db import DBHelper, DatabaseLifecycleHandler
    from config import DefaultConfig
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    db_lifecycle = DatabaseLifecycleHandler(config)
    await db_lifecycle.connect()

    dbh = DBHelper(db_lifecycle)

    with (
        patch("webhook.merge_request.database", db_lifecycle),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle),
        patch("db.database", db_lifecycle),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())
        payload = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    gitlab_instances = await db_connection.fetch("SELECT * FROM gitlab_mr_api.gitlab_instance")
    assert len(gitlab_instances) == 1

    mr_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    assert len(mr_refs) == 1

    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs) == 1

    await db_lifecycle.disconnect()
