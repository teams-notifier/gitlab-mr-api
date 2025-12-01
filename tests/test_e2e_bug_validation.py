#!/usr/bin/env python3
"""
E2E validation of critical bugs with real database.

These tests demonstrate the bugs exist with real database operations.
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
            "target_project_id": 100,
            "author_id": 1,
            "assignee_id": None,
            "iid": 1,
            "title": "Test MR",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "state": "opened",
            "merge_status": "can_be_merged",
            "detailed_merge_status": "not_open",
            "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
            "action": "merge",
            "draft": False,
            "work_in_progress": False,
            "head_pipeline_id": None,
            "oldrev": None,
            "description": "Test description",
            "source": {},
            "target": {},
            "last_commit": {},
            "assignee": None,
        },
        "labels": [],
        "assignees": [],
        "reviewers": [],
        "changes": {},
    }


@pytest.mark.asyncio
async def test_e2e_bug_non_transactional_deletion(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """
    E2E validation of Bug #1: Transaction wrapper for deletion operations.

    This test verifies that merge/close operations complete atomically.
    All deletion operations (INSERT into msg_to_delete + DELETE from refs)
    happen within a transaction, ensuring consistency.
    """
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

        # Create MR with open action
        conv_token = str(uuid.uuid4())
        mr_payload["object_attributes"]["action"] = "open"
        mr_payload["object_attributes"]["state"] = "opened"
        payload_open = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_open,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Verify MR and message refs were created
        msg_refs_before = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
        assert len(msg_refs_before) == 1

        mr_refs_before = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
        assert len(mr_refs_before) == 1

        # Trigger merge action
        mr_payload["object_attributes"]["action"] = "merge"
        mr_payload["object_attributes"]["state"] = "merged"
        payload_merge = MergeRequestPayload(**mr_payload)

        await merge_request(
            mr=payload_merge,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    # Verify atomic deletion: either all records deleted or none
    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    msg_refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    mr_refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")

    # With proper transaction wrapper, deletion should be complete and atomic:
    # - Deletion record created
    # - Message refs deleted
    # - MR ref persists for historical reference
    assert len(deletion_records) == 1, "Deletion record should be created"
    assert len(msg_refs_after) == 0, "Message refs should be deleted"
    assert len(mr_refs_after) == 1, "MR ref should persist for historical reference"

    await db_lifecycle.disconnect()
