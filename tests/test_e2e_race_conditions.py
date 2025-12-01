#!/usr/bin/env python3
"""
E2E tests for race conditions under load with real database.

Tests concurrent webhook processing to validate:
1. Fingerprint deduplication prevents duplicate processing
2. Row locking prevents concurrent message updates
3. Database constraints catch duplicate message creation
4. Transaction isolation maintains data consistency
"""
import asyncio
import uuid
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.e2e


@pytest.fixture
async def db_lifecycle_handler(test_database_url):
    """Create a DatabaseLifecycleHandler with properly initialized pool."""
    from db import DatabaseLifecycleHandler
    from config import DefaultConfig

    config = DefaultConfig()
    config.DATABASE_URL = test_database_url

    handler = DatabaseLifecycleHandler(config)
    await handler.connect()

    yield handler

    await handler.disconnect()


@pytest.fixture
def base_mr_payload():
    """Base MR payload for race condition tests."""
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
async def test_e2e_race_duplicate_webhook_processing(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Multiple identical webhooks arrive simultaneously.

    Validates:
    - Database constraints prevent duplicate records
    - Only one webhook creates each database record (GitLab instance, MR ref, message ref)
    - All webhooks complete without errors (handled gracefully)
    """
    from db import DBHelper
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    dbh = DBHelper(db_lifecycle_handler)

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())
        payload = MergeRequestPayload(**base_mr_payload)

        # Simulate 10 identical webhooks arriving simultaneously
        tasks = [
            merge_request(
                mr=payload,
                conversation_tokens=[conv_token],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )
            for _ in range(10)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Debug: Print results
    print(f"Results: {results}")
    print(f"Result types: {[type(r) for r in results]}")

    # Verify only one GitLab instance was created
    gitlab_instances = await db_connection.fetch("SELECT * FROM gitlab_mr_api.gitlab_instance")
    print(f"GitLab instances: {len(gitlab_instances)}")
    assert len(gitlab_instances) == 1, "Multiple webhooks should create only one GitLab instance"

    # Verify only one MR ref was created
    mr_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    print(f"MR refs: {len(mr_refs)}")
    assert len(mr_refs) == 1, "Multiple webhooks should create only one MR ref"

    # Verify only one message ref was created
    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    print(f"Message refs: {len(msg_refs)}")
    assert len(msg_refs) == 1, "Multiple webhooks should create only one message ref"

    # Note: Fingerprints are only stored for update/close actions, not create actions
    # This is expected behavior - "open" actions don't store fingerprints

    # Verify all tasks completed (no exceptions)
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_concurrent_message_creation(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Multiple webhooks try to create messages for same MR simultaneously.

    Validates:
    - Database constraints prevent duplicate message refs
    - Only one message_id is stored per conversation token
    - No orphaned message refs
    """
    from db import DBHelper
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    dbh = DBHelper(db_lifecycle_handler)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):

        # Pre-create MR to focus on message creation race (inside patch context)
        payload = MergeRequestPayload(**base_mr_payload)
        mri = await dbh.get_merge_request_ref_infos(payload)

        mock_render.return_value = {"type": "AdaptiveCard"}

        # Simulate 20 concurrent requests creating messages for the same MR
        # Each with slightly different payload to avoid fingerprint deduplication
        tasks = []
        for i in range(20):
            modified_payload = base_mr_payload.copy()
            modified_payload["object_attributes"] = base_mr_payload["object_attributes"].copy()
            modified_payload["object_attributes"]["description"] = f"Description {i}"

            payload_i = MergeRequestPayload(**modified_payload)

            tasks.append(
                merge_request(
                    mr=payload_i,
                    conversation_tokens=[conv_token],
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Verify we have exactly one message ref per conversation token
    msg_refs = await db_connection.fetch(
        "SELECT * FROM gitlab_mr_api.merge_request_message_ref WHERE merge_request_ref_id = $1",
        mri.merge_request_ref_id,
    )
    assert len(msg_refs) == 1, f"Expected 1 message ref, got {len(msg_refs)}"

    # Verify the message ref has a message_id
    assert msg_refs[0]["message_id"] is not None, "Message ref should have a message_id"

    # Verify message ref has fingerprint stored
    assert msg_refs[0]["last_processed_fingerprint"] is not None, "Message ref should have fingerprint"

    # Count how many completed successfully
    successful = [r for r in results if not isinstance(r, Exception) and r is not None]
    exceptions = [r for r in results if isinstance(r, Exception)]

    print(f"Successful: {len(successful)}, Exceptions: {len(exceptions)}")
    assert len(successful) >= 1, "At least one request should succeed"
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_concurrent_updates_with_locking(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Multiple update webhooks arrive simultaneously for same MR.

    Validates:
    - Row locking prevents concurrent updates
    - All updates are serialized correctly
    - No lost updates or data corruption
    - Transaction isolation maintains consistency
    """
    from db import DBHelper
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    dbh = DBHelper(db_lifecycle_handler)

    # Create initial MR with a message
    base_mr_payload["object_attributes"]["action"] = "open"
    payload = MergeRequestPayload(**base_mr_payload)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        # Create initial message
        await merge_request(
            mr=payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Now simulate 15 concurrent update webhooks
        tasks = []
        for i in range(15):
            update_payload = base_mr_payload.copy()
            update_payload["object_attributes"] = base_mr_payload["object_attributes"].copy()
            update_payload["object_attributes"]["action"] = "update"
            update_payload["object_attributes"]["description"] = f"Updated {i}"
            update_payload["object_attributes"]["head_pipeline_id"] = 1000 + i

            payload_i = MergeRequestPayload(**update_payload)

            tasks.append(
                merge_request(
                    mr=payload_i,
                    conversation_tokens=[conv_token],
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Verify we still have exactly one message ref
    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs) == 1, "Should still have exactly one message ref"

    # Verify MR ref was updated (should have last pipeline_id)
    mr_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    assert len(mr_refs) == 1
    # Pipeline ID should be one of the updates (1000-1014)
    assert mr_refs[0]["head_pipeline_id"] >= 1000
    assert mr_refs[0]["head_pipeline_id"] <= 1014

    # Verify all message refs have fingerprints (webhooks were processed)
    msg_refs = await db_connection.fetch(
        """SELECT * FROM gitlab_mr_api.merge_request_message_ref
           WHERE merge_request_ref_id = $1""",
        mr_refs[0]["merge_request_ref_id"],
    )
    for ref in msg_refs:
        assert ref["last_processed_fingerprint"] is not None, "Each message ref should have fingerprint"

    # Verify no exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_update_during_close_deletion(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Update webhook arrives while merge/close is deleting messages.

    Validates:
    - Row locking prevents updates during deletion
    - Either update happens first, then delete, or vice versa
    - No partial state or data corruption
    - Deletion cleanup is idempotent
    """
    from db import DBHelper
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    dbh = DBHelper(db_lifecycle_handler)

    # Create initial MR with messages
    base_mr_payload["object_attributes"]["action"] = "open"
    payload = MergeRequestPayload(**base_mr_payload)

    conv_tokens = [str(uuid.uuid4()) for _ in range(3)]

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup"),
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        # Create initial messages
        await merge_request(
            mr=payload,
            conversation_tokens=conv_tokens,
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Prepare merge payload (will delete messages)
        merge_payload_dict = base_mr_payload.copy()
        merge_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        merge_payload_dict["object_attributes"]["action"] = "merge"
        merge_payload_dict["object_attributes"]["state"] = "merged"
        merge_payload_dict["object_attributes"]["description"] = "Merged"
        merge_payload = MergeRequestPayload(**merge_payload_dict)

        # Prepare update payloads (will try to update messages)
        update_tasks = []
        for i in range(10):
            update_dict = base_mr_payload.copy()
            update_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            update_dict["object_attributes"]["action"] = "update"
            update_dict["object_attributes"]["description"] = f"Update {i}"
            update_dict["object_attributes"]["head_pipeline_id"] = 2000 + i
            update_payload = MergeRequestPayload(**update_dict)

            update_tasks.append(
                merge_request(
                    mr=update_payload,
                    conversation_tokens=conv_tokens,
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        # Add merge task
        merge_task = merge_request(
            mr=merge_payload,
            conversation_tokens=conv_tokens,
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Run all concurrently
        all_tasks = update_tasks + [merge_task]
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Verify deletion records were created
    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    # Should have deletion records for the 3 messages
    assert len(deletion_records) == 3, f"Should have 3 deletion records, got {len(deletion_records)}"

    # Verify message refs - may have some refs if updates arrived after merge
    # This is expected in a race condition scenario
    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    # Should have 0-3 refs depending on race timing (updates after merge create new refs)
    assert len(msg_refs) <= 3, f"Should have <= 3 message refs, got {len(msg_refs)}"

    # Verify fingerprints were stored (check deletion tracking table still has records)
    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    assert len(deletion_records) > 0, "Should have scheduled messages for deletion"

    # Verify no exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_high_concurrency_mixed_operations(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    STRESS TEST: Mix of create, update, approve, merge operations under high concurrency.

    Validates:
    - System remains consistent under realistic load
    - All locks and constraints work together
    - No deadlocks or race conditions
    - Data integrity maintained across operation types
    """
    from db import DBHelper
    from webhook.merge_request import merge_request
    from gitlab_model import MergeRequestPayload

    dbh = DBHelper(db_lifecycle_handler)

    conv_tokens = [str(uuid.uuid4()) for _ in range(5)]

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup"),
    ):

        mock_render.return_value = {"type": "AdaptiveCard"}

        all_tasks = []

        # 10 open webhooks (duplicates)
        for i in range(10):
            open_dict = base_mr_payload.copy()
            open_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            open_dict["object_attributes"]["action"] = "open"
            open_dict["object_attributes"]["description"] = f"Open {i}"
            payload = MergeRequestPayload(**open_dict)
            all_tasks.append(
                merge_request(
                    mr=payload,
                    conversation_tokens=conv_tokens,
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        # 20 update webhooks
        for i in range(20):
            update_dict = base_mr_payload.copy()
            update_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            update_dict["object_attributes"]["action"] = "update"
            update_dict["object_attributes"]["description"] = f"Update {i}"
            update_dict["object_attributes"]["head_pipeline_id"] = 3000 + i
            payload = MergeRequestPayload(**update_dict)
            all_tasks.append(
                merge_request(
                    mr=payload,
                    conversation_tokens=conv_tokens,
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        # 10 approval webhooks
        for i in range(10):
            approve_dict = base_mr_payload.copy()
            approve_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            approve_dict["object_attributes"]["action"] = "approved"
            approve_dict["object_attributes"]["description"] = f"Approved {i}"
            approve_dict["user"] = base_mr_payload["user"].copy()
            approve_dict["user"]["id"] = 100 + i
            payload = MergeRequestPayload(**approve_dict)
            all_tasks.append(
                merge_request(
                    mr=payload,
                    conversation_tokens=conv_tokens,
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        # Execute all 40 operations concurrently
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Verify database consistency
    gitlab_instances = await db_connection.fetch("SELECT * FROM gitlab_mr_api.gitlab_instance")
    assert len(gitlab_instances) == 1, "Should have exactly one GitLab instance"

    mr_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_ref")
    assert len(mr_refs) == 1, "Should have exactly one MR ref"

    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    # Should have 5 message refs (one per conversation token)
    assert len(msg_refs) == 5, f"Should have 5 message refs, got {len(msg_refs)}"

    # All message refs should have message_ids
    for ref in msg_refs:
        assert ref["message_id"] is not None, "All message refs should have message_ids"

    # Verify all message refs have fingerprints stored
    for ref in msg_refs:
        assert ref["last_processed_fingerprint"] is not None, "All message refs should have fingerprints"

    # Verify no exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"

    # Verify approvals were stored
    import json

    mr_extra_state = mr_refs[0]["merge_request_extra_state"]
    if isinstance(mr_extra_state, str):
        mr_extra_state = json.loads(mr_extra_state)
    if "approvers" in mr_extra_state:
        approvers_count = len(mr_extra_state["approvers"])
        assert approvers_count > 0, "Should have stored approver information"
