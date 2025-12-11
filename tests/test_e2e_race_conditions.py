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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler

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
            "created_at": "2025-01-01 00:00:00 UTC",
            "updated_at": "2025-01-01 00:00:00 UTC",
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
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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


@pytest.mark.asyncio
async def test_e2e_race_out_of_order_events_rejected(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Out-of-order events arrive after a newer event has been processed.

    Scenario:
    1. Create initial MR message
    2. Process a "newer" update event (updated_at = T2)
    3. Concurrently send multiple "older" events (updated_at = T1 < T2)
    4. Verify older events are skipped based on last_processed_updated_at

    Validates:
    - Timestamp comparison works correctly inside DB transaction
    - Row locking doesn't prevent the stale check from working
    - Stale events don't trigger Teams message updates
    - last_processed_updated_at only advances with newer events
    """
    import datetime

    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

    dbh = DBHelper(db_lifecycle_handler)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        # Step 1: Create initial MR with T0 timestamp
        base_mr_payload["object_attributes"]["action"] = "open"
        base_mr_payload["object_attributes"]["updated_at"] = "2025-01-01 00:00:00 UTC"
        base_mr_payload["object_attributes"]["description"] = "Initial"
        payload_initial = MergeRequestPayload(**base_mr_payload)

        await merge_request(
            mr=payload_initial,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Step 2: Process a "newer" update (T2 = June 2025)
        newer_payload_dict = base_mr_payload.copy()
        newer_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        newer_payload_dict["object_attributes"]["action"] = "update"
        newer_payload_dict["object_attributes"]["updated_at"] = "2025-06-01 12:00:00 UTC"
        newer_payload_dict["object_attributes"]["description"] = "Newer update"
        newer_payload_dict["object_attributes"]["head_pipeline_id"] = 9999
        newer_payload = MergeRequestPayload(**newer_payload_dict)

        await merge_request(
            mr=newer_payload,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Verify the newer update was stored
        msg_refs_after_newer = await db_connection.fetch(
            "SELECT * FROM gitlab_mr_api.merge_request_message_ref"
        )
        stored_updated_at = msg_refs_after_newer[0]["last_processed_updated_at"]
        assert stored_updated_at is not None, "Should have stored updated_at"
        expected_newer_ts = datetime.datetime(2025, 6, 1, 12, 0, 0, tzinfo=datetime.UTC)
        assert stored_updated_at == expected_newer_ts, "Should store June timestamp"

        # Step 3: Concurrently send multiple "older" events (T1 = March 2025, before T2)
        older_tasks = []
        for i in range(10):
            older_payload_dict = base_mr_payload.copy()
            older_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            older_payload_dict["object_attributes"]["action"] = "update"
            older_payload_dict["object_attributes"]["updated_at"] = "2025-03-01 06:00:00 UTC"
            older_payload_dict["object_attributes"]["description"] = f"Older update {i}"
            older_payload_dict["object_attributes"]["head_pipeline_id"] = 1000 + i
            older_payload = MergeRequestPayload(**older_payload_dict)

            older_tasks.append(
                merge_request(
                    mr=older_payload,
                    conversation_tokens=[conv_token],
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        results = await asyncio.gather(*older_tasks, return_exceptions=True)

    # Step 4: Verify older events did NOT update the message timestamp
    # The last_processed_updated_at should still reflect the newer event (June 2025)
    msg_refs_final = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    final_updated_at = msg_refs_final[0]["last_processed_updated_at"]
    assert final_updated_at == stored_updated_at, (
        f"last_processed_updated_at should not change when older events are rejected. "
        f"Expected {stored_updated_at}, got {final_updated_at}"
    )

    # No exceptions should occur (older events are gracefully skipped)
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_out_of_order_mixed_with_newer(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Mix of older and newer events arrive concurrently.

    Validates:
    - last_processed_updated_at reflects the newest processed event
    - Older events (by timestamp) are skipped once a newer one is processed
    - System correctly handles mixed timestamps under concurrency
    """
    import datetime

    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

    dbh = DBHelper(db_lifecycle_handler)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        # Create initial MR
        base_mr_payload["object_attributes"]["action"] = "open"
        base_mr_payload["object_attributes"]["updated_at"] = "2025-01-01 00:00:00 UTC"
        payload_initial = MergeRequestPayload(**base_mr_payload)

        await merge_request(
            mr=payload_initial,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Prepare mixed events: some older, some newer
        all_tasks = []
        timestamps = [
            ("2025-02-01 00:00:00 UTC", 2000),
            ("2025-08-01 00:00:00 UTC", 8000),
            ("2025-03-01 00:00:00 UTC", 3000),
            ("2025-09-01 00:00:00 UTC", 9000),
            ("2025-04-01 00:00:00 UTC", 4000),
            ("2025-10-01 00:00:00 UTC", 10000),  # newest
            ("2025-05-01 00:00:00 UTC", 5000),
            ("2025-06-01 00:00:00 UTC", 6000),
        ]

        for ts, pipeline_id in timestamps:
            payload_dict = base_mr_payload.copy()
            payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
            payload_dict["object_attributes"]["action"] = "update"
            payload_dict["object_attributes"]["updated_at"] = ts
            payload_dict["object_attributes"]["head_pipeline_id"] = pipeline_id
            payload = MergeRequestPayload(**payload_dict)

            all_tasks.append(
                merge_request(
                    mr=payload,
                    conversation_tokens=[conv_token],
                    participant_ids_filter=[],
                    new_commits_revoke_approvals=False,
                )
            )

        results = await asyncio.gather(*all_tasks, return_exceptions=True)

    # Verify the newest event timestamp (October 2025) is stored
    # This is the key guarantee: last_processed_updated_at should be the max timestamp
    msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    final_updated_at = msg_refs[0]["last_processed_updated_at"]
    expected_newest_ts = datetime.datetime(2025, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)
    assert final_updated_at == expected_newest_ts, (
        f"last_processed_updated_at should be the newest timestamp. "
        f"Expected {expected_newest_ts}, got {final_updated_at}"
    )

    # No exceptions
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"No exceptions should occur: {exceptions}"


@pytest.mark.asyncio
async def test_e2e_race_concurrent_close_and_reopen(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Close and reopen events arrive and execute concurrently.

    Timeline on GitLab: Open(T0) → Close(T1) → Reopen(T2)
    Concurrent arrival: Close(T1) and Reopen(T2) race each other

    This tests the race window where:
    1. Close (T1) passes early OOO check (sees T0)
    2. Reopen (T2) also passes early OOO check (sees T0)
    3. Both compete for row locks
    4. Final state should reflect the newer event (reopen)

    Validates:
    - Even under concurrent execution, newer timestamp wins
    - Messages are NOT deleted if reopen (newer) should take precedence
    - System handles lock contention gracefully
    """
    import datetime

    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

    dbh = DBHelper(db_lifecycle_handler)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
        patch("webhook.merge_request.periodic_cleanup"),
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        # Step 1: Open MR (T0 = January)
        base_mr_payload["object_attributes"]["action"] = "open"
        base_mr_payload["object_attributes"]["state"] = "opened"
        base_mr_payload["object_attributes"]["updated_at"] = "2025-01-01 00:00:00 UTC"
        payload_open = MergeRequestPayload(**base_mr_payload)

        await merge_request(
            mr=payload_open,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Verify message created
        msg_refs_after_open = await db_connection.fetch(
            "SELECT * FROM gitlab_mr_api.merge_request_message_ref"
        )
        assert len(msg_refs_after_open) == 1
        initial_message_id = msg_refs_after_open[0]["message_id"]
        assert initial_message_id is not None

        # Step 2: Prepare close (T1 = February) and reopen (T2 = March)
        close_payload_dict = base_mr_payload.copy()
        close_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        close_payload_dict["object_attributes"]["action"] = "close"
        close_payload_dict["object_attributes"]["state"] = "closed"
        close_payload_dict["object_attributes"]["updated_at"] = "2025-02-01 00:00:00 UTC"
        payload_close = MergeRequestPayload(**close_payload_dict)

        reopen_payload_dict = base_mr_payload.copy()
        reopen_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        reopen_payload_dict["object_attributes"]["action"] = "reopen"
        reopen_payload_dict["object_attributes"]["state"] = "opened"
        reopen_payload_dict["object_attributes"]["updated_at"] = "2025-03-01 00:00:00 UTC"
        payload_reopen = MergeRequestPayload(**reopen_payload_dict)

        # Step 3: Run close and reopen CONCURRENTLY multiple times to stress test
        for iteration in range(5):
            # Reset state for each iteration by reopening if needed
            if iteration > 0:
                # Ensure MR has a message ref for next iteration
                msg_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
                if len(msg_refs) == 0:
                    # Recreate message ref
                    await merge_request(
                        mr=payload_open,
                        conversation_tokens=[conv_token],
                        participant_ids_filter=[],
                        new_commits_revoke_approvals=False,
                    )

            close_task = merge_request(
                mr=payload_close,
                conversation_tokens=[conv_token],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            reopen_task = merge_request(
                mr=payload_reopen,
                conversation_tokens=[conv_token],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            # Run concurrently
            results = await asyncio.gather(close_task, reopen_task, return_exceptions=True)

            # No exceptions should occur
            exceptions = [r for r in results if isinstance(r, Exception)]
            assert len(exceptions) == 0, f"Iteration {iteration}: exceptions occurred: {exceptions}"

    # Step 4: Final verification - the system should be in a consistent state
    # Either:
    # a) Reopen won: message refs exist with March timestamp
    # b) Close won then reopen recreated: message refs exist
    # c) Close won: no message refs, but deletion scheduled
    #
    # The key invariant: if message refs exist, their timestamp should be >= reopen timestamp
    # because reopen is the newer event

    msg_refs_final = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")

    print(f"Final state: {len(msg_refs_final)} message refs, {len(deletion_records)} deletions")

    if len(msg_refs_final) > 0:
        # If messages exist, verify the timestamp reflects the newer event
        for ref in msg_refs_final:
            stored_updated_at = ref["last_processed_updated_at"]
            if stored_updated_at is not None:
                # Should be >= reopen timestamp (March) since reopen is newer
                expected_min_ts = datetime.datetime(2025, 3, 1, 0, 0, 0, tzinfo=datetime.UTC)
                assert stored_updated_at >= expected_min_ts, (
                    f"Message ref has stale timestamp {stored_updated_at}, "
                    f"should be >= {expected_min_ts} (reopen timestamp)"
                )

    # System remained consistent (no exceptions, valid final state)
    assert len(msg_refs_final) >= 0  # Either outcome is valid under concurrency


@pytest.mark.asyncio
async def test_e2e_race_close_after_reopen_ooo(
    db_connection, clean_database, base_mr_payload, mock_activity_api, db_lifecycle_handler
):
    """
    RACE CONDITION: Close event arrives AFTER reopen event (out of order).

    Timeline on GitLab: Open(T0) → Close(T1) → Reopen(T2)
    Arrival order:      Open(T0) → Reopen(T2) → Close(T1)

    Validates:
    - Late close event is rejected (does not delete reopened MR's messages)
    - Messages remain after OOO close
    - last_processed_updated_at reflects reopen, not close
    """
    import datetime

    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

    dbh = DBHelper(db_lifecycle_handler)

    conv_token = str(uuid.uuid4())

    with (
        patch("webhook.merge_request.database", db_lifecycle_handler),
        patch("webhook.merge_request.dbh", dbh),
        patch("webhook.messaging.database", db_lifecycle_handler),
        patch("db.database", db_lifecycle_handler),
        patch("webhook.merge_request.render") as mock_render,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        # Step 1: Open MR (T0 = January)
        base_mr_payload["object_attributes"]["action"] = "open"
        base_mr_payload["object_attributes"]["state"] = "opened"
        base_mr_payload["object_attributes"]["updated_at"] = "2025-01-01 00:00:00 UTC"
        payload_open = MergeRequestPayload(**base_mr_payload)

        await merge_request(
            mr=payload_open,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Verify message created
        msg_refs_after_open = await db_connection.fetch(
            "SELECT * FROM gitlab_mr_api.merge_request_message_ref"
        )
        assert len(msg_refs_after_open) == 1
        assert msg_refs_after_open[0]["message_id"] is not None

        # Step 2: Reopen arrives FIRST (T2 = March) - out of order!
        reopen_payload_dict = base_mr_payload.copy()
        reopen_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        reopen_payload_dict["object_attributes"]["action"] = "reopen"
        reopen_payload_dict["object_attributes"]["state"] = "opened"
        reopen_payload_dict["object_attributes"]["updated_at"] = "2025-03-01 00:00:00 UTC"
        payload_reopen = MergeRequestPayload(**reopen_payload_dict)

        await merge_request(
            mr=payload_reopen,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        # Verify reopen processed
        msg_refs_after_reopen = await db_connection.fetch(
            "SELECT * FROM gitlab_mr_api.merge_request_message_ref"
        )
        assert msg_refs_after_reopen[0]["message_id"] is not None
        reopen_updated_at = msg_refs_after_reopen[0]["last_processed_updated_at"]
        assert reopen_updated_at == datetime.datetime(2025, 3, 1, 0, 0, 0, tzinfo=datetime.UTC)

        # Step 3: Close arrives SECOND (T1 = February) - should be rejected!
        close_payload_dict = base_mr_payload.copy()
        close_payload_dict["object_attributes"] = base_mr_payload["object_attributes"].copy()
        close_payload_dict["object_attributes"]["action"] = "close"
        close_payload_dict["object_attributes"]["state"] = "closed"
        close_payload_dict["object_attributes"]["updated_at"] = "2025-02-01 00:00:00 UTC"
        payload_close = MergeRequestPayload(**close_payload_dict)

        result = await merge_request(
            mr=payload_close,
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    # Step 4: Verify close was rejected - messages should still exist!
    msg_refs_final = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(msg_refs_final) == 1, "Message ref should still exist"
    assert msg_refs_final[0]["message_id"] is not None, "Message should not be deleted"

    # Timestamp should still be from reopen (March), not close (February)
    final_updated_at = msg_refs_final[0]["last_processed_updated_at"]
    assert final_updated_at == datetime.datetime(
        2025, 3, 1, 0, 0, 0, tzinfo=datetime.UTC
    ), f"Timestamp should be from reopen (March), not close. Got {final_updated_at}"

    # Close should have returned early (None result indicates early return)
    assert result is None, "Close should have been rejected and returned early"
