#!/usr/bin/env python3
"""
E2E tests for complete webhook flow with real database.

Tests the full application flow: endpoint → handler → database → activity-API
"""

import uuid

from typing import Any
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
async def test_e2e_mr_open_creates_database_records(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Test MR open creates GitLab instance, MR ref, and message ref."""
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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


def _draft_toggle_payload(
    mr_payload: dict[str, Any], draft: bool, updated_at: str = "2025-01-01T12:00:00Z"
) -> dict[str, Any]:
    """Turn the payload into the draft toggle event GitLab fans out to every hook."""
    mr_payload["object_attributes"]["draft"] = draft
    mr_payload["object_attributes"]["action"] = "update"
    mr_payload["object_attributes"]["updated_at"] = updated_at
    mr_payload["changes"] = {"draft": {"previous": not draft, "current": draft}}
    return mr_payload


def _ready_payload(mr_payload: dict[str, Any], updated_at: str = "2025-01-01T12:00:00Z") -> dict[str, Any]:
    return _draft_toggle_payload(mr_payload, draft=False, updated_at=updated_at)


def _message_posts(requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in requests if r["method"] == "POST" and r["url"].endswith("api/v1/message")]


@pytest.mark.asyncio
async def test_e2e_draft_to_ready_delivered_twice_keeps_one_new_card(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Two hooks on the same channel deliver the same draft→ready event: one fresh card survives.

    Covers 001-AC1.
    """
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
        await merge_request(
            mr=MergeRequestPayload(**mr_payload),
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        draft_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
        assert len(draft_refs) == 1
        draft_msg_id = str(draft_refs[0]["message_id"])
        posts_before_ready = len(_message_posts(mock_activity_api["requests"]))

        payload_ready = MergeRequestPayload(**_ready_payload(mr_payload))
        for _ in range(2):
            await merge_request(
                mr=payload_ready,
                conversation_tokens=[conv_token],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    deleted_ids = [r["message_id"] for r in deletion_records]
    assert deleted_ids == [draft_msg_id]

    refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(refs_after) == 1
    assert str(refs_after[0]["conversation_token"]) == conv_token
    new_msg_id = refs_after[0]["message_id"]
    assert new_msg_id is not None
    assert str(new_msg_id) != draft_msg_id
    assert str(new_msg_id) not in deleted_ids

    # The replay must not repost: a second POST is the card that delivery 2 used to recreate.
    assert len(_message_posts(mock_activity_api["requests"])) - posts_before_ready == 1

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_draft_to_ready_replay_creates_card_in_its_own_channel(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Hooks on two channels deliver the same draft→ready event, one channel each.

    Delivery 1 reposts in its channel; the replay must create the missing card in the other
    channel without deleting the card delivery 1 just posted. Covers 001-AC3 and 001-AC2.
    """
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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

        token_x = str(uuid.uuid4())
        token_y = str(uuid.uuid4())

        mr_payload["object_attributes"]["draft"] = True
        await merge_request(
            mr=MergeRequestPayload(**mr_payload),
            conversation_tokens=[token_x, token_y],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        draft_refs = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
        assert len(draft_refs) == 2
        draft_msg_ids = {str(r["message_id"]) for r in draft_refs}

        payload_ready = MergeRequestPayload(**_ready_payload(mr_payload))
        await merge_request(
            mr=payload_ready,
            conversation_tokens=[token_x],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        x_card_id = await db_connection.fetchval(
            "SELECT message_id FROM gitlab_mr_api.merge_request_message_ref WHERE conversation_token = $1",
            uuid.UUID(token_x),
        )
        assert x_card_id is not None

        await merge_request(
            mr=payload_ready,
            conversation_tokens=[token_y],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    # Delivery 1 drops every draft card, including Y's, since the repost covers all channels.
    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    deleted_ids = [r["message_id"] for r in deletion_records]
    assert len(deleted_ids) == 2
    assert set(deleted_ids) == draft_msg_ids
    assert str(x_card_id) not in deleted_ids

    refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    by_token = {str(r["conversation_token"]): r["message_id"] for r in refs_after}
    assert set(by_token) == {token_x, token_y}
    assert by_token[token_x] == x_card_id
    assert by_token[token_y] is not None
    assert str(by_token[token_y]) not in draft_msg_ids | set(deleted_ids)

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_draft_to_ready_reposts_when_refs_already_stamped(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """The API refresh stamped the refs with the event's updated_at before the webhook: still repost.

    Covers 001-AC5.
    """
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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
        await merge_request(
            mr=MergeRequestPayload(**mr_payload),
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

        draft_msg_id = str(
            await db_connection.fetchval("SELECT message_id FROM gitlab_mr_api.merge_request_message_ref")
        )
        posts_before_ready = len(_message_posts(mock_activity_api["requests"]))

        # What periodic_cleanup's refresh from the GitLab API does when it runs before the webhook
        await db_connection.execute(
            """UPDATE gitlab_mr_api.merge_request_message_ref
               SET last_processed_updated_at = '2025-01-01T12:00:00Z'"""
        )

        await merge_request(
            mr=MergeRequestPayload(**_ready_payload(mr_payload)),
            conversation_tokens=[conv_token],
            participant_ids_filter=[],
            new_commits_revoke_approvals=False,
        )

    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    assert [r["message_id"] for r in deletion_records] == [draft_msg_id]

    refs_after = await db_connection.fetch("SELECT * FROM gitlab_mr_api.merge_request_message_ref")
    assert len(refs_after) == 1
    assert refs_after[0]["message_id"] is not None
    assert str(refs_after[0]["message_id"]) != draft_msg_id
    assert len(_message_posts(mock_activity_api["requests"])) - posts_before_ready == 1

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_ready_draft_ready_reposts_on_each_ready_event(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """Ready, back to draft, ready again: the second ready event reposts, its replay does not.

    Covers 001-AC6.
    """
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request

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

        async def deliver(payload: dict[str, Any]) -> str:
            await merge_request(
                mr=MergeRequestPayload(**payload),
                conversation_tokens=[conv_token],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )
            msg_id = await db_connection.fetchval(
                "SELECT message_id FROM gitlab_mr_api.merge_request_message_ref"
            )
            return str(msg_id)

        mr_payload["object_attributes"]["draft"] = True
        draft_msg_id = await deliver(mr_payload)
        t1_msg_id = await deliver(_ready_payload(mr_payload, "2025-01-01T01:00:00Z"))
        redraft_msg_id = await deliver(_draft_toggle_payload(mr_payload, True, "2025-01-01T02:00:00Z"))
        # Going back to draft only collapses the card in place
        assert redraft_msg_id == t1_msg_id

        t3_payload = _ready_payload(mr_payload, "2025-01-01T03:00:00Z")
        t3_msg_id = await deliver(t3_payload)
        deleted_after_t3 = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")

        assert await deliver(t3_payload) == t3_msg_id

    deletion_records = await db_connection.fetch("SELECT * FROM gitlab_mr_api.msg_to_delete")
    assert len(deletion_records) == len(deleted_after_t3) == 2
    deleted_ids = {r["message_id"] for r in deletion_records}
    assert deleted_ids == {draft_msg_id, t1_msg_id}
    assert t3_msg_id not in deleted_ids
    assert t3_msg_id not in (draft_msg_id, t1_msg_id, "None")

    await db_lifecycle.disconnect()


@pytest.mark.asyncio
async def test_e2e_late_older_ready_event_never_reclaims_the_repost(
    db_connection, clean_database, mr_payload, mock_activity_api, test_database_url
):
    """A late older ready event, then a replay of the newer one: the repost is claimed only once.

    The participant filter excludes everyone, so no ref carries a timestamp and the upstream
    out-of-order check lets the late event through: only the claim can reject it. Covers 001-AC7.
    """
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler
    from db import DBHelper
    from gitlab_model import MergeRequestPayload
    from webhook.merge_request import merge_request
    from webhook.messaging import update_all_messages_transactional

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
        patch(
            "webhook.merge_request.update_all_messages_transactional", wraps=update_all_messages_transactional
        ) as spy_update_all,
    ):
        mock_render.return_value = {"type": "AdaptiveCard"}

        conv_token = str(uuid.uuid4())

        async def deliver(payload: dict[str, Any]) -> str | None:
            await merge_request(
                mr=MergeRequestPayload(**payload),
                conversation_tokens=[conv_token],
                participant_ids_filter=[999],
                new_commits_revoke_approvals=False,
            )
            marker: str | None = await db_connection.fetchval(
                "SELECT merge_request_extra_state->>'draft_to_ready_at' FROM gitlab_mr_api.merge_request_ref"
            )
            return marker

        mr_payload["object_attributes"]["draft"] = True
        await deliver(mr_payload)
        stamped = await db_connection.fetchval(
            "SELECT count(*) FROM gitlab_mr_api.merge_request_message_ref"
            " WHERE last_processed_updated_at IS NOT NULL"
        )
        assert stamped == 0

        t3_marker = await deliver(_ready_payload(mr_payload, "2025-01-01T03:00:00Z"))
        assert t3_marker == "2025-01-01T03:00:00+00:00"
        assert await deliver(_ready_payload(mr_payload, "2025-01-01T01:00:00Z")) == t3_marker
        assert await deliver(_ready_payload(mr_payload, "2025-01-01T03:00:00Z")) == t3_marker

    reposts = [c for c in spy_update_all.call_args_list if c.args[5] == "draft-to-ready"]
    assert len(reposts) == 1

    await db_lifecycle.disconnect()
