#!/usr/bin/env python3
"""
Comprehensive tests for merge_request webhook handler.
Focus on critical paths, race conditions, and edge cases.

Tests aligned with workflow documentation (docs/workflow.md).
"""

import datetime
import hashlib
import uuid

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from tests.conftest import MockAsyncHttpxClient


@pytest.fixture
def sample_mr_payload():
    """Sample MergeRequestPayload mock."""
    mr = MagicMock()
    mr.object_attributes.action = "open"
    mr.object_attributes.state = "opened"
    mr.object_attributes.draft = False
    mr.object_attributes.work_in_progress = False
    mr.object_attributes.title = "Test MR"
    mr.object_attributes.head_pipeline_id = 123
    mr.object_attributes.oldrev = None
    mr.object_attributes.updated_at = "2025-01-01 00:00:00 UTC"
    mr.project.path_with_namespace = "test/repo"
    mr.user.id = 1
    mr.user.name = "Test User"
    mr.assignees = []
    mr.reviewers = []
    mr.changes = None
    mr.model_dump_json.return_value = '{"test":"mr"}'
    mr.model_dump.return_value = {"test": "mr"}
    return mr


@pytest.fixture
def sample_mri():
    """Sample MergeRequestInfos mock."""
    mri = MagicMock()
    mri.merge_request_ref_id = 1
    mri.merge_request_payload.object_attributes.state = "opened"
    mri.merge_request_payload.object_attributes.title = "Test MR"
    mri.merge_request_payload.object_attributes.draft = False
    mri.merge_request_payload.object_attributes.work_in_progress = False
    mri.merge_request_payload.object_attributes.updated_at = "2025-01-01 00:00:00 UTC"
    mri.merge_request_payload.project.path_with_namespace = "test/repo"
    mri.merge_request_payload.assignees = []
    mri.merge_request_payload.reviewers = []
    mri.merge_request_extra_state.opener.id = 1
    return mri


def make_mock_ref(conv_token, message_id=None, ref_id=1):
    """Create a properly configured mock message ref."""
    mock_ref = MagicMock()
    mock_ref.merge_request_message_ref_id = ref_id
    mock_ref.conversation_token = conv_token
    mock_ref.message_id = message_id
    mock_ref.last_processed_fingerprint = None
    mock_ref.last_processed_updated_at = None
    return mock_ref


class TestMergeRequestBasicFlow:
    """Test basic MR webhook processing flows."""

    async def test_mr_open_creates_message(self, sample_mr_payload, sample_mri, mock_database):
        """Test that opening an MR creates a new message (workflow section 1)."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = uuid.uuid4()

            result = await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            assert result is not None
            assert "merge_request_infos" in result
            mock_create.assert_called_once()

    async def test_mr_merge_triggers_deletion(self, sample_mr_payload, sample_mri, mock_database):
        """Test that merging an MR triggers message deletion (workflow section 4)."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "merge"
        sample_mr_payload.object_attributes.state = "merged"

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
            patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        ):
            mock_update.return_value = 1

            result = await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            assert result is not None
            mock_update.assert_called_once()
            call_args = mock_update.call_args
            assert call_args[0][5] == "close/merge"
            assert call_args[1]["schedule_deletion"] is True
            mock_cleanup.reschedule.assert_called_once()

    async def test_mr_close_triggers_deletion(self, sample_mr_payload, sample_mri, mock_database):
        """Test that closing an MR triggers message deletion (workflow section 4)."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "close"
        sample_mr_payload.object_attributes.state = "closed"

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
            patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
        ):
            mock_update.return_value = 1

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_update.assert_called_once()
            mock_cleanup.reschedule.assert_called_once()


class TestMergeRequestApprovals:
    """Test approval/unapproval handling."""

    async def test_mr_approval_updates_state(self, sample_mr_payload, sample_mri, mock_database):
        """Test that approval updates MR state (workflow: non-closing operation)."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "approved"
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            conn = mock_database.connection
            assert conn.fetchrow.called
            call_args = conn.fetchrow.call_args[0]
            assert "approvers" in str(call_args[1])

    async def test_mr_unapproval_updates_state(self, sample_mr_payload, sample_mri, mock_database):
        """Test that unapproval updates MR state."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "unapproved"
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            conn = mock_database.connection
            assert conn.fetchrow.called


class TestMergeRequestUpdate:
    """Test MR update action handling."""

    async def test_mr_update_updates_pipeline_id(self, sample_mr_payload, sample_mri, mock_database):
        """Test that update action updates head_pipeline_id."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "update"
        sample_mr_payload.object_attributes.head_pipeline_id = 999
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        mock_database.connection.fetchrow.return_value = {"merge_request_extra_state": {}}

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            conn = mock_database.connection
            assert conn.fetchrow.called
            call_args = conn.fetchrow.call_args[0]
            assert 999 in call_args

    async def test_mr_update_with_new_commits_revokes_approvals(
        self, sample_mr_payload, sample_mri, mock_database
    ):
        """Test that new commits revoke approvals when configured."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "update"
        sample_mr_payload.object_attributes.oldrev = "abc123"
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        mock_database.connection.fetchrow.return_value = {"merge_request_extra_state": {}}

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=True,
            )

            conn = mock_database.connection
            # 3 calls: OOO check, pipeline update, approvers clear
            assert conn.fetchrow.call_count == 3
            call_args = conn.fetchrow.call_args_list[2][0]
            assert "approvers" in str(call_args[1])

    async def test_draft_to_ready_transition_deletes_and_creates_messages(
        self, sample_mr_payload, sample_mri, mock_database
    ):
        """Test that draft→ready transition deletes old messages and creates new ones."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "update"
        sample_mr_payload.object_attributes.draft = False
        sample_mr_payload.changes = {"draft": {"previous": True, "current": False}}
        new_mock_ref = make_mock_ref("token1", message_id=None)

        mock_database.connection.fetchrow.return_value = {"merge_request_extra_state": {}}

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.update_all_messages_transactional") as mock_update,
            patch("webhook.merge_request.periodic_cleanup") as mock_cleanup,
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_update.return_value = 1
            mock_get_refs.return_value = {"token1": new_mock_ref}
            mock_get_all.return_value = [new_mock_ref]
            mock_create.return_value = uuid.uuid4()

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_update.assert_called_once()
            call_args = mock_update.call_args
            assert call_args[0][5] == "draft-to-ready"
            assert call_args[1]["schedule_deletion"] is True
            assert call_args[1]["deletion_delay"] == datetime.timedelta(seconds=0)
            mock_cleanup.reschedule.assert_called_once()

            mock_get_refs.assert_called_once()
            mock_create.assert_called_once()
            create_args = mock_create.call_args
            assert create_args[1]["update_only"] is False


class TestParticipantFiltering:
    """Test participant filtering logic (workflow section: Filtering Behavior)."""

    async def test_no_filter_processes_all_mrs(self, sample_mr_payload, sample_mri, mock_database):
        """Test that empty filter processes all MRs."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = uuid.uuid4()

            result = await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            assert result is not None
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["update_only"] is False

    async def test_filter_matches_opener(self, sample_mr_payload, sample_mri, mock_database):
        """Test that filter matches MR opener."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = uuid.uuid4()

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[1],  # Matches opener ID
                new_commits_revoke_approvals=False,
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["update_only"] is False

    async def test_filter_no_match_skips_creation(self, sample_mr_payload, sample_mri, mock_database):
        """Test that non-matching filter sets update_only=True (workflow: Filtering Behavior)."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = None

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[999],  # No match
                new_commits_revoke_approvals=False,
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["update_only"] is True

    async def test_filter_matches_assignee(self, sample_mr_payload, sample_mri, mock_database):
        """Test that filter matches assignee."""
        from webhook.merge_request import merge_request

        assignee = MagicMock()
        assignee.id = 42
        sample_mri.merge_request_payload.assignees = [assignee]
        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = uuid.uuid4()

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[42],  # Matches assignee
                new_commits_revoke_approvals=False,
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["update_only"] is False


class TestRaceConditions:
    """Test race condition scenarios (workflow section: Race Condition Protections)."""

    async def test_fingerprint_computed_early(self, sample_mr_payload, sample_mri, mock_database):
        """Test that fingerprint is computed at start for deduplication."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("webhook.merge_request.logger") as mock_logger,
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            expected_fingerprint = hashlib.sha256(b'{"test":"mr"}').hexdigest()
            info_calls = [call for call in mock_logger.info.call_args_list if "fingerprint" in str(call)]
            assert len(info_calls) > 0
            assert expected_fingerprint in str(info_calls[0])

    async def test_message_id_update_after_creation(self, sample_mr_payload, sample_mri, mock_database):
        """Test that message_id is persisted after creation (race protection)."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=None, ref_id=123)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            new_message_id = uuid.uuid4()
            mock_create.return_value = new_message_id

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            conn = mock_database.connection
            assert conn.execute.called
            call_args = conn.execute.call_args[0][0]
            assert "UPDATE merge_request_message_ref" in call_args
            assert "SET message_id = $1" in call_args

    async def test_out_of_order_event_skipped(self, sample_mr_payload, sample_mri, mock_database):
        """Test that events with older updated_at are skipped."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4(), ref_id=123)
        mock_ref.last_processed_updated_at = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint") as mock_update,
            patch("webhook.merge_request.logger") as mock_logger,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_update.assert_not_called()
            warning_calls = [c for c in mock_logger.warning.call_args_list if "out of order" in str(c)]
            assert len(warning_calls) == 1

    async def test_duplicate_event_same_timestamp_and_fingerprint_skipped(
        self, sample_mr_payload, sample_mri, mock_database
    ):
        """Test that duplicate events (same timestamp + fingerprint) are skipped."""
        from webhook.merge_request import merge_request

        payload_fingerprint = hashlib.sha256(sample_mr_payload.model_dump_json().encode("utf8")).hexdigest()
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4(), ref_id=123)
        mock_ref.last_processed_updated_at = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        mock_ref.last_processed_fingerprint = payload_fingerprint

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint") as mock_update,
            patch("webhook.merge_request.logger") as mock_logger,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_update.assert_not_called()
            debug_calls = [
                c for c in mock_logger.debug.call_args_list if "same timestamp and fingerprint" in str(c)
            ]
            assert len(debug_calls) == 1

    async def test_message_ref_no_message_id_conv_token_not_in_webhook_skipped(
        self, sample_mr_payload, sample_mri, mock_database
    ):
        """Test that message refs without message_id and conv_token not in webhook are skipped."""
        from webhook.merge_request import merge_request

        mock_ref = make_mock_ref("other-token", message_id=None, ref_id=123)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("webhook.merge_request.update_message_with_fingerprint") as mock_update,
            patch("webhook.merge_request.logger") as mock_logger,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_create.assert_not_called()
            mock_update.assert_not_called()
            debug_calls = [
                c for c in mock_logger.debug.call_args_list if "conv_token not in webhook" in str(c)
            ]
            assert len(debug_calls) == 1


class TestDraftAndWIPHandling:
    """Test draft and work-in-progress handling (workflow section 2)."""

    async def test_draft_mr_collapses_card(self, sample_mr_payload, sample_mri, mock_database):
        """Test that draft MR renders collapsed card."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.draft = True
        sample_mri.merge_request_payload.object_attributes.draft = True
        mock_ref = make_mock_ref("token1", message_id=uuid.uuid4())

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render") as mock_render,
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint"),
            patch("httpx.AsyncClient", MockAsyncHttpxClient),
        ):
            mock_render.return_value = {"card": "collapsed"}
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_render.assert_called_once()
            call_args = mock_render.call_args
            assert call_args[1]["collapsed"] is True
            assert call_args[1]["show_collapsible"] is True

    async def test_draft_mr_skips_new_message_creation(self, sample_mr_payload, sample_mri, mock_database):
        """Test that draft MR in non-open action sets update_only=True."""
        from webhook.merge_request import merge_request

        sample_mr_payload.object_attributes.action = "update"
        sample_mr_payload.object_attributes.draft = True
        mock_ref = make_mock_ref("token1", message_id=None)

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {"token1": mock_ref}
            mock_get_all.return_value = [mock_ref]
            mock_create.return_value = None

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert call_args[1]["update_only"] is True


class TestMultipleConversations:
    """Test handling multiple conversation tokens (workflow: Multi-Channel Processing)."""

    async def test_creates_message_in_all_conversations(self, sample_mr_payload, sample_mri, mock_database):
        """Test that messages are created in all specified conversations."""
        from webhook.merge_request import merge_request

        mock_refs = [
            make_mock_ref("token1", message_id=None, ref_id=1),
            make_mock_ref("token2", message_id=None, ref_id=2),
            make_mock_ref("token3", message_id=None, ref_id=3),
        ]

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.create_or_update_message") as mock_create,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {ref.conversation_token: ref for ref in mock_refs}
            mock_get_all.return_value = mock_refs
            mock_create.return_value = uuid.uuid4()

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1", "token2", "token3"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            assert mock_create.call_count == 3

    async def test_updates_existing_messages_in_all_conversations(
        self, sample_mr_payload, sample_mri, mock_database
    ):
        """Test that existing messages are updated in all conversations."""
        from webhook.merge_request import merge_request

        mock_refs = [
            make_mock_ref("token1", message_id=uuid.uuid4(), ref_id=1),
            make_mock_ref("token2", message_id=uuid.uuid4(), ref_id=2),
        ]

        with (
            patch("webhook.merge_request.dbh.get_or_create_merge_request_ref_id", return_value=1),
            patch("webhook.merge_request.dbh.update_merge_request_ref_payload", return_value=sample_mri),
            patch("webhook.merge_request.dbh.get_merge_request_ref_infos", return_value=sample_mri),
            patch("webhook.merge_request.render", return_value={"card": "data"}),
            patch("webhook.merge_request.database", mock_database),
            patch("webhook.messaging.database", mock_database),
            patch("webhook.merge_request.get_or_create_message_refs") as mock_get_refs,
            patch("webhook.merge_request.get_all_message_refs") as mock_get_all,
            patch("webhook.merge_request.update_message_with_fingerprint") as mock_update,
            patch("httpx.AsyncClient"),
        ):
            mock_get_refs.return_value = {ref.conversation_token: ref for ref in mock_refs}
            mock_get_all.return_value = mock_refs

            await merge_request(
                mr=sample_mr_payload,
                conversation_tokens=["token1", "token2"],
                participant_ids_filter=[],
                new_commits_revoke_approvals=False,
            )

            assert mock_update.call_count == 2
