#!/usr/bin/env python3
"""Tests for debounce and deduplication mechanisms."""

import datetime

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


class TestNoteHandlerDebounce:
    """Tests for note handler debounce behavior."""

    @pytest.fixture
    def mock_mri(self):
        """Mock MergeRequestInfos."""
        mri = MagicMock()
        mri.merge_request_ref_id = 1
        mri.merge_request_payload.object_attributes.draft = False
        mri.merge_request_payload.object_attributes.work_in_progress = False
        mri.merge_request_payload.object_attributes.state = "opened"
        mri.merge_request_payload.object_attributes.title = "Test MR"
        mri.merge_request_payload.project.path_with_namespace = "test/repo"
        mri.merge_request_payload.project.web_url = "https://gitlab.example.com/test/repo"
        mri.merge_request_extra_state.discussion_stats = None
        return mri

    @pytest.fixture
    def mock_note_payload(self):
        """Mock NotePayload."""
        payload = MagicMock()
        payload.merge_request = MagicMock()
        payload.merge_request.url = "https://gitlab.example.com/test/repo/-/merge_requests/1"
        payload.merge_request.iid = 1
        payload.project.id = 100
        payload.project.web_url = "https://gitlab.example.com/test/repo"
        payload.object_attributes.noteable_type = "MergeRequest"
        return payload

    @pytest.mark.asyncio
    async def test_first_note_processes_immediately(self, mock_mri, mock_note_payload):
        """First note event should process immediately."""
        with (
            patch("webhook.note.dbh") as mock_dbh,
            patch("webhook.note.fetch_and_persist_discussion_stats", new_callable=AsyncMock) as mock_fetch,
            patch("webhook.note.render") as mock_render,
            patch("webhook.note.get_all_message_refs", new_callable=AsyncMock) as mock_get_refs,
            patch("webhook.note.httpx.AsyncClient") as mock_client_class,
        ):
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=mock_mri)
            mock_dbh.upsert_pending_mr_refresh = AsyncMock(return_value=True)  # First event
            mock_fetch.return_value = None
            mock_render.return_value = {"type": "AdaptiveCard"}
            mock_get_refs.return_value = []

            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            from webhook.note import note

            result = await note(mock_note_payload)

        assert result["status"] == "ok"
        mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_subsequent_note_is_debounced(self, mock_mri, mock_note_payload):
        """Subsequent note events should be debounced."""
        with (
            patch("webhook.note.dbh") as mock_dbh,
            patch("webhook.note.fetch_and_persist_discussion_stats", new_callable=AsyncMock) as mock_fetch,
        ):
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=mock_mri)
            mock_dbh.upsert_pending_mr_refresh = AsyncMock(return_value=False)  # Debounced

            from webhook.note import note

            result = await note(mock_note_payload)

        assert result["status"] == "debounced"
        assert result["reason"] == "pending_catchup"
        mock_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_note_skipped_when_no_mr_ref(self, mock_note_payload):
        """Note should be skipped when MR ref not found."""
        with patch("webhook.note.dbh") as mock_dbh:
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=None)

            from webhook.note import note

            result = await note(mock_note_payload)

        assert result["status"] == "skipped"
        assert result["reason"] == "no_mr_ref"


class TestPendingRefreshCatchup:
    """Tests for periodic cleanup pending refresh processing."""

    @pytest.mark.asyncio
    async def test_process_pending_refreshes_empty(self):
        """No-op when no pending refreshes."""
        with (
            patch("periodic_cleanup.dbh") as mock_dbh,
            patch("periodic_cleanup.fetch_and_persist_discussion_stats", new_callable=AsyncMock),
        ):
            mock_dbh.get_pending_refreshes = AsyncMock(return_value=[])

            from periodic_cleanup import _process_pending_refreshes

            mock_client = AsyncMock()
            result = await _process_pending_refreshes(mock_client)

        assert result == 0

    @pytest.mark.asyncio
    async def test_process_pending_refreshes_success(self):
        """Successfully process pending refresh."""

        mock_payload = {
            "object_kind": "merge_request",
            "event_type": "merge_request",
            "repository": {"homepage": "https://gitlab.example.com/test/project", "name": "project"},
            "user": {"id": 1, "username": "testuser", "name": "Test User", "email": "test@example.com"},
            "project": {
                "id": 1,
                "path_with_namespace": "test/project",
                "web_url": "https://gitlab.example.com/test/project",
            },
            "object_attributes": {
                "id": 123,
                "iid": 1,
                "title": "Test MR",
                "created_at": "2025-01-01 00:00:00 UTC",
                "draft": False,
                "state": "opened",
                "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
                "action": "open",
                "updated_at": "2025-01-01 00:00:00 UTC",
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
        }

        mock_extra_state = {
            "version": 1,
            "opener": {"id": 1, "username": "testuser", "name": "Test User"},
            "approvers": {},
            "pipeline_statuses": {},
            "emojis": {},
        }

        pending_row = {
            "merge_request_ref_id": 1,
            "payload_type": "note",
            "first_event_at": datetime.datetime.now(tz=datetime.UTC),
            "last_event_at": datetime.datetime.now(tz=datetime.UTC),
            "merge_request_payload": mock_payload,
            "merge_request_extra_state": mock_extra_state,
            "head_pipeline_id": None,
        }

        with (
            patch("periodic_cleanup.dbh") as mock_dbh,
            patch(
                "periodic_cleanup.fetch_and_persist_discussion_stats", new_callable=AsyncMock
            ) as mock_fetch,
            patch("periodic_cleanup.render") as mock_render,
            patch("periodic_cleanup.get_all_message_refs", new_callable=AsyncMock) as mock_get_refs,
        ):
            mock_dbh.get_pending_refreshes = AsyncMock(return_value=[pending_row])
            mock_dbh.delete_pending_refresh = AsyncMock()
            mock_fetch.return_value = None
            mock_render.return_value = {"type": "AdaptiveCard"}
            mock_get_refs.return_value = []

            from periodic_cleanup import _process_pending_refreshes

            mock_client = AsyncMock()
            result = await _process_pending_refreshes(mock_client)

        assert result == 1
        mock_dbh.delete_pending_refresh.assert_called_once_with(1)


class TestPreCheckDeduplication:
    """Tests for pre-check deduplication in webhook handlers."""

    @pytest.mark.asyncio
    async def test_merge_request_skips_stats_fetch_when_no_update_needed(self, sample_merge_request_payload):
        """merge_request handler should skip stats fetch when all messages up-to-date."""
        payload = sample_merge_request_payload
        payload.object_attributes.action = "update"
        payload.object_attributes.state = "opened"

        mock_mri = MagicMock()
        mock_mri.merge_request_ref_id = 1
        mock_mri.merge_request_payload = payload
        mock_mri.merge_request_extra_state.opener.id = 1
        mock_mri.merge_request_extra_state.discussion_stats = None

        with (
            patch("webhook.merge_request.dbh") as mock_dbh,
            patch("webhook.merge_request.database") as mock_database,
            patch(
                "webhook.merge_request.fetch_and_persist_discussion_stats", new_callable=AsyncMock
            ) as mock_fetch,
            patch("webhook.merge_request.render") as mock_render,
            patch("webhook.merge_request.get_or_create_message_refs", new_callable=AsyncMock),
            patch("webhook.merge_request.get_all_message_refs", new_callable=AsyncMock) as mock_get_refs,
            patch("webhook.merge_request.httpx.AsyncClient") as mock_client_class,
        ):
            mock_dbh.get_or_create_merge_request_ref_id = AsyncMock(return_value=1)
            mock_dbh.update_merge_request_ref_payload = AsyncMock(return_value=mock_mri)
            mock_dbh.get_merge_request_ref_infos = AsyncMock(return_value=mock_mri)
            mock_dbh.any_message_needs_update = AsyncMock(return_value=False)  # No update needed

            mock_conn = AsyncMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)
            mock_database.acquire = AsyncMock(
                return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn))
            )

            mock_render.return_value = {"type": "AdaptiveCard"}
            mock_get_refs.return_value = []

            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client

            from webhook.merge_request import merge_request

            await merge_request(payload, ["conv-token"], [], False)

        mock_fetch.assert_not_called()
