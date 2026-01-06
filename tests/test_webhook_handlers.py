#!/usr/bin/env python3
"""
Tests for emoji and pipeline webhook handlers.
"""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_database():
    with patch("webhook.emoji.database") as mock_db:
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.fetch = AsyncMock()
        mock_conn.fetchrow = AsyncMock()
        mock_conn.fetchval = AsyncMock()

        mock_acquire_ctx = MagicMock()
        mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_ctx.__aexit__ = AsyncMock()
        mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

        yield mock_db, mock_conn


@pytest.fixture
def sample_emoji_payload():
    payload = MagicMock()
    payload.object_attributes.awardable_type = "MergeRequest"
    payload.object_attributes.name = "thumbsup"
    payload.object_attributes.user_id = 42
    payload.object_attributes.awarded_on_url = "https://gitlab.com/test/project/-/merge_requests/1"
    payload.merge_request.target_project_id = 123
    payload.merge_request.iid = 1
    payload.event_type = "award"
    payload.object_kind = "emoji"
    payload.user.id = 42
    payload.user.name = "Test User"
    payload.user.username = "testuser"
    payload.model_dump_json.return_value = '{"test":"data"}'
    payload.model_dump.return_value = {"test": "data"}
    return payload


@pytest.fixture
def sample_pipeline_payload():
    payload = MagicMock()
    payload.object_attributes.id = 999
    payload.object_attributes.status = "success"
    payload.model_dump_json.return_value = '{"pipeline":"data"}'
    payload.model_dump.return_value = {"pipeline": "data"}
    return payload


@pytest.fixture
def sample_mri():
    mri = MagicMock()
    mri.merge_request_ref_id = 1
    mri.merge_request_payload.object_attributes.state = "opened"
    mri.merge_request_payload.object_attributes.title = "Test MR"
    mri.merge_request_payload.object_attributes.updated_at = "2025-01-01 00:00:00 UTC"
    mri.merge_request_payload.project.path_with_namespace = "test/project"
    return mri


class TestEmojiHandler:
    async def test_emoji_skips_non_merge_request_awards(self, sample_emoji_payload):
        from webhook.emoji import emoji

        sample_emoji_payload.object_attributes.awardable_type = "Issue"

        result = await emoji(sample_emoji_payload, ["token-1"])

        assert result["status"] == "skipped"
        assert result["reason"] == "not_mr_emoji"

    async def test_emoji_queues_for_processing(self, sample_emoji_payload, sample_mri):
        """Emoji event should persist state and queue for processing."""
        from webhook.emoji import emoji

        mock_emoji_entry = MagicMock()
        mock_emoji_entry.model_dump.return_value = {"emoji": "data"}

        with (
            patch("webhook.emoji.database") as mock_db,
            patch("webhook.emoji.dbh") as mock_dbh,
            patch("webhook.emoji.periodic_cleanup") as mock_cleanup,
            patch("webhook.emoji.EmojiEntry", return_value=mock_emoji_entry),
        ):
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=sample_mri)
            mock_dbh.upsert_pending_mr_refresh = AsyncMock(return_value=True)

            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value={"merge_request_ref_id": 1})

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            result = await emoji(sample_emoji_payload, ["token-1"])

        assert result["status"] == "queued"
        mock_dbh.upsert_pending_mr_refresh.assert_called_once()
        mock_cleanup.reschedule.assert_called_once()

    async def test_emoji_skipped_when_no_mr_ref(self, sample_emoji_payload):
        """Emoji should be skipped when MR ref not found."""
        from webhook.emoji import emoji

        with patch("webhook.emoji.dbh") as mock_dbh:
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=None)

            result = await emoji(sample_emoji_payload, ["token-1"])

        assert result["status"] == "skipped"
        assert result["reason"] == "no_mr_ref"

    async def test_emoji_persists_state_before_queuing(self, sample_emoji_payload, sample_mri):
        """Emoji handler should persist emoji state to DB before queuing."""
        from webhook.emoji import emoji

        mock_emoji_entry = MagicMock()
        mock_emoji_entry.model_dump.return_value = {"emoji": "data"}

        with (
            patch("webhook.emoji.database") as mock_db,
            patch("webhook.emoji.dbh") as mock_dbh,
            patch("webhook.emoji.periodic_cleanup"),
            patch("webhook.emoji.EmojiEntry", return_value=mock_emoji_entry),
        ):
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=sample_mri)
            mock_dbh.upsert_pending_mr_refresh = AsyncMock(return_value=True)

            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value={"merge_request_ref_id": 1})

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            await emoji(sample_emoji_payload, ["token-1"])

        mock_conn.fetchrow.assert_called_once()
        call_args = mock_conn.fetchrow.call_args[0][0]
        assert "UPDATE merge_request_ref" in call_args
        assert "jsonb_set" in call_args
        assert "emojis" in str(mock_conn.fetchrow.call_args)

    async def test_emoji_skipped_when_db_update_fails(self, sample_emoji_payload, sample_mri):
        from webhook.emoji import emoji

        mock_emoji_entry = MagicMock()
        mock_emoji_entry.model_dump.return_value = {"emoji": "data"}

        with (
            patch("webhook.emoji.database") as mock_db,
            patch("webhook.emoji.dbh") as mock_dbh,
            patch("webhook.emoji.EmojiEntry", return_value=mock_emoji_entry),
        ):
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=sample_mri)

            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            result = await emoji(sample_emoji_payload, ["token-1"])

        assert result["status"] == "skipped"
        assert result["reason"] == "update_failed"


class TestPipelineHandler:
    async def test_pipeline_updates_extra_state_and_messages(self, sample_pipeline_payload, sample_mri):
        from webhook.pipeline import pipeline

        with (
            patch("webhook.pipeline.database") as mock_db,
            patch("webhook.pipeline.render") as mock_render,
            patch("webhook.pipeline.compute_mri_fingerprint", return_value="test-fp"),
            patch("webhook.pipeline.update_all_messages_transactional") as mock_update,
            patch("webhook.pipeline.MergeRequestInfos", return_value=sample_mri),
        ):
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(
                return_value={
                    "merge_request_ref_id": 1,
                    "merge_request_payload": sample_mri.merge_request_payload,
                    "merge_request_extra_state": {},
                    "head_pipeline_id": 999,
                }
            )

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            mock_render.return_value = {"type": "AdaptiveCard"}
            mock_update.return_value = 1

            result = await pipeline(sample_pipeline_payload, ["token-1"])

            assert result is not None
            mock_update.assert_called_once()

    async def test_pipeline_returns_none_when_no_mr_matches(self, sample_pipeline_payload):
        from webhook.pipeline import pipeline

        with patch("webhook.pipeline.database") as mock_db:
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value=None)

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            result = await pipeline(sample_pipeline_payload, ["token-1"])

            assert result is None

    async def test_pipeline_passes_fingerprint_to_update(self, sample_pipeline_payload, sample_mri):
        from webhook.pipeline import pipeline

        with (
            patch("webhook.pipeline.database") as mock_db,
            patch("webhook.pipeline.render") as mock_render,
            patch("webhook.pipeline.compute_mri_fingerprint", return_value="test-fp"),
            patch("webhook.pipeline.update_all_messages_transactional") as mock_update,
            patch("webhook.pipeline.MergeRequestInfos", return_value=sample_mri),
        ):
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(
                return_value={
                    "merge_request_ref_id": 1,
                    "merge_request_payload": sample_mri.merge_request_payload,
                    "merge_request_extra_state": {},
                    "head_pipeline_id": 999,
                }
            )

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            mock_render.return_value = {"type": "AdaptiveCard"}
            mock_update.return_value = 1

            await pipeline(sample_pipeline_payload, ["token-1"])

            mock_update.assert_called_once()
            call_args = mock_update.call_args
            assert call_args[0][3] == "test-fp"

    async def test_pipeline_returns_none_when_update_fails(self, sample_pipeline_payload):
        from webhook.pipeline import pipeline

        with patch("webhook.pipeline.database") as mock_db:
            mock_conn = MagicMock()
            mock_conn.fetchval = AsyncMock(return_value=None)
            mock_conn.fetchrow = AsyncMock(return_value=None)

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            result = await pipeline(sample_pipeline_payload, ["token-1"])

            assert result is None

    async def test_pipeline_updates_pipeline_status_in_database(self, sample_pipeline_payload, sample_mri):
        from webhook.pipeline import pipeline

        with (
            patch("webhook.pipeline.database") as mock_db,
            patch("webhook.pipeline.render") as mock_render,
            patch("webhook.pipeline.compute_mri_fingerprint", return_value="test-fp"),
            patch("webhook.pipeline.update_all_messages_transactional"),
            patch("webhook.pipeline.MergeRequestInfos", return_value=sample_mri),
        ):
            mock_conn = MagicMock()
            mock_conn.fetchval = AsyncMock(return_value=None)
            mock_conn.fetchrow = AsyncMock(
                return_value={
                    "merge_request_ref_id": 1,
                    "merge_request_payload": sample_mri.merge_request_payload,
                    "merge_request_extra_state": {},
                    "head_pipeline_id": 999,
                }
            )

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            mock_render.return_value = {"type": "AdaptiveCard"}

            await pipeline(sample_pipeline_payload, ["token-1"])

            assert mock_conn.fetchrow.called
            call_args = mock_conn.fetchrow.call_args[0][0]
            assert "UPDATE merge_request_ref" in call_args
            assert "jsonb_set" in call_args


class TestFingerprintDeduplication:
    """Test webhook fingerprint deduplication across handlers."""

    async def test_pipeline_logs_on_entry(self, sample_pipeline_payload):
        from webhook.pipeline import pipeline

        with patch("webhook.pipeline.database") as mock_db, patch("webhook.pipeline.logger") as mock_logger:
            mock_conn = MagicMock()
            mock_conn.fetchval = AsyncMock(return_value=None)
            mock_conn.fetchrow = AsyncMock(return_value=None)

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            await pipeline(sample_pipeline_payload, ["token-1"])

            info_calls = [call for call in mock_logger.info.call_args_list if "pipeline" in str(call)]
            assert len(info_calls) > 0


class TestCriticalErrorPaths:
    """Test critical error handling paths."""

    async def test_emoji_survives_missing_mr(self, sample_emoji_payload):
        from webhook.emoji import emoji

        with patch("webhook.emoji.dbh") as mock_dbh:
            mock_dbh.get_mri_from_url_pid_mriid = AsyncMock(return_value=None)
            result = await emoji(sample_emoji_payload, ["token-1"])
            assert result["status"] == "skipped"

    async def test_pipeline_survives_database_update_failure(self, sample_pipeline_payload):
        from webhook.pipeline import pipeline

        with patch("webhook.pipeline.database") as mock_db:
            mock_conn = MagicMock()
            mock_conn.fetchval = AsyncMock(return_value=None)
            mock_conn.fetchrow = AsyncMock(return_value=None)

            mock_acquire_ctx = MagicMock()
            mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_acquire_ctx.__aexit__ = AsyncMock()
            mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

            result = await pipeline(sample_pipeline_payload, ["token-1"])
            assert result is None
