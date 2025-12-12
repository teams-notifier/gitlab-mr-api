#!/usr/bin/env python3
import datetime
import uuid

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from webhook.messaging import MRMessRef
from webhook.messaging import create_or_update_message
from webhook.messaging import get_or_create_message_refs
from webhook.messaging import update_all_messages_transactional


@pytest.fixture
def mock_database():
    with patch("webhook.messaging.database") as mock_db:
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()
        mock_conn.fetch = AsyncMock()
        mock_conn.fetchrow = AsyncMock()
        mock_conn.fetchval = AsyncMock()

        mock_transaction = MagicMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock()
        mock_conn.transaction = MagicMock(return_value=mock_transaction)

        mock_acquire_ctx = MagicMock()
        mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_ctx.__aexit__ = AsyncMock()
        mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

        yield mock_db, mock_conn


@pytest.fixture
def mock_http_client():
    client = AsyncMock(spec=httpx.AsyncClient)
    client.request = AsyncMock()
    return client


@pytest.fixture
def sample_mr_mess_ref():
    return MRMessRef(
        merge_request_message_ref_id=123,
        conversation_token=uuid.uuid4(),
        message_id=uuid.uuid4(),
        last_processed_fingerprint=None,
    )


@pytest.fixture
def sample_mri():
    mri = MagicMock()
    mri.merge_request_ref_id = 1
    mri.merge_request_payload.object_attributes.state = "opened"
    mri.merge_request_payload.object_attributes.title = "Test MR"
    mri.merge_request_payload.project.path_with_namespace = "test/project"
    return mri


class TestGetOrCreateMessageRefs:
    async def test_returns_existing_refs(self, mock_database):
        mock_db, mock_conn = mock_database
        existing_ref_id = uuid.uuid4()

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": existing_ref_id,
                "message_id": uuid.uuid4(),
                "last_processed_fingerprint": None,
            }
        ]

        result = await get_or_create_message_refs(
            merge_request_ref_id=1,
            conv_tokens=[str(existing_ref_id)],
        )

        assert str(existing_ref_id) in result
        assert result[str(existing_ref_id)].merge_request_message_ref_id == 1
        mock_conn.execute.assert_called_once()

    async def test_creates_new_refs_for_missing_tokens(self, mock_database):
        mock_db, mock_conn = mock_database
        new_token = str(uuid.uuid4())
        new_ref_id = uuid.uuid4()

        mock_conn.fetch.return_value = []
        mock_conn.fetchrow.return_value = {
            "merge_request_message_ref_id": 2,
            "conversation_token": new_ref_id,
            "message_id": None,
            "last_processed_fingerprint": None,
        }

        result = await get_or_create_message_refs(
            merge_request_ref_id=1,
            conv_tokens=[new_token],
        )

        assert len(result) == 1
        mock_conn.fetchrow.assert_called_once()

    async def test_locks_merge_request_ref(self, mock_database):
        mock_db, mock_conn = mock_database
        mock_conn.fetch.return_value = []

        await get_or_create_message_refs(
            merge_request_ref_id=42,
            conv_tokens=[],
        )

        execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
        assert any("FOR UPDATE" in call for call in execute_calls)
        assert any("merge_request_ref_id = $1" in call for call in execute_calls)


class TestCreateOrUpdateMessage:
    async def test_create_new_message_with_card(self, mock_http_client, mock_database, sample_mr_mess_ref):
        mock_db, mock_conn = mock_database
        sample_mr_mess_ref.message_id = None
        new_message_id = str(uuid.uuid4())

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"message_id": new_message_id}
        mock_http_client.request.return_value = response_mock

        mock_conn.fetchrow.return_value = {"merge_request_message_ref_id": 123}

        card = {"type": "AdaptiveCard", "body": []}
        summary = "Test summary"

        result = await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            card=card,
            summary=summary,
        )

        assert result == uuid.UUID(new_message_id)
        mock_http_client.request.assert_called_once()
        call_args = mock_http_client.request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[1]["json"]["card"] == card
        assert call_args[1]["json"]["summary"] == summary

    async def test_update_existing_message(self, mock_http_client, mock_database, sample_mr_mess_ref):
        mock_db, mock_conn = mock_database
        existing_message_id = sample_mr_mess_ref.message_id

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"message_id": str(existing_message_id)}
        mock_http_client.request.return_value = response_mock

        card = {"type": "AdaptiveCard", "body": []}

        result = await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            card=card,
        )

        assert result == existing_message_id
        mock_http_client.request.assert_called_once()
        call_args = mock_http_client.request.call_args
        assert call_args[0][0] == "PATCH"
        assert call_args[1]["json"]["message_id"] == str(existing_message_id)

    async def test_update_only_skips_creation(self, mock_http_client, sample_mr_mess_ref):
        sample_mr_mess_ref.message_id = None

        result = await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            card={"body": []},
            update_only=True,
        )

        assert result is None
        mock_http_client.request.assert_not_called()

    async def test_deletes_duplicate_message_on_db_conflict(
        self, mock_http_client, mock_database, sample_mr_mess_ref
    ):
        mock_db, mock_conn = mock_database
        sample_mr_mess_ref.message_id = None
        new_message_id = str(uuid.uuid4())

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"message_id": new_message_id}
        mock_http_client.request.return_value = response_mock

        mock_conn.fetchrow.return_value = None

        await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            card={"body": []},
        )

        assert mock_http_client.request.call_count == 2
        delete_call = mock_http_client.request.call_args_list[1]
        assert delete_call[0][0] == "DELETE"
        assert delete_call[1]["json"]["message_id"] == new_message_id

    async def test_error_on_create_raises_exception(self, mock_http_client, sample_mr_mess_ref):
        sample_mr_mess_ref.message_id = None

        mock_http_client.request.side_effect = httpx.HTTPError("Connection failed")

        with pytest.raises(httpx.HTTPError):
            await create_or_update_message(
                mock_http_client,
                sample_mr_mess_ref,
                card={"body": []},
            )


class TestUpdateAllMessagesTransactional:
    async def test_updates_all_messages_successfully(self, mock_database, sample_mri):
        mock_db, mock_conn = mock_database
        message_id_1 = uuid.uuid4()
        message_id_2 = uuid.uuid4()

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id_1,
            },
            {
                "merge_request_message_ref_id": 2,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id_2,
            },
        ]

        card = {"type": "AdaptiveCard", "body": []}
        summary = "Test summary"
        fingerprint = "abc123"

        with patch("webhook.messaging.httpx.AsyncClient") as mock_client_class:
            mock_client_instance = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client_instance.request.return_value = mock_response

            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                card,
                summary,
                fingerprint,
                datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "test-action",
            )

            assert count == 2
            assert mock_client_instance.request.call_count == 2
            mock_conn.fetch.assert_called_once()

    async def test_skips_messages_without_message_id(self, mock_database, sample_mri):
        mock_db, mock_conn = mock_database

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": None,
            },
        ]

        card = {"type": "AdaptiveCard"}
        fingerprint = "def456"

        with patch("webhook.messaging.httpx.AsyncClient") as mock_client_class:
            mock_client_instance = AsyncMock()
            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                card,
                "summary",
                fingerprint,
                datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "test-action",
            )

            assert count == 0
            mock_client_instance.request.assert_not_called()

    async def test_stores_fingerprint_after_updates(self, mock_database, sample_mri):
        mock_db, mock_conn = mock_database
        message_id = uuid.uuid4()

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id,
            }
        ]

        fingerprint = "unique123"

        with patch("webhook.messaging.httpx.AsyncClient") as mock_client_class:
            mock_client_instance = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client_instance.request.return_value = mock_response

            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                fingerprint,
                datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "test-action",
            )

            assert count == 1

    async def test_schedules_message_deletion_when_requested(self, mock_database, sample_mri):
        mock_db, mock_conn = mock_database
        message_id = uuid.uuid4()

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id,
            }
        ]

        with patch("webhook.messaging.httpx.AsyncClient") as mock_client_class:
            mock_client_instance = AsyncMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client_instance.request.return_value = mock_response

            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                "fingerprint",
                datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "test-action",
                schedule_deletion=True,
                deletion_delay=datetime.timedelta(seconds=300),
            )

            execute_calls = [str(call[0][0]) for call in mock_conn.execute.call_args_list]
            assert any("msg_to_delete" in call for call in execute_calls)
            assert any("DELETE FROM merge_request_message_ref" in call for call in execute_calls)

    async def test_continues_on_http_error_per_message(self, mock_database, sample_mri):
        mock_db, mock_conn = mock_database
        message_id_1 = uuid.uuid4()
        message_id_2 = uuid.uuid4()

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id_1,
            },
            {
                "merge_request_message_ref_id": 2,
                "conversation_token": uuid.uuid4(),
                "message_id": message_id_2,
            },
        ]

        with patch("webhook.messaging.httpx.AsyncClient") as mock_client_class:
            mock_client_instance = AsyncMock()

            call_count = 0

            async def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise httpx.HTTPError("Network error")
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.raise_for_status = MagicMock()
                return mock_response

            mock_client_instance.request.side_effect = side_effect

            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                "fingerprint",
                datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "test-action",
            )

            assert count == 1
            assert mock_client_instance.request.call_count == 2

    async def test_update_raises_error_on_http_failure(
        self, mock_http_client, mock_database, sample_mr_mess_ref
    ):
        mock_db, mock_conn = mock_database

        mock_http_client.request.side_effect = httpx.HTTPError("Connection failed")

        with pytest.raises(httpx.HTTPError):
            await create_or_update_message(
                mock_http_client,
                sample_mr_mess_ref,
                card={"body": []},
            )

    async def test_create_with_text_message(self, mock_http_client, mock_database, sample_mr_mess_ref):
        mock_db, mock_conn = mock_database
        sample_mr_mess_ref.message_id = None
        new_message_id = str(uuid.uuid4())

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"message_id": new_message_id}
        mock_http_client.request.return_value = response_mock

        mock_conn.fetchrow.return_value = {"merge_request_message_ref_id": 123}

        result = await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            message_text="Hello World",
        )

        assert result == uuid.UUID(new_message_id)
        call_args = mock_http_client.request.call_args
        assert call_args[1]["json"]["text"] == "Hello World"

    async def test_handles_delete_failure_gracefully(
        self, mock_http_client, mock_database, sample_mr_mess_ref
    ):
        mock_db, mock_conn = mock_database
        sample_mr_mess_ref.message_id = None
        new_message_id = str(uuid.uuid4())

        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"message_id": new_message_id}

        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return response_mock
            raise httpx.HTTPError("Delete failed")

        mock_http_client.request.side_effect = side_effect
        mock_conn.fetchrow.return_value = None

        await create_or_update_message(
            mock_http_client,
            sample_mr_mess_ref,
            card={"body": []},
        )

        assert mock_http_client.request.call_count == 2


class TestUpdateAllMessagesTransactionalOrdering:
    """Tests for out-of-order and duplicate event handling in transactional updates."""

    async def test_out_of_order_event_skipped_in_transactional(self, mock_database):
        """Test that out-of-order events are skipped in transactional update."""
        mock_db, mock_conn = mock_database
        stored_time = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        payload_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": uuid.uuid4(),
                "last_processed_fingerprint": "old-fingerprint",
                "last_processed_updated_at": stored_time,
            }
        ]

        sample_mri = MagicMock()
        sample_mri.merge_request_ref_id = 1

        with (
            patch("webhook.messaging.logger") as mock_logger,
            patch("httpx.AsyncClient") as mock_client_class,
        ):
            mock_client_instance = MagicMock()
            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                "fingerprint",
                payload_time,
                "test-action",
            )

            assert count == 0
            warning_calls = [c for c in mock_logger.warning.call_args_list if "out of order" in str(c)]
            assert len(warning_calls) == 1
            mock_client_instance.request.assert_not_called()

    async def test_duplicate_event_same_timestamp_fingerprint_skipped_in_transactional(self, mock_database):
        """Test that duplicate events (same timestamp + fingerprint) are skipped."""
        mock_db, mock_conn = mock_database
        same_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        same_fingerprint = "same-fingerprint"

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": uuid.uuid4(),
                "last_processed_fingerprint": same_fingerprint,
                "last_processed_updated_at": same_time,
            }
        ]

        sample_mri = MagicMock()
        sample_mri.merge_request_ref_id = 1

        with (
            patch("webhook.messaging.logger") as mock_logger,
            patch("httpx.AsyncClient") as mock_client_class,
        ):
            mock_client_instance = MagicMock()
            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                same_fingerprint,
                same_time,
                "test-action",
            )

            assert count == 0
            debug_calls = [
                c for c in mock_logger.debug.call_args_list if "same timestamp and fingerprint" in str(c)
            ]
            assert len(debug_calls) == 1
            mock_client_instance.request.assert_not_called()

    async def test_newer_event_processed_in_transactional(self, mock_database):
        """Test that newer events are processed normally."""
        mock_db, mock_conn = mock_database
        stored_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        payload_time = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)

        mock_conn.fetch.return_value = [
            {
                "merge_request_message_ref_id": 1,
                "conversation_token": uuid.uuid4(),
                "message_id": uuid.uuid4(),
                "last_processed_fingerprint": "old-fingerprint",
                "last_processed_updated_at": stored_time,
            }
        ]

        sample_mri = MagicMock()
        sample_mri.merge_request_ref_id = 1

        with (
            patch("webhook.messaging.logger"),
            patch("httpx.AsyncClient") as mock_client_class,
        ):
            mock_client_instance = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client_instance.request = AsyncMock(return_value=mock_response)
            mock_client_ctx = MagicMock()
            mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_ctx.__aexit__ = AsyncMock()
            mock_client_class.return_value = mock_client_ctx

            count = await update_all_messages_transactional(
                sample_mri,
                {"body": []},
                "summary",
                "new-fingerprint",
                payload_time,
                "test-action",
            )

            assert count == 1
            mock_client_instance.request.assert_called_once()


class TestMRMessRef:
    def test_creates_with_all_fields(self):
        ref_id = 123
        conv_token = uuid.uuid4()
        msg_id = uuid.uuid4()

        ref = MRMessRef(
            merge_request_message_ref_id=ref_id,
            conversation_token=conv_token,
            message_id=msg_id,
        )

        assert ref.merge_request_message_ref_id == ref_id
        assert ref.conversation_token == conv_token
        assert ref.message_id == msg_id

    def test_allows_none_message_id(self):
        ref = MRMessRef(
            merge_request_message_ref_id=1,
            conversation_token=uuid.uuid4(),
            message_id=None,
            last_processed_fingerprint=None,
        )

        assert ref.message_id is None
        assert ref.last_processed_fingerprint is None
