#!/usr/bin/env python3
"""
E2E tests with real PostgreSQL database.

Tests application code (DBHelper, messaging) against a real database.
"""
import uuid
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.e2e


class ConnectionContextManager:
    """Wraps a connection to work with 'async with await db.acquire()'."""

    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *args):
        pass

    def __await__(self):
        """Make this awaitable - returns self as the context manager."""

        async def _await():
            return self

        return _await().__await__()


def make_mock_db(db_connection):
    """Create a mock database handler that uses the test connection."""
    mock_db = MagicMock()
    mock_db.acquire = MagicMock(return_value=ConnectionContextManager(db_connection))
    return mock_db


# =============================================================================
# DBHelper.get_gitlab_instance_id_from_url tests
# =============================================================================


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_from_url_creates_new(db_connection, clean_database):
    """Test get_gitlab_instance_id_from_url creates instance when not exists."""
    from db import DBHelper

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/project/repo")

    assert isinstance(result, int)
    assert result > 0

    row = await db_connection.fetchrow(
        "SELECT hostname FROM gitlab_mr_api.gitlab_instance WHERE gitlab_instance_id = $1",
        result,
    )
    assert row["hostname"] == "gitlab.example.com"


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_from_url_idempotent(db_connection, clean_database):
    """Test get_gitlab_instance_id_from_url returns same ID for same host."""
    from db import DBHelper

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result1 = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/project/repo")
        result2 = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/other/project")
        result3 = await dbh.get_gitlab_instance_id_from_url("https://GITLAB.EXAMPLE.COM/case/test")

    assert result1 == result2 == result3

    count = await db_connection.fetchval(
        "SELECT COUNT(*) FROM gitlab_mr_api.gitlab_instance WHERE hostname = 'gitlab.example.com'"
    )
    assert count == 1


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_from_url_different_hosts(db_connection, clean_database):
    """Test get_gitlab_instance_id_from_url creates separate instances for different hosts."""
    from db import DBHelper

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result1 = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/project")
        result2 = await dbh.get_gitlab_instance_id_from_url("https://gitlab.other.com/project")

    assert result1 != result2

    count = await db_connection.fetchval("SELECT COUNT(*) FROM gitlab_mr_api.gitlab_instance")
    assert count == 2


# =============================================================================
# DBHelper.get_mri_from_url_pid_mriid tests
# =============================================================================


@pytest.mark.asyncio
async def test_get_mri_from_url_pid_mriid_not_found(db_connection, clean_database):
    """Test get_mri_from_url_pid_mriid returns None when MR doesn't exist."""
    from db import DBHelper

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result = await dbh.get_mri_from_url_pid_mriid(
            url="https://gitlab.example.com/project",
            project_id=999,
            mr_iid=999,
        )

    assert result is None


# =============================================================================
# DBHelper._generic_norm_upsert tests
# =============================================================================


@pytest.mark.asyncio
async def test_generic_norm_upsert_insert_new(db_connection, clean_database):
    """Test _generic_norm_upsert inserts new record when not exists."""
    from db import DBHelper

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result = await dbh._generic_norm_upsert(
            table="gitlab_instance",
            identity_col="gitlab_instance_id",
            select_attrs={"hostname": "test.gitlab.com"},
        )

    assert isinstance(result, int)
    assert result > 0

    row = await db_connection.fetchrow(
        "SELECT hostname FROM gitlab_mr_api.gitlab_instance WHERE gitlab_instance_id = $1",
        result,
    )
    assert row["hostname"] == "test.gitlab.com"


@pytest.mark.asyncio
async def test_generic_norm_upsert_returns_existing(db_connection, clean_database):
    """Test _generic_norm_upsert returns existing record without duplicating."""
    from db import DBHelper

    await db_connection.execute(
        "INSERT INTO gitlab_mr_api.gitlab_instance (hostname) VALUES ('existing.gitlab.com')"
    )

    mock_db = make_mock_db(db_connection)
    dbh = DBHelper(mock_db)

    with patch("db.database", mock_db):
        result = await dbh._generic_norm_upsert(
            table="gitlab_instance",
            identity_col="gitlab_instance_id",
            select_attrs={"hostname": "existing.gitlab.com"},
        )

    assert isinstance(result, int)
    assert result > 0

    count = await db_connection.fetchval(
        "SELECT COUNT(*) FROM gitlab_mr_api.gitlab_instance WHERE hostname = 'existing.gitlab.com'"
    )
    assert count == 1


# =============================================================================
# Messaging ref tests (simplified - no internal function dependencies)
# =============================================================================


@pytest.mark.asyncio
async def test_get_or_create_message_refs_creates_new(db_connection, clean_database):
    """Test get_or_create_message_refs creates message refs for new MR."""
    from webhook.messaging import get_or_create_message_refs

    await db_connection.execute(
        "INSERT INTO gitlab_mr_api.gitlab_instance (hostname) VALUES ('gitlab.example.com')"
    )
    mr_ref_id = await db_connection.fetchval(
        """
        INSERT INTO gitlab_mr_api.merge_request_ref (
            gitlab_instance_id, gitlab_project_id, gitlab_merge_request_id,
            gitlab_merge_request_iid, merge_request_payload, merge_request_extra_state
        ) VALUES (1, 100, 1000, 1, '{}', '{"approvers": {}, "pipeline_statuses": {}, "emojis": {}}')
        RETURNING merge_request_ref_id
        """
    )

    mock_db = make_mock_db(db_connection)
    conv_tokens = [str(uuid.uuid4()), str(uuid.uuid4())]

    with patch("webhook.messaging.database", mock_db):
        refs = await get_or_create_message_refs(mr_ref_id, conv_tokens)

    assert len(refs) == 2
    assert all(ref.message_id is None for ref in refs.values())
    assert {str(ref.conversation_token) for ref in refs.values()} == set(conv_tokens)


@pytest.mark.asyncio
async def test_get_or_create_message_refs_idempotent(db_connection, clean_database):
    """Test get_or_create_message_refs returns same refs on subsequent calls."""
    from webhook.messaging import get_or_create_message_refs

    await db_connection.execute(
        "INSERT INTO gitlab_mr_api.gitlab_instance (hostname) VALUES ('gitlab.example.com')"
    )
    mr_ref_id = await db_connection.fetchval(
        """
        INSERT INTO gitlab_mr_api.merge_request_ref (
            gitlab_instance_id, gitlab_project_id, gitlab_merge_request_id,
            gitlab_merge_request_iid, merge_request_payload, merge_request_extra_state
        ) VALUES (1, 100, 1000, 1, '{}', '{"approvers": {}, "pipeline_statuses": {}, "emojis": {}}')
        RETURNING merge_request_ref_id
        """
    )

    mock_db = make_mock_db(db_connection)
    conv_token = str(uuid.uuid4())

    with patch("webhook.messaging.database", mock_db):
        refs1 = await get_or_create_message_refs(mr_ref_id, [conv_token])
        refs2 = await get_or_create_message_refs(mr_ref_id, [conv_token])

    assert refs1[conv_token].merge_request_message_ref_id == refs2[conv_token].merge_request_message_ref_id

    count = await db_connection.fetchval(
        "SELECT COUNT(*) FROM gitlab_mr_api.merge_request_message_ref WHERE merge_request_ref_id = $1",
        mr_ref_id,
    )
    assert count == 1
