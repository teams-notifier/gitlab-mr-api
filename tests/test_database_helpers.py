#!/usr/bin/env python3
"""
Tests for database helper functions.

Tests DBHelper methods:
- get_gitlab_instance_id_from_url
- get_merge_request_ref_infos
- _generic_norm_upsert
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import asyncpg
import pytest


class MockRecord(dict[str, Any]):
    """Mock asyncpg.Record that behaves like a dict but passes isinstance checks."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


# Store original Record for restoration
_original_record = asyncpg.Record


@pytest.fixture(autouse=True)
def patch_asyncpg_record():
    """Temporarily patch asyncpg.Record for isinstance checks in unit tests."""
    asyncpg.Record = MockRecord  # type: ignore[misc]
    yield
    asyncpg.Record = _original_record


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_from_url_new_instance(mock_database):
    """Test getting GitLab instance ID for new instance."""
    from db import DBHelper

    connection = mock_database.connection
    connection.fetchrow.side_effect = [
        None,
        {"gitlab_instance_id": 1},
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/test/project")

    assert result == 1

    insert_calls = [call for call in connection.fetchrow.call_args_list if "INSERT INTO" in str(call)]
    assert len(insert_calls) > 0


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_from_url_existing_instance(mock_database):
    """Test getting GitLab instance ID for existing instance."""
    from db import DBHelper

    connection = mock_database.connection
    connection.fetchrow.return_value = {"gitlab_instance_id": 42}

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_gitlab_instance_id_from_url("https://gitlab.example.com/test/project")

    assert result == 42


@pytest.mark.asyncio
async def test_get_gitlab_instance_id_invalid_url(mock_database):
    """Test that invalid URL raises ValueError."""
    from db import DBHelper

    dbh = DBHelper(mock_database)

    with pytest.raises(ValueError, match="unable to determine gitlab host"):
        await dbh.get_gitlab_instance_id_from_url("invalid-url")


@pytest.mark.asyncio
async def test_get_merge_request_ref_infos_new_mr(mock_database, sample_merge_request_payload):
    """Test getting MR ref infos for new MR."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        MockRecord({"gitlab_instance_id": 1}),
        None,
        MockRecord(
            {
                "merge_request_ref_id": 1,
                "merge_request_payload": sample_merge_request_payload.model_dump(),
                "merge_request_extra_state": {
                    "version": 1,
                    "opener": {
                        "id": sample_merge_request_payload.user.id,
                        "name": sample_merge_request_payload.user.name,
                        "username": sample_merge_request_payload.user.username,
                    },
                    "approvers": {},
                    "pipeline_statuses": {},
                    "emojis": {},
                },
                "head_pipeline_id": None,
            }
        ),
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_merge_request_ref_infos(sample_merge_request_payload)

    assert result.merge_request_ref_id == 1
    assert result.merge_request_extra_state.opener.id == sample_merge_request_payload.user.id


@pytest.mark.asyncio
async def test_get_merge_request_ref_infos_existing_mr(mock_database, sample_merge_request_payload):
    """Test getting MR ref infos for existing MR (update)."""
    from db import DBHelper

    connection = mock_database.connection

    existing_extra_state = {
        "version": 1,
        "opener": {
            "id": sample_merge_request_payload.user.id,
            "name": sample_merge_request_payload.user.name,
            "username": sample_merge_request_payload.user.username,
        },
        "approvers": {"1": {"id": 1, "name": "Approver", "username": "approver", "status": "approved"}},
        "pipeline_statuses": {},
        "emojis": {},
    }

    connection.fetchrow.side_effect = [
        MockRecord({"gitlab_instance_id": 1}),
        MockRecord(
            {
                "merge_request_ref_id": 1,
                "merge_request_payload": sample_merge_request_payload.model_dump(),
                "merge_request_extra_state": existing_extra_state,
                "head_pipeline_id": 123,
            }
        ),
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_merge_request_ref_infos(sample_merge_request_payload)

    assert result.merge_request_ref_id == 1
    assert result.head_pipeline_id == 123
    assert "1" in result.merge_request_extra_state.approvers


@pytest.mark.asyncio
async def test_get_mri_from_url_pid_mriid_found(mock_database, sample_merge_request_payload):
    """Test getting MRI by URL, project ID, and MR IID when found."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        {"gitlab_instance_id": 1},
        {
            "merge_request_ref_id": 1,
            "merge_request_payload": sample_merge_request_payload.model_dump(),
            "merge_request_extra_state": {
                "version": 1,
                "opener": {
                    "id": 1,
                    "name": "Test User",
                    "username": "testuser",
                },
                "approvers": {},
                "pipeline_statuses": {},
                "emojis": {},
            },
            "head_pipeline_id": None,
        },
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_mri_from_url_pid_mriid(
            url="https://gitlab.example.com/test/project",
            project_id=100,
            mr_iid=1,
        )

    assert result is not None
    assert result.merge_request_ref_id == 1


@pytest.mark.asyncio
async def test_get_mri_from_url_pid_mriid_not_found(mock_database):
    """Test getting MRI when not found returns None."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        {"gitlab_instance_id": 1},
        None,
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh.get_mri_from_url_pid_mriid(
            url="https://gitlab.example.com/test/project",
            project_id=100,
            mr_iid=999,
        )

    assert result is None


@pytest.mark.asyncio
async def test_generic_norm_upsert_insert_new_record(mock_database):
    """Test generic upsert when inserting new record."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        None,
        {"test_id": 1},
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh._generic_norm_upsert(
            table="test_table",
            identity_col="test_id",
            select_attrs={"name": "test"},
            extra_insert_and_update_vals={"value": 123},
        )

    assert result == 1

    insert_calls = [call for call in connection.fetchrow.call_args_list if "INSERT INTO" in str(call)]
    assert len(insert_calls) == 1


@pytest.mark.asyncio
async def test_generic_norm_upsert_update_existing_record(mock_database):
    """Test generic upsert when updating existing record."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.return_value = {"test_id": 1}

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh._generic_norm_upsert(
            table="test_table",
            identity_col="test_id",
            select_attrs={"name": "test"},
            extra_insert_and_update_vals={"value": 456},
        )

    assert result == 1
    assert connection.fetchrow.call_count == 1

    call = connection.fetchrow.call_args
    assert 'UPDATE "test_table"' in call.args[0]
    assert '"value" = $1' in call.args[0]
    assert '"name" = $2' in call.args[0]
    assert 'RETURNING "test_id"' in call.args[0]
    assert call.args[1:] == (456, "test")


@pytest.mark.asyncio
async def test_generic_norm_upsert_with_insert_only_vals(mock_database):
    """Test generic upsert with insert-only values."""
    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        None,
        {"test_id": 1, "created_at": "2025-01-01"},
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh._generic_norm_upsert(
            table="test_table",
            identity_col="test_id",
            select_attrs={"name": "test"},
            extra_insert_and_update_vals={"value": 123},
            insert_only_vals={"created_at": "2025-01-01"},
            extra_sel_cols=["created_at"],
        )

    assert result["test_id"] == 1
    assert result["created_at"] == "2025-01-01"
    assert connection.fetchrow.call_count == 2

    calls = connection.fetchrow.call_args_list

    # 1st call: UPDATE query (returns None, triggering INSERT)
    assert 'UPDATE "test_table"' in calls[0].args[0]
    assert 'RETURNING "test_id", "created_at"' in calls[0].args[0]
    assert calls[0].args[1:] == (123, "test")

    # 2nd call: INSERT with insert_only_vals included
    assert 'INSERT INTO "test_table"' in calls[1].args[0]
    assert '"created_at"' in calls[1].args[0]
    assert calls[1].args[1:] == ("test", 123, "2025-01-01")


@pytest.mark.asyncio
async def test_generic_norm_upsert_race_condition_handling(mock_database):
    """Test generic upsert handles race condition (UniqueViolationError)."""
    import asyncpg.exceptions

    from db import DBHelper

    connection = mock_database.connection

    connection.fetchrow.side_effect = [
        None,
        asyncpg.exceptions.UniqueViolationError("duplicate key"),
        {"test_id": 1},
    ]

    dbh = DBHelper(mock_database)

    with patch("db.database", mock_database):
        result = await dbh._generic_norm_upsert(
            table="test_table",
            identity_col="test_id",
            select_attrs={"name": "test"},
            extra_insert_and_update_vals={"value": 123},
        )

    assert result == 1
    assert connection.fetchrow.call_count == 3

    calls = connection.fetchrow.call_args_list

    # 1st call: UPDATE query
    assert 'UPDATE "test_table"' in calls[0].args[0]
    assert '"value" = $1' in calls[0].args[0]
    assert '"name" = $2' in calls[0].args[0]
    assert calls[0].args[1:] == (123, "test")

    # 2nd call: INSERT (fails with UniqueViolationError)
    assert 'INSERT INTO "test_table"' in calls[1].args[0]
    assert calls[1].args[1:] == ("test", 123)

    # 3rd call: retry UPDATE after race condition
    assert calls[2].args == calls[0].args


@pytest.mark.asyncio
async def test_database_lifecycle_connect_disconnect(mock_database):
    """Test database connection lifecycle."""
    from config import DefaultConfig
    from db import DatabaseLifecycleHandler

    config = DefaultConfig()

    with patch("db.asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)
        mock_conn.set_type_codec = AsyncMock()

        class MockPoolAcquireContext:
            async def __aenter__(self):
                return mock_conn

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        mock_pool = MagicMock()  # Not AsyncMock, because pool.acquire() is sync
        mock_pool.acquire.return_value = MockPoolAcquireContext()
        mock_pool.close = AsyncMock()
        mock_create_pool.return_value = mock_pool

        dbh = DatabaseLifecycleHandler(config)

        await dbh.connect()

        assert mock_create_pool.called
        assert dbh._pool is not None

        await dbh.disconnect()

        assert mock_pool.close.called


@pytest.mark.asyncio
async def test_database_acquire_context(mock_database):
    """Test database acquire context manager."""
    connection = mock_database.connection

    async with await mock_database.acquire() as conn:
        assert conn == connection
