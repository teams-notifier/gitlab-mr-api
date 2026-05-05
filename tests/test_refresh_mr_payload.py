#!/usr/bin/env python3
"""
Tests for the GitLab API state-refresh recovery path:

- DBHelper.refresh_mr_payload_from_api (db.py)
- fetch_and_refresh_mr_status (gitlab_api.py)
- _process_pending_refreshes api-refresh-close branch (periodic_cleanup.py)
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


def _stored_payload(**overrides: Any) -> dict[str, Any]:
    """Webhook-format stored payload, valid for MergeRequestPayload schema."""
    oa: dict[str, Any] = {
        "id": 123,
        "iid": 1,
        "title": "Test MR",
        "created_at": "2026-01-01 00:00:00 UTC",
        "draft": False,
        "state": "opened",
        "url": "https://gitlab.example.com/test/project/-/merge_requests/1",
        "action": "open",
        "updated_at": "2026-01-01 00:00:00 UTC",
        "detailed_merge_status": "mergeable",
        "head_pipeline_id": None,
        "work_in_progress": False,
        "source_project_id": 100,
        "source_branch": "feature",
        "target_project_id": 100,
        "target_branch": "main",
    }
    oa.update(overrides)
    return {
        "object_kind": "merge_request",
        "event_type": "merge_request",
        "repository": {"homepage": "https://gitlab.example.com/test/project", "name": "project"},
        "user": {"id": 1, "username": "u", "name": "U", "email": "u@example.com"},
        "project": {
            "id": 1,
            "path_with_namespace": "test/project",
            "web_url": "https://gitlab.example.com/test/project",
        },
        "object_attributes": oa,
        "changes": {},
        "assignees": [],
        "reviewers": [],
    }


def _stored_extra_state() -> dict[str, Any]:
    return {
        "version": 1,
        "opener": {"id": 1, "username": "u", "name": "U"},
        "approvers": {},
        "pipeline_statuses": {},
        "emojis": {},
    }


def _mock_db_with_row(stored_row: dict[str, Any]):
    """Build a mock_database whose fetchrow returns stored_row."""
    db = MagicMock()
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=stored_row)

    class TxnCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    conn.transaction = MagicMock(return_value=TxnCtx())

    class AcquireCtx:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *args):
            return None

    db.acquire = AsyncMock(return_value=AcquireCtx())
    return db, conn


@pytest.mark.asyncio
async def test_refresh_state_opened_to_merged_sets_action_merge():
    """API state 'merged' on a webhook-recorded 'open' must synthesize action='merge'."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="opened", action="open"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, conn = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {
        "state": "merged",
        "title": "Test MR",
        "draft": False,
        "detailed_merge_status": "mergeable",
        "source_branch": "feature",
        "target_branch": "main",
        "updated_at": "2026-05-05T14:36:14.118Z",
    }

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    oa = result.merge_request_payload.object_attributes
    assert oa.state == "merged"
    assert oa.action == "merge"


@pytest.mark.asyncio
async def test_refresh_state_opened_to_closed_sets_action_close():
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="opened", action="open"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "closed", "updated_at": "2026-05-05T14:36:14Z"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.merge_request_payload.object_attributes.action == "close"


@pytest.mark.asyncio
async def test_refresh_state_still_opened_does_not_mutate_action():
    """If API confirms still open and stored action is benign, action must stay."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="opened", action="approved"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "opened", "updated_at": "2026-05-05T14:36:14Z"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    # No transition: action stays as it was on the webhook.
    assert result.merge_request_payload.object_attributes.action == "approved"


@pytest.mark.asyncio
async def test_refresh_reopen_path_when_state_returned_to_opened():
    """Stored action close/merge but API says opened → action=reopen."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="merged", action="merge"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "opened", "updated_at": "2026-05-05T14:36:14Z"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.merge_request_payload.object_attributes.action == "reopen"
    assert result.merge_request_payload.object_attributes.state == "opened"


@pytest.mark.asyncio
async def test_refresh_normalizes_iso_updated_at_to_webhook_format():
    """API ISO-8601 updated_at must be stored in webhook 'YYYY-MM-DD HH:MM:SS UTC' format."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "merged", "updated_at": "2026-05-05T14:36:14.118Z"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.merge_request_payload.object_attributes.updated_at == "2026-05-05 14:36:14 UTC"


@pytest.mark.asyncio
async def test_refresh_naive_iso_updated_at_assumed_utc_not_local_tz():
    """Defensive: API returning naive ISO (no offset) must NOT be shifted by host TZ.

    Without the explicit UTC tag, astimezone() treats naive datetimes as the
    host's local timezone — wrong if the host runs in any non-UTC TZ.
    """
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "merged", "updated_at": "2026-05-05T14:36:14"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    # Time must be preserved as-is (assumed UTC), not shifted by host TZ.
    assert result.merge_request_payload.object_attributes.updated_at == "2026-05-05 14:36:14 UTC"


@pytest.mark.asyncio
async def test_refresh_unparseable_updated_at_keeps_stored_value():
    """Defensive: bad updated_at format must not raise — would stick the pending refresh."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(updated_at="2026-01-01 00:00:00 UTC"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "merged", "updated_at": "not a date"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    # Stored value preserved, no exception raised, state still updated.
    assert result.merge_request_payload.object_attributes.updated_at == "2026-01-01 00:00:00 UTC"
    assert result.merge_request_payload.object_attributes.state == "merged"


@pytest.mark.asyncio
async def test_refresh_normalizes_iso_updated_at_with_offset():
    """Non-Z ISO with explicit offset must also normalize to UTC webhook format."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "merged", "updated_at": "2026-05-05T16:36:14+02:00"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.merge_request_payload.object_attributes.updated_at == "2026-05-05 14:36:14 UTC"


@pytest.mark.asyncio
async def test_refresh_head_pipeline_present_updates_column_and_oa():
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(head_pipeline_id=10),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": 10,
    }
    db, conn = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {
        "state": "merged",
        "head_pipeline": {"id": 99, "status": "success"},
        "updated_at": "2026-05-05T14:36:14Z",
    }

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.head_pipeline_id == 99
    assert result.merge_request_payload.object_attributes.head_pipeline_id == 99

    update_calls = [c for c in conn.execute.call_args_list if "UPDATE merge_request_ref" in c.args[0]]
    assert update_calls, "expected UPDATE of merge_request_ref"
    assert update_calls[-1].args[2] == 99


@pytest.mark.asyncio
async def test_refresh_head_pipeline_null_keeps_stored_value():
    """API returning head_pipeline=null must not overwrite stored pipeline id."""
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(head_pipeline_id=10),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": 10,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {
        "state": "merged",
        "head_pipeline": None,
        "updated_at": "2026-05-05T14:36:14Z",
    }

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.head_pipeline_id == 10
    assert result.merge_request_payload.object_attributes.head_pipeline_id == 10


@pytest.mark.asyncio
async def test_refresh_draft_syncs_work_in_progress():
    from db import DBHelper

    stored = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(draft=False, work_in_progress=False),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
    }
    db, _ = _mock_db_with_row(stored)
    dbh = DBHelper(db)

    api_data: dict[str, Any] = {"state": "opened", "draft": True, "updated_at": "2026-05-05T14:36:14Z"}

    with patch("db.database", db):
        result = await dbh.refresh_mr_payload_from_api(1, api_data)

    assert result.merge_request_payload.object_attributes.draft is True
    assert result.merge_request_payload.object_attributes.work_in_progress is True


@pytest.mark.asyncio
async def test_emoji_refresh_calls_api_even_when_state_already_terminal():
    """Even when stored state is already terminal, emoji refresh must call the API.

    The merge_request handler updates payload state and schedules deletion
    in separate transactions, so a partial rollback can leave state='merged'
    with refs still present. The api-refresh-close branch (gated on
    api_refreshed) is the recovery — short-circuiting the API call here
    would skip that recovery.
    """
    import periodic_cleanup as pc

    pending_row = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="merged", action="merge"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
        "payload_type": "emoji",
        "first_event_at": None,
        "last_event_at": None,
    }

    mock_fetch = AsyncMock(return_value=None)
    mock_dbh = MagicMock()
    mock_dbh.get_pending_refreshes = AsyncMock(return_value=[pending_row])
    mock_dbh.delete_pending_refresh = AsyncMock()
    mock_update = AsyncMock(return_value=0)

    with (
        patch.object(pc, "fetch_and_refresh_mr_status", mock_fetch),
        patch.object(pc, "fetch_and_persist_discussion_stats", AsyncMock(return_value=None)),
        patch.object(pc, "update_all_messages_transactional", mock_update),
        patch.object(pc, "dbh", mock_dbh),
    ):
        await pc._process_pending_refreshes()

    mock_fetch.assert_called_once()


@pytest.mark.asyncio
async def test_emoji_refresh_calls_api_when_state_still_open():
    """Stored state is non-terminal → emoji refresh must call the API."""
    import periodic_cleanup as pc

    pending_row = {
        "merge_request_ref_id": 1,
        "merge_request_payload": _stored_payload(state="opened", action="open"),
        "merge_request_extra_state": _stored_extra_state(),
        "head_pipeline_id": None,
        "payload_type": "emoji",
        "first_event_at": None,
        "last_event_at": None,
    }

    mock_fetch = AsyncMock(return_value=None)
    mock_dbh = MagicMock()
    mock_dbh.get_pending_refreshes = AsyncMock(return_value=[pending_row])
    mock_dbh.delete_pending_refresh = AsyncMock()
    mock_update = AsyncMock(return_value=0)

    with (
        patch.object(pc, "fetch_and_refresh_mr_status", mock_fetch),
        patch.object(pc, "fetch_and_persist_discussion_stats", AsyncMock(return_value=None)),
        patch.object(pc, "update_all_messages_transactional", mock_update),
        patch.object(pc, "dbh", mock_dbh),
    ):
        await pc._process_pending_refreshes()

    mock_fetch.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_and_refresh_returns_none_when_token_missing():
    """No GitLab token configured for the project URL → return None, no DB write."""
    from gitlab_api import fetch_and_refresh_mr_status

    with (
        patch("gitlab_api.config") as mock_cfg,
        patch("db.dbh") as mock_dbh,
    ):
        mock_cfg.get_gitlab_api_token.return_value = None
        mock_dbh.refresh_mr_payload_from_api = AsyncMock()

        result = await fetch_and_refresh_mr_status(
            merge_request_ref_id=1,
            project_url="https://gitlab.example.com/test/project",
            project_id=100,
            mr_iid=1,
        )

    assert result is None
    mock_dbh.refresh_mr_payload_from_api.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_and_refresh_returns_none_on_http_error():
    """HTTP 404/4xx on the GitLab API call → return None, no DB write."""
    import httpx

    from gitlab_api import fetch_and_refresh_mr_status

    api_token = MagicMock()
    api_token.url = "https://gitlab.example.com"
    api_token.token = "tok"  # noqa: S105
    api_token.name = "test"

    class MockResponse:
        status_code = 404

        def raise_for_status(self):
            raise httpx.HTTPStatusError("not found", request=MagicMock(), response=self)

    class MockClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return MockResponse()

    with (
        patch("gitlab_api.config") as mock_cfg,
        patch("gitlab_api.httpx.AsyncClient", MockClient),
        patch("db.dbh") as mock_dbh,
    ):
        mock_cfg.get_gitlab_api_token.return_value = api_token
        mock_dbh.refresh_mr_payload_from_api = AsyncMock()

        result = await fetch_and_refresh_mr_status(
            merge_request_ref_id=1,
            project_url="https://gitlab.example.com/test/project",
            project_id=100,
            mr_iid=1,
        )

    assert result is None
    mock_dbh.refresh_mr_payload_from_api.assert_not_called()
