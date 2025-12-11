#!/usr/bin/env python3
import time
import uuid

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import asyncpg
import pytest

from config import DefaultConfig


class MockRecord(dict[str, Any]):
    """Mock asyncpg.Record that behaves like a dict but passes isinstance checks."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


class MockConnection:
    def __init__(self):
        self.execute = AsyncMock()
        self.fetch = AsyncMock(return_value=[])
        self.fetchrow = AsyncMock(return_value=None)
        self.fetchval = AsyncMock(return_value=None)
        self.prepare = AsyncMock()
        self._transaction = None

    def transaction(self):
        if self._transaction is None:
            self._transaction = MockTransaction()
        return self._transaction


class MockTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockAcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockDatabase:
    def __init__(self):
        self.connection = MockConnection()

    async def acquire(self):
        return MockAcquireContext(self.connection)

    async def connect(self):
        pass

    async def disconnect(self):
        pass


class MockCursor:
    def __init__(self, records):
        self.records = records
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.records):
            raise StopAsyncIteration
        record = self.records[self.index]
        self.index += 1
        return record


class MockPreparedStatement:
    def __init__(self, records):
        self.records = records

    def cursor(self):
        return MockCursor(self.records)


@pytest.fixture
def mock_database():
    return MockDatabase()


@pytest.fixture
def mock_httpx_client():
    client = AsyncMock()
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"message_id": str(uuid.uuid4())}
    client.request = AsyncMock(return_value=response)
    return client


class MockAsyncHttpxClient:
    """Mock httpx.AsyncClient that properly handles async context manager."""

    def __init__(self, *args, **kwargs):
        self.response = MagicMock()
        self.response.status_code = 200
        self.response.json.return_value = {"message_id": str(uuid.uuid4())}
        self.response.raise_for_status = MagicMock()
        self.request = AsyncMock(return_value=self.response)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_httpx_async_client_class():
    """Returns MockAsyncHttpxClient class for patching httpx.AsyncClient."""
    return MockAsyncHttpxClient


@pytest.fixture
def mock_mr_database():
    """Mock database for merge_request module with connection context manager."""
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.fetchrow = AsyncMock(return_value={"merge_request_extra_state": {}})
    mock_conn.fetchval = AsyncMock(return_value=None)

    mock_acquire_ctx = MagicMock()
    mock_acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_ctx.__aexit__ = AsyncMock()
    mock_db.acquire = AsyncMock(return_value=mock_acquire_ctx)

    return mock_db, mock_conn


@pytest.fixture
def sample_mri():
    """Sample MergeRequestInfos mock for testing."""
    mri = MagicMock()
    mri.merge_request_ref_id = 1
    mri.merge_request_payload.object_attributes.state = "opened"
    mri.merge_request_payload.object_attributes.title = "Test MR"
    mri.merge_request_payload.object_attributes.draft = False
    mri.merge_request_payload.object_attributes.work_in_progress = False
    mri.merge_request_payload.project.path_with_namespace = "test/repo"
    mri.merge_request_payload.assignees = []
    mri.merge_request_payload.reviewers = []
    mri.merge_request_extra_state.opener.id = 1
    return mri


@pytest.fixture
def sample_message_ref():
    """Sample message reference mock."""
    ref = MagicMock()
    ref.message_id = None
    ref.merge_request_message_ref_id = 123
    return ref


@pytest.fixture
def sample_message_ref_with_id():
    """Sample message reference mock with existing message_id."""
    ref = MagicMock()
    ref.message_id = uuid.uuid4()
    ref.merge_request_message_ref_id = 123
    return ref


@pytest.fixture
def sample_merge_request_payload():
    """Complete MergeRequestPayload fixture matching gitlab_model.py schema."""
    from gitlab_model import GLMRAttributes
    from gitlab_model import GLMRRepo
    from gitlab_model import GLProject
    from gitlab_model import GLUser
    from gitlab_model import MergeRequestPayload

    return MergeRequestPayload(
        object_kind="merge_request",
        event_type="merge_request",
        repository=GLMRRepo(
            homepage="https://gitlab.example.com/test/project",
            name="project",
        ),
        user=GLUser(
            id=1,
            username="testuser",
            name="Test User",
            email="testuser@example.com",
        ),
        project=GLProject(
            id=1,
            path_with_namespace="test/project",
            web_url="https://gitlab.example.com/test/project",
        ),
        object_attributes=GLMRAttributes(
            id=123,
            iid=1,
            title="Test MR",
            created_at="2025-01-01 00:00:00 UTC",
            draft=False,
            state="merged",
            url="https://gitlab.example.com/test/project/-/merge_requests/1",
            action="merge",
            updated_at="2025-01-01 00:00:00 UTC",
            detailed_merge_status="mergeable",
            head_pipeline_id=None,
            work_in_progress=False,
            source_project_id=100,
            source_branch="feature-branch",
            target_project_id=100,
            target_branch="main",
        ),
        changes={},
        assignees=[],
        reviewers=[],
    )


# E2E test fixtures with real PostgreSQL database


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container using testcontainers."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(
        image="postgres:16-alpine",
        username="test_user",
        password="test_password",  # pragma: allowlist secret
        dbname="gitlab_mr_api_test",
    ) as container:
        time.sleep(2)

        import psycopg2

        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)

        conn = psycopg2.connect(
            host=host,
            port=port,
            user="test_user",
            password="test_password",  # pragma: allowlist secret
            dbname="gitlab_mr_api_test",
        )
        cursor = conn.cursor()

        try:
            with open("db/schema.sql") as f:
                schema_sql = f.read()
                cursor.execute(schema_sql)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        yield container


@pytest.fixture(scope="session")
def test_database_url(postgres_container):
    """Database URL for testing from testcontainer."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    # pragma: allowlist nextline secret
    return (
        f"postgresql://test_user:test_password@{host}:{port}"  # pragma: allowlist secret
        f"/gitlab_mr_api_test?options=-c%20search_path%3Dgitlab_mr_api"
    )


@pytest.fixture
async def db_connection(test_database_url) -> AsyncGenerator[asyncpg.Connection, None]:
    """Create a database connection for a single test."""
    conn = await asyncpg.connect(test_database_url)

    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
async def clean_database(db_connection: asyncpg.Connection):
    """Clean database before each test."""
    await db_connection.execute("TRUNCATE gitlab_mr_api.msg_to_delete CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.merge_request_message_ref CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.merge_request_ref CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.gitlab_instance CASCADE")

    yield

    await db_connection.execute("TRUNCATE gitlab_mr_api.msg_to_delete CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.merge_request_message_ref CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.merge_request_ref CASCADE")
    await db_connection.execute("TRUNCATE gitlab_mr_api.gitlab_instance CASCADE")


@pytest.fixture
async def db_pool(test_database_url) -> AsyncGenerator[asyncpg.Pool, None]:
    """Create a connection pool for tests."""
    pool = await asyncpg.create_pool(
        dsn=test_database_url,
        min_size=1,
        max_size=5,
    )

    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def e2e_config(test_database_url):
    """Test configuration with real database URL."""
    config = DefaultConfig()
    config.DATABASE_URL = test_database_url
    return config


@pytest.fixture
def sample_gitlab_instance_data():
    """Sample GitLab instance data."""
    return {"hostname": "gitlab.example.com"}


@pytest.fixture
def sample_merge_request_data():
    """Sample merge request data for database."""
    return {
        "gitlab_instance_id": 1,
        "gitlab_project_id": 100,
        "gitlab_merge_request_id": 1000,
        "gitlab_merge_request_iid": 1,
        "merge_request_payload": {
            "object_kind": "merge_request",
            "object_attributes": {
                "id": 1000,
                "iid": 1,
                "title": "Test MR",
                "state": "opened",
                "action": "open",
            },
        },
        "merge_request_extra_state": {
            "version": 1,
            "opener": {"id": 1, "username": "testuser", "name": "Test User"},
            "approvers": {},
            "pipeline_statuses": {},
            "emojis": {},
        },
    }


@pytest.fixture
def sample_message_ref_data():
    """Sample message reference data."""
    return {
        "merge_request_ref_id": 1,
        "conversation_token": uuid.uuid4(),
        "message_id": None,
    }


@pytest.fixture
async def mock_activity_api(monkeypatch):
    """Mock activity API client for e2e tests."""
    responses: list[dict[str, Any]] = []
    requests: list[dict[str, Any]] = []

    class MockResponse:
        def __init__(self, status_code, json_data):
            self.status_code = status_code
            self._json = json_data

        def json(self):
            return self._json

        def raise_for_status(self):
            if self.status_code >= 400:
                import httpx

                raise httpx.HTTPStatusError(
                    f"HTTP {self.status_code}",
                    request=None,
                    response=self,
                )

    class MockAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def request(self, method, url, **kwargs):
            requests.append({"method": method, "url": url, "kwargs": kwargs})

            if responses:
                response_data = responses.pop(0)
                return MockResponse(**response_data)

            return MockResponse(200, {"message_id": str(uuid.uuid4())})

    import httpx

    original_client = httpx.AsyncClient
    httpx.AsyncClient = MockAsyncClient

    yield {"responses": responses, "requests": requests}

    httpx.AsyncClient = original_client
