#!/usr/bin/env python3
"""E2E tests for db_migrate.py against real PostgreSQL."""

import time

import asyncpg
import pytest

from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def fresh_postgres():
    """Start a fresh PostgreSQL container WITHOUT pre-loaded schema."""
    with PostgresContainer(
        image="postgres:16-alpine",
        username="test_user",
        password="test_password",  # pragma: allowlist secret
        dbname="migrate_test",
    ) as container:
        time.sleep(2)
        yield container


@pytest.fixture(scope="module")
def fresh_database_url(fresh_postgres):
    """Database URL for fresh database."""
    host = fresh_postgres.get_container_host_ip()
    port = fresh_postgres.get_exposed_port(5432)
    # pragma: allowlist nextline secret
    return f"postgresql://test_user:test_password@{host}:{port}/migrate_test"


@pytest.mark.e2e
class TestMigrateE2E:
    """E2E tests for database migrations."""

    @pytest.mark.asyncio
    async def test_migrate_fresh_database(self, fresh_database_url, monkeypatch):
        """Test running migrations on a completely fresh database."""
        import db_migrate

        monkeypatch.setenv("DATABASE_URL", fresh_database_url)

        result = await db_migrate.main()

        assert result == 0, "Migration should succeed"

        conn = await asyncpg.connect(fresh_database_url)
        try:
            schema_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata
                    WHERE schema_name = 'gitlab_mr_api'
                )
            """)
            assert schema_exists, "Schema gitlab_mr_api should exist"

            tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'gitlab_mr_api'
                ORDER BY table_name
            """)
            table_names = {row["table_name"] for row in tables}

            expected_tables = {
                "schema_migrations",
                "gitlab_instance",
                "merge_request_ref",
                "merge_request_message_ref",
                "msg_to_delete",
            }
            assert expected_tables.issubset(table_names), f"Missing tables: {expected_tables - table_names}"

            applied = await conn.fetch("SELECT version FROM gitlab_mr_api.schema_migrations ORDER BY version")
            applied_versions = {row["version"] for row in applied}

            migration_files = db_migrate.get_migration_files()
            expected_versions = {version for version, _ in migration_files}

            assert expected_versions == applied_versions, (
                f"All migrations should be recorded.\n"
                f"Expected: {sorted(expected_versions)}\n"
                f"Applied: {sorted(applied_versions)}\n"
                f"Missing: {sorted(expected_versions - applied_versions)}"
            )

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_migrate_idempotent(self, fresh_database_url, monkeypatch):
        """Test that running migrations twice is idempotent."""
        import db_migrate

        monkeypatch.setenv("DATABASE_URL", fresh_database_url)

        result1 = await db_migrate.main()
        assert result1 == 0, "First migration should succeed"

        conn = await asyncpg.connect(fresh_database_url)
        try:
            count_before = await conn.fetchval("SELECT COUNT(*) FROM gitlab_mr_api.schema_migrations")
        finally:
            await conn.close()

        result2 = await db_migrate.main()
        assert result2 == 0, "Second migration should succeed (no-op)"

        conn = await asyncpg.connect(fresh_database_url)
        try:
            count_after = await conn.fetchval("SELECT COUNT(*) FROM gitlab_mr_api.schema_migrations")
            assert count_after == count_before, "No new migrations should be applied"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_columns_exist_after_migration(self, fresh_database_url):
        """Verify specific columns added by migrations exist."""
        conn = await asyncpg.connect(fresh_database_url)
        try:
            columns = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'gitlab_mr_api'
                AND table_name = 'merge_request_message_ref'
            """)
            column_names = {row["column_name"] for row in columns}

            assert (
                "last_processed_fingerprint" in column_names
            ), "Column from migration 20250121010000 should exist"
            assert (
                "last_processed_updated_at" in column_names
            ), "Column from migration 20251210000000 should exist"
        finally:
            await conn.close()
