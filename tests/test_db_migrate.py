#!/usr/bin/env python3
"""Tests for db_migrate.py."""

import tempfile

from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from db_migrate import BOOTSTRAP_CHECKS
from db_migrate import get_migration_files
from db_migrate import parse_migration


class TestParseMigration:
    """Tests for parse_migration function."""

    def test_parse_migration_up_only(self):
        content = """-- migrate:up
CREATE TABLE foo (id int);
"""
        result = parse_migration(content)
        assert result == "CREATE TABLE foo (id int);"

    def test_parse_migration_up_and_down(self):
        content = """-- migrate:up
CREATE TABLE foo (id int);
ALTER TABLE foo ADD COLUMN name text;

-- migrate:down
DROP TABLE foo;
"""
        result = parse_migration(content)
        assert result == "CREATE TABLE foo (id int);\nALTER TABLE foo ADD COLUMN name text;"

    def test_parse_migration_no_up_section(self):
        content = """-- some comment
SELECT 1;
"""
        result = parse_migration(content)
        assert result is None

    def test_parse_migration_whitespace_variations(self):
        content = """--migrate:up
CREATE TABLE bar (id int);
--migrate:down
DROP TABLE bar;
"""
        result = parse_migration(content)
        assert result == "CREATE TABLE bar (id int);"

    def test_parse_migration_windows_crlf(self):
        content = "-- migrate:up\r\nCREATE TABLE foo (id int);\r\n-- migrate:down\r\nDROP TABLE foo;\r\n"
        result = parse_migration(content)
        assert result is not None
        assert "CREATE TABLE foo" in result

    def test_parse_migration_extra_spaces(self):
        content = """--   migrate:up
CREATE TABLE foo (id int);
--   migrate:down
DROP TABLE foo;
"""
        result = parse_migration(content)
        assert result == "CREATE TABLE foo (id int);"

    def test_parse_migration_empty_up_section(self):
        content = """-- migrate:up

-- migrate:down
DROP TABLE foo;
"""
        result = parse_migration(content)
        assert result == ""

    def test_parse_migration_multiline_statements(self):
        content = """-- migrate:up
CREATE TABLE foo (
    id int,
    name text,
    created_at timestamp
);

INSERT INTO foo VALUES (1, 'test', now());

-- migrate:down
DROP TABLE foo;
"""
        result = parse_migration(content)
        assert result is not None
        assert "CREATE TABLE foo" in result
        assert "INSERT INTO foo" in result

    def test_parse_migration_no_down_section(self):
        content = """-- migrate:up
CREATE TABLE foo (id int);
"""
        result = parse_migration(content)
        assert result == "CREATE TABLE foo (id int);"

    def test_parse_migration_comments_in_sql(self):
        content = """-- migrate:up
-- This is a comment
CREATE TABLE foo (id int);
/* multi-line
   comment */
ALTER TABLE foo ADD COLUMN bar text;

-- migrate:down
DROP TABLE foo;
"""
        result = parse_migration(content)
        assert result is not None
        assert "-- This is a comment" in result
        assert "CREATE TABLE foo" in result


class TestGetMigrationFiles:
    """Tests for get_migration_files function."""

    def test_get_migration_files_sorted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir) / "db" / "migrations"
            migrations_dir.mkdir(parents=True)

            (migrations_dir / "20250102_second.sql").write_text("-- migrate:up\n")
            (migrations_dir / "20250101_first.sql").write_text("-- migrate:up\n")
            (migrations_dir / "20250103_third.sql").write_text("-- migrate:up\n")

            with patch("db_migrate.MIGRATIONS_DIR", migrations_dir):
                files = get_migration_files()

            assert len(files) == 3
            assert files[0][0] == "20250101"
            assert files[1][0] == "20250102"
            assert files[2][0] == "20250103"

    def test_get_migration_files_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir) / "db" / "migrations"
            migrations_dir.mkdir(parents=True)

            with patch("db_migrate.MIGRATIONS_DIR", migrations_dir):
                files = get_migration_files()

            assert files == []

    def test_get_migration_files_nonexistent_dir(self):
        with patch("db_migrate.MIGRATIONS_DIR", Path("/nonexistent/path")):
            files = get_migration_files()
        assert files == []

    def test_get_migration_files_ignores_non_sql(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir) / "db" / "migrations"
            migrations_dir.mkdir(parents=True)

            (migrations_dir / "20250101_first.sql").write_text("-- migrate:up\n")
            (migrations_dir / "20250102_second.txt").write_text("not sql")
            (migrations_dir / "README.md").write_text("readme")

            with patch("db_migrate.MIGRATIONS_DIR", migrations_dir):
                files = get_migration_files()

            assert len(files) == 1
            assert files[0][0] == "20250101"


class TestBootstrapChecks:
    """Tests for bootstrap check SQL queries.

    BOOTSTRAP_CHECKS are only needed for migrations that existed before
    db_migrate.py was introduced. Future migrations don't need checks
    because schema_migrations will track them properly.
    """

    def test_legacy_migrations_have_checks(self):
        """Legacy migrations (before db_migrate.py) must have bootstrap checks."""
        legacy_versions = ["00000000000000", "20250121000000", "20250121010000", "20251210000000"]
        for version in legacy_versions:
            assert version in BOOTSTRAP_CHECKS, f"Missing bootstrap check for legacy migration {version}"

    def test_checks_are_valid_sql(self):
        for version, sql in BOOTSTRAP_CHECKS.items():
            assert "SELECT EXISTS" in sql, f"Check for {version} should use SELECT EXISTS"
            assert "information_schema" in sql, f"Check for {version} should query information_schema"


class TestEnsureSchemaMigrationsTable:
    """Tests for ensure_schema_migrations_table function."""

    @pytest.mark.asyncio
    async def test_creates_schema_and_table(self):
        from db_migrate import ensure_schema_migrations_table

        mock_conn = AsyncMock()
        await ensure_schema_migrations_table(mock_conn)

        assert mock_conn.execute.call_count == 2
        calls = [call[0][0] for call in mock_conn.execute.call_args_list]
        assert "CREATE SCHEMA IF NOT EXISTS gitlab_mr_api" in calls[0]
        assert "CREATE TABLE IF NOT EXISTS" in calls[1]
        assert "schema_migrations" in calls[1]


class TestGetAppliedVersions:
    """Tests for get_applied_versions function."""

    @pytest.mark.asyncio
    async def test_returns_set_of_versions(self):
        from db_migrate import get_applied_versions

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {"version": "00000000000000"},
            {"version": "20250101000000"},
        ]

        result = await get_applied_versions(mock_conn)

        assert result == {"00000000000000", "20250101000000"}

    @pytest.mark.asyncio
    async def test_returns_empty_set_when_no_migrations(self):
        from db_migrate import get_applied_versions

        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []

        result = await get_applied_versions(mock_conn)

        assert result == set()


class TestBootstrapExisting:
    """Tests for bootstrap_existing function."""

    @pytest.mark.asyncio
    async def test_skips_already_recorded(self):
        from db_migrate import bootstrap_existing

        mock_conn = AsyncMock()
        applied = set(BOOTSTRAP_CHECKS.keys())

        await bootstrap_existing(mock_conn, applied)

        mock_conn.fetchval.assert_not_called()
        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_inserts_when_check_passes(self):
        from db_migrate import bootstrap_existing

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = True
        applied: set[str] = set()

        with patch.dict("db_migrate.BOOTSTRAP_CHECKS", {"test123": "SELECT true"}, clear=True):
            await bootstrap_existing(mock_conn, applied)

        assert "test123" in applied
        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_check_fails(self):
        from db_migrate import bootstrap_existing

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = False
        applied: set[str] = set()

        with patch.dict("db_migrate.BOOTSTRAP_CHECKS", {"test123": "SELECT false"}, clear=True):
            await bootstrap_existing(mock_conn, applied)

        assert "test123" not in applied
        mock_conn.execute.assert_not_called()


class TestRunMigration:
    """Tests for run_migration function."""

    @pytest.mark.asyncio
    async def test_runs_migration_successfully(self):
        from db_migrate import run_migration

        mock_conn = AsyncMock()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- migrate:up\nCREATE TABLE test (id int);\n-- migrate:down\nDROP TABLE test;")
            f.flush()
            path = Path(f.name)

        try:
            result = await run_migration(mock_conn, "20250101", path)
        finally:
            path.unlink()

        assert result is True
        assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_skips_when_no_up_section(self):
        from db_migrate import run_migration

        mock_conn = AsyncMock()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- just a comment\nSELECT 1;")
            f.flush()
            path = Path(f.name)

        try:
            result = await run_migration(mock_conn, "20250101", path)
        finally:
            path.unlink()

        assert result is True
        mock_conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_false_on_error(self):
        from db_migrate import run_migration

        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = Exception("DB error")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- migrate:up\nINVALID SQL;")
            f.flush()
            path = Path(f.name)

        try:
            result = await run_migration(mock_conn, "20250101", path)
        finally:
            path.unlink()

        assert result is False

    @pytest.mark.asyncio
    async def test_handles_windows_line_endings(self):
        from db_migrate import run_migration

        mock_conn = AsyncMock()

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".sql", delete=False) as f:
            f.write(b"-- migrate:up\r\nCREATE TABLE test (id int);\r\n-- migrate:down\r\nDROP TABLE test;")
            path = Path(f.name)

        try:
            result = await run_migration(mock_conn, "20250101", path)
        finally:
            path.unlink()

        assert result is True


class TestMain:
    """Tests for main function."""

    @pytest.mark.asyncio
    async def test_fails_without_database_url(self):
        from db_migrate import main

        with patch.dict("os.environ", {}, clear=True):
            result = await main()

        assert result == 1

    @pytest.mark.asyncio
    async def test_fails_on_connection_error(self):
        from db_migrate import main

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://invalid"}),
            patch("db_migrate.asyncpg.connect", side_effect=Exception("Connection failed")),
        ):
            result = await main()

        assert result == 1

    @pytest.mark.asyncio
    async def test_successful_run_no_pending(self):
        from db_migrate import main

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = False
        mock_conn.fetch.return_value = [{"version": "00000000000000"}]

        with (
            patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}),
            patch("db_migrate.asyncpg.connect", return_value=mock_conn),
            patch("db_migrate.get_migration_files", return_value=[("00000000000000", Path("test.sql"))]),
        ):
            result = await main()

        assert result == 0
        mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_runs_pending_migrations(self):
        from db_migrate import main

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = False
        mock_conn.fetch.return_value = []

        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            migration_file = migrations_dir / "20250101_test.sql"
            migration_file.write_text("-- migrate:up\nCREATE TABLE test (id int);")

            with (
                patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}),
                patch("db_migrate.asyncpg.connect", return_value=mock_conn),
                patch("db_migrate.get_migration_files", return_value=[("20250101", migration_file)]),
                patch.dict("db_migrate.BOOTSTRAP_CHECKS", {}, clear=True),
            ):
                result = await main()

        assert result == 0

    @pytest.mark.asyncio
    async def test_stops_on_migration_failure(self):
        from db_migrate import main

        call_count = 0

        async def execute_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # Allow first 2 calls (CREATE SCHEMA + CREATE TABLE), fail on migration
            if call_count > 2:
                raise Exception("Migration failed")

        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = False
        mock_conn.fetch.return_value = []
        mock_conn.execute.side_effect = execute_side_effect

        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            migration_file = migrations_dir / "20250101_test.sql"
            migration_file.write_text("-- migrate:up\nCREATE TABLE test (id int);")

            with (
                patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}),
                patch("db_migrate.asyncpg.connect", return_value=mock_conn),
                patch("db_migrate.get_migration_files", return_value=[("20250101", migration_file)]),
                patch.dict("db_migrate.BOOTSTRAP_CHECKS", {}, clear=True),
            ):
                result = await main()

        assert result == 1
