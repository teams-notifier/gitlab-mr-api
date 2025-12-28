#!/usr/bin/env python3
"""Database migration script - implements dbmate-compatible migrations."""

import asyncio
import os
import re
import sys

from pathlib import Path

import asyncpg


MIGRATIONS_DIR = Path(__file__).parent / "db" / "migrations"

BOOTSTRAP_CHECKS = {
    "00000000000000": """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'gitlab_mr_api' AND table_name = 'merge_request_ref'
        )
    """,
    "20250121000000": """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'gitlab_mr_api'
            AND table_name = 'merge_request_message_ref'
            AND column_name = 'last_processed_fingerprint'
        )
    """,
    "20250121010000": """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'gitlab_mr_api'
            AND table_name = 'merge_request_message_ref'
            AND column_name = 'last_processed_fingerprint'
        )
    """,
    "20251210000000": """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'gitlab_mr_api'
            AND table_name = 'merge_request_message_ref'
            AND column_name = 'last_processed_updated_at'
        )
    """,
}


def parse_migration(content: str) -> str | None:
    """Extract the -- migrate:up section from a migration file."""
    match = re.search(r"--\s*migrate:up\s*\n(.*?)(?:--\s*migrate:down|$)", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def get_migration_files() -> list[tuple[str, Path]]:
    """Get sorted list of (version, path) for all migration files."""
    if not MIGRATIONS_DIR.exists():
        return []
    files = []
    for f in MIGRATIONS_DIR.glob("*.sql"):
        version = f.stem.split("_")[0]
        files.append((version, f))
    return sorted(files, key=lambda x: x[0])


async def ensure_schema_migrations_table(conn: asyncpg.Connection) -> None:
    """Create schema and schema_migrations table if they don't exist."""
    await conn.execute("CREATE SCHEMA IF NOT EXISTS gitlab_mr_api")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS gitlab_mr_api.schema_migrations (
            version character varying(128) PRIMARY KEY
        )
    """)


async def get_applied_versions(conn: asyncpg.Connection) -> set[str]:
    """Get set of already applied migration versions."""
    rows = await conn.fetch("SELECT version FROM gitlab_mr_api.schema_migrations")
    return {row["version"] for row in rows}


async def bootstrap_existing(conn: asyncpg.Connection, applied: set[str]) -> None:
    """Seed schema_migrations for existing databases based on table/column checks."""
    for version, check_sql in BOOTSTRAP_CHECKS.items():
        if version in applied:
            continue
        is_applied = await conn.fetchval(check_sql)
        if is_applied:
            await conn.execute(
                "INSERT INTO gitlab_mr_api.schema_migrations (version) VALUES ($1)",
                version,
            )
            print(f"  {version}: bootstrapped (already applied)")
            applied.add(version)


async def run_migration(conn: asyncpg.Connection, version: str, path: Path) -> bool:
    """Run a single migration file. Returns True on success."""
    content = path.read_text()
    up_sql = parse_migration(content)
    if not up_sql:
        print(f"  {version}: skipped (no migrate:up section)")
        return True

    try:
        await conn.execute(up_sql)
        await conn.execute(
            "INSERT INTO gitlab_mr_api.schema_migrations (version) VALUES ($1)",
            version,
        )
        print(f"  {version}: applied")
        return True
    except Exception as e:
        print(f"  {version}: FAILED - {e}", file=sys.stderr)
        return False


async def main() -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL is required", file=sys.stderr)
        return 1

    try:
        conn = await asyncpg.connect(database_url)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        return 1

    try:
        print("Ensuring schema_migrations table exists...")
        await ensure_schema_migrations_table(conn)

        applied = await get_applied_versions(conn)
        print(f"Found {len(applied)} applied migrations")

        print("Checking for existing schema to bootstrap...")
        await bootstrap_existing(conn, applied)

        migrations = get_migration_files()
        pending = [(v, p) for v, p in migrations if v not in applied]

        if not pending:
            print("No pending migrations")
            return 0

        print(f"Running {len(pending)} pending migrations...")
        for version, path in pending:
            if not await run_migration(conn, version, path):
                return 1

        print("All migrations completed successfully")
        return 0

    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
