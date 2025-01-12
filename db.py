#!/usr/bin/env python3
import asyncio
import json
import logging
import urllib.parse
from typing import Any
from typing import Literal

import asyncpg.connect_utils
from pydantic import BaseModel

from config import config
from gitlab_model import GLEmojiAttributes
from gitlab_model import MergeRequestPayload
from gitlab_model import PipelinePayload

log = logging.getLogger(__name__)

__all__ = ["database", "dbh"]


class NoResetConnection(asyncpg.connection.Connection):
    def __init__(
        self,
        protocol: asyncpg.protocol.protocol.BaseProtocol,
        transport: object,
        loop: asyncio.AbstractEventLoop,
        addr: tuple[str, int] | str,
        config: asyncpg.connect_utils._ClientConfiguration,
        params: asyncpg.connect_utils._ConnectionParameters,
    ) -> None:
        super().__init__(protocol, transport, loop, addr, config, params)
        self._reset_query: list[str] = []


class DatabaseLifecycleHandler:
    def __init__(self, dsn: str, debug_queries: bool):
        self._pool: asyncpg.Pool | None = None
        self.debug_queries = debug_queries
        self.dsn = dsn

    async def connect(self):
        log.debug("creating database connection pool")
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            server_settings={
                "application_name": "notiteams-gitlab-mr-api",
            },
            connection_class=NoResetConnection,
            init=self.init_connection,
        )

        # Simple check at startup, will validate database resolution and creds
        async with await self.acquire() as connection:
            await connection.fetchval("SELECT 1")

    async def init_connection(self, conn: asyncpg.Connection) -> None:
        log.debug("connecting to database")
        await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")

        if self.debug_queries:

            def relog(value: asyncpg.connection.LoggedQuery):
                log.debug(
                    json.dumps(
                        {
                            "query": value.query,
                            "args": value.args,
                            "timeout": value.timeout,
                            "elapsed": value.elapsed,
                            "exception": str(value.exception),
                        },
                        default=str,
                    )
                )

            conn.add_query_logger(relog)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()

    async def acquire(self) -> asyncpg.pool.PoolAcquireContext:
        assert self._pool is not None
        return self._pool.acquire()


class GitlabUser(BaseModel):
    id: int
    name: str
    username: str


class GitlabApprovals(GitlabUser):
    status: str


class EmojiEntry(BaseModel, extra="allow"):
    object_kind: Literal["emoji"]
    event_type: Literal["award"] | Literal["revoke"]
    object_attributes: GLEmojiAttributes
    user: GitlabUser


class MergeRequestExtraState(BaseModel):
    version: int
    opener: GitlabUser
    approvers: dict[str, GitlabApprovals]
    pipeline_statuses: dict[str, PipelinePayload]
    emojis: dict[str, EmojiEntry]


class MergeRequestInfos(BaseModel):
    merge_request_ref_id: int
    merge_request_payload: MergeRequestPayload
    merge_request_extra_state: MergeRequestExtraState
    head_pipeline_id: int | None


class DBHelper:
    def __init__(self, database: DatabaseLifecycleHandler):
        self.db: DatabaseLifecycleHandler = database

    async def get_gitlab_instance_id_from_url(self, urlstr: str) -> int:
        url = urllib.parse.urlparse(urlstr)
        if not url.netloc:
            raise ValueError(f"unable to determine gitlab host from {urlstr}")

        hostname = url.netloc.lower()

        gli_id = await self._generic_norm_upsert(
            table="gitlab_instance",
            identity_col="gitlab_instance_id",
            select_attrs={"hostname": hostname},
        )
        assert isinstance(gli_id, int)
        return gli_id

    async def get_merge_request_ref_infos(self, merge_request: MergeRequestPayload) -> MergeRequestInfos:
        gitlab_instance_id = await self.get_gitlab_instance_id_from_url(merge_request.object_attributes.url)

        merge_ref = await self._generic_norm_upsert(
            table="merge_request_ref",
            identity_col="merge_request_ref_id",
            select_attrs={
                "gitlab_instance_id": gitlab_instance_id,
                "gitlab_project_id": merge_request.object_attributes.target_project_id,
                "gitlab_merge_request_iid": merge_request.object_attributes.iid,
            },
            extra_insert_and_update_vals={
                "gitlab_merge_request_id": merge_request.object_attributes.id,
                "head_pipeline_id": merge_request.object_attributes.head_pipeline_id,
                "merge_request_payload": merge_request.model_dump(),
            },
            insert_only_vals={
                "merge_request_extra_state": {
                    "version": 1,
                    "opener": {
                        "id": merge_request.user.id,
                        "name": merge_request.user.name,
                        "username": merge_request.user.username,
                    },
                    "approvers": {},
                    "pipeline_statuses": {},
                    "emojis": {},
                },
            },
            extra_sel_cols=["merge_request_payload", "merge_request_extra_state", "head_pipeline_id"],
        )
        assert isinstance(merge_ref, asyncpg.Record)
        return MergeRequestInfos(**merge_ref)

    async def get_mri_from_url_pid_mriid(
        self,
        url: str,
        project_id: int,
        mr_iid: int,
    ) -> MergeRequestInfos | None:
        gitlab_instance_id = await self.get_gitlab_instance_id_from_url(url)

        connection: asyncpg.Connection
        async with await database.acquire() as connection:
            row = await connection.fetchrow(
                """SELECT
                        merge_request_ref_id,
                        merge_request_payload,
                        merge_request_extra_state,
                        head_pipeline_id
                    FROM merge_request_ref
                    WHERE
                        gitlab_instance_id = $1
                        AND gitlab_project_id = $2
                        AND gitlab_merge_request_iid = $3
                """,
                gitlab_instance_id,
                project_id,
                mr_iid,
            )
            if row is not None:
                return MergeRequestInfos(**row)
        return None

    async def _generic_norm_upsert(
        self,
        *,
        table: str,
        identity_col: str,
        select_attrs: dict[str, Any],
        extra_insert_and_update_vals: dict[str, Any] | None = None,
        insert_only_vals: dict[str, Any] | None = None,
        extra_sel_cols: list[str] | None = None,
    ) -> Any:

        if extra_insert_and_update_vals is None:
            extra_insert_and_update_vals = {}

        if insert_only_vals is None:
            insert_only_vals = {}

        if extra_sel_cols is None:
            extra_sel_cols = []

        sel_cols: list[str] = [f'"{identity_col}"']
        sel_cols.extend([f'"{extra_col}"' for extra_col in extra_sel_cols])
        sel_where: list[str] = []
        sel_args: list[Any] = []
        upd_set: list[str] = []
        upd_where: list[str] = []
        upd_args: list[Any] = []

        for k, v in extra_insert_and_update_vals.items():
            upd_args.append(v)
            upd_set.append(f'"{k}" = ${len(upd_args)}')

        for k, v in select_attrs.items():
            sel_args.append(v)
            sel_where.append(f'"{k}" = ${len(sel_args)}')

            upd_args.append(v)
            upd_where.append(f'"{k}" = ${len(upd_args)}')

        if len(upd_set):
            query = f"""UPDATE "{table}"
                    SET {", ".join(upd_set)}
                    WHERE {" AND ".join(upd_where)}
                    RETURNING {", ".join(sel_cols)}"""
            args = upd_args
        else:
            query = f"""SELECT
                        {", ".join(sel_cols)}
                    FROM "{table}"
                    WHERE {" AND ".join(sel_where)}"""
            args = sel_args

        connection: asyncpg.Connection
        async with await database.acquire() as connection:
            row = await connection.fetchrow(
                query,
                *args,
            )
            if row is None:
                try:
                    ins_col = []
                    ins_args = []
                    for k, v in select_attrs.items():
                        ins_col.append(f'"{k}"')
                        ins_args.append(v)

                    for k, v in extra_insert_and_update_vals.items():
                        ins_col.append(f'"{k}"')
                        ins_args.append(v)

                    for k, v in insert_only_vals.items():
                        ins_col.append(f'"{k}"')
                        ins_args.append(v)

                    row = await connection.fetchrow(
                        f"""
                        INSERT INTO "{table}" (
                            {", ".join(ins_col)}
                        ) VALUES (
                            {", ".join(["$"+str(i+1) for i in range(len(ins_col))])}
                        ) RETURNING {", ".join(sel_cols)}
                        """,
                        *ins_args,
                    )
                except asyncpg.exceptions.UniqueViolationError:
                    row = await connection.fetchrow(
                        query,
                        *args,
                    )

        assert row is not None
        if len(extra_sel_cols) == 0:
            return row[identity_col]
        return row


database = DatabaseLifecycleHandler(config.DATABASE_URL, config.log_queries)
dbh = DBHelper(database)
