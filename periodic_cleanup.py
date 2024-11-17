#!/usr/bin/env python3
import asyncio
import logging

import asyncpg
import httpx

from config import DefaultConfig
from db import DatabaseLifecycleHandler

logger = logging.getLogger(__name__)


async def periodic_cleanup(config: DefaultConfig, database: DatabaseLifecycleHandler):
    return await _log_exception(_cleanup_task(config, database))


async def _cleanup_task(config: DefaultConfig, database: DatabaseLifecycleHandler):
    client = httpx.AsyncClient()
    while True:
        # Cleanup message function goes here :)
        try:
            connection: asyncpg.Connection
            async with await database.acquire() as connection:
                stmt = await connection.prepare(
                    "SELECT msg_to_delete_id, message_id FROM msg_to_delete WHERE expire_at < NOW()"
                )
                async with connection.transaction():
                    async for record in stmt.cursor():
                        try:
                            res = await client.request(
                                "DELETE",
                                config.ACTIVITY_API + "api/v1/message",
                                json={
                                    "message_id": str(record["message_id"]),
                                },
                            )
                            if res.status_code not in (410, 200):
                                res.raise_for_status()
                            await connection.execute(
                                "DELETE FROM msg_to_delete WHERE msg_to_delete_id = $1",
                                record["msg_to_delete_id"],
                            )
                            logger.info("deleted message %s", record["msg_to_delete_id"])
                        except Exception as e:
                            logger.exception(f"Error processing record {record['msg_to_delete_id']}: {e}")
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(2)


async def _log_exception(awaitable):
    try:
        return await awaitable
    except Exception as e:
        logger.exception(e)
