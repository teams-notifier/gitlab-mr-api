#!/usr/bin/env python3
import asyncio
import datetime
import logging

import asyncpg
import httpx

from config import DefaultConfig
from db import DatabaseLifecycleHandler

logger = logging.getLogger(__name__)

signal = asyncio.Event()
MAX_WAIT = 300


def reschedule():
    signal.set()


async def periodic_cleanup(config: DefaultConfig, database: DatabaseLifecycleHandler):
    return await _log_exception(_cleanup_task(config, database))


async def _cleanup_task(config: DefaultConfig, database: DatabaseLifecycleHandler):
    client = httpx.AsyncClient()
    while True:
        # Cleanup message function goes here :)
        wait_sec = MAX_WAIT
        try:
            connection: asyncpg.Connection
            async with await database.acquire() as connection:
                stmt = await connection.prepare(
                    """SELECT msg_to_delete_id, message_id
                        FROM msg_to_delete
                        WHERE expire_at < NOW()
                        FOR UPDATE"""
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
                value = await connection.fetchval("SELECT min(expire_at) FROM msg_to_delete")
                if value is not None:
                    wait_sec = min(MAX_WAIT, (value - datetime.datetime.now(tz=datetime.UTC)).total_seconds())
        except Exception as e:
            logger.exception(e)
        try:
            logger.debug(f"wait for signal or {wait_sec}s")
            await asyncio.wait_for(signal.wait(), wait_sec)
            signal.clear()
            logger.debug("signal received")
        except TimeoutError:
            logger.debug("wait_for timeout")


async def _log_exception(awaitable):
    try:
        return await awaitable
    except Exception as e:
        logger.exception(e)
