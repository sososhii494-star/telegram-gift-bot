import os
import json
import logging
import asyncpg

log = logging.getLogger(__name__)
_pool = None


async def init_db(database_url: str | None = None):
    """Create the PostgreSQL pool and all required tables."""
    global _pool
    database_url = database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add a Render PostgreSQL database and "
            "set DATABASE_URL in the bot service environment."
        )

    _pool = await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS allowed_chats (
                chat_id BIGINT PRIMARY KEY
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                messages BIGINT NOT NULL DEFAULT 0,
                wins BIGINT NOT NULL DEFAULT 0,
                gifts_sent BIGINT NOT NULL DEFAULT 0,
                errors BIGINT NOT NULL DEFAULT 0
            )
        """)
        await conn.execute("""
            INSERT INTO bot_stats (id)
            VALUES (1)
            ON CONFLICT (id) DO NOTHING
        """)

    log.info("PostgreSQL database initialized")


def _ensure_pool():
    if _pool is None:
        raise RuntimeError("Database pool is not initialized")


async def get_setting(key, default=None):
    _ensure_pool()
    async with _pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT value FROM bot_settings WHERE key = $1", key
        )
    if value is None:
        return default
    return value


async def set_setting(key, value):
    _ensure_pool()
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False)
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bot_settings(key, value)
            VALUES($1, $2)
            ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
        """, key, value)


async def delete_setting(key):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM bot_settings WHERE key = $1", key)


async def get_json_setting(key, default=None):
    raw = await get_setting(key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


async def set_json_setting(key, value):
    await set_setting(key, json.dumps(value, ensure_ascii=False))


async def get_bool_setting(key, default=False):
    raw = await get_setting(key)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


async def get_int_setting(key, default=0):
    raw = await get_setting(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


async def get_float_setting(key, default=0.0):
    raw = await get_setting(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


async def load_allowed_chats():
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM allowed_chats")
    return {int(row["chat_id"]) for row in rows}


async def add_allowed_chat(chat_id):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO allowed_chats(chat_id) VALUES($1) ON CONFLICT DO NOTHING",
            int(chat_id),
        )


async def remove_allowed_chat(chat_id):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM allowed_chats WHERE chat_id = $1", int(chat_id)
        )


async def clear_allowed_chats():
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute("DELETE FROM allowed_chats")


async def load_stats():
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT messages, wins, gifts_sent, errors
            FROM bot_stats WHERE id = 1
        """)
    if not row:
        return {"messages": 0, "wins": 0, "gifts_sent": 0, "errors": 0}
    return {
        "messages": int(row["messages"]),
        "wins": int(row["wins"]),
        "gifts_sent": int(row["gifts_sent"]),
        "errors": int(row["errors"]),
    }


async def increment_stat(name, amount=1):
    _ensure_pool()
    allowed = {"messages", "wins", "gifts_sent", "errors"}
    if name not in allowed:
        raise ValueError(f"Unknown statistic: {name}")

    async with _pool.acquire() as conn:
        await conn.execute(
            f"UPDATE bot_stats SET {name} = {name} + $1 WHERE id = 1",
            int(amount),
        )


async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
