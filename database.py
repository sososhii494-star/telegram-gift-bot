"""
Слой хранения состояния бота.

Работает в двух режимах:
  1. PostgreSQL (если задана переменная окружения DATABASE_URL и установлен asyncpg)
  2. Локальный JSON-файл (fallback) — чтобы бот запускался даже без базы

Все функции асинхронные и безопасны для конкурентного вызова.
Соединения берутся из пула и всегда возвращаются обратно (нет утечек коннектов).
"""

import asyncio
import json
import logging
import os
import time
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
STATE_FILE = os.environ.get("STATE_FILE", "bot_state.json")

_pool = None            # asyncpg.Pool
_json_state: Dict = {}
_json_lock = asyncio.Lock()
_use_pg = False


# =========================================================
# ИНИЦИАЛИЗАЦИЯ
# =========================================================

async def init_db() -> None:
    global _pool, _use_pg, _json_state

    if DATABASE_URL:
        try:
            import asyncpg  # noqa
        except ImportError:
            log.warning("asyncpg не установлен — использую JSON-хранилище")
        else:
            try:
                _pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=1,
                    max_size=5,
                    command_timeout=30,
                )
                async with _pool.acquire() as con:
                    await con.execute(
                        """
                        CREATE TABLE IF NOT EXISTS bot_settings (
                            key   TEXT PRIMARY KEY,
                            value TEXT NOT NULL DEFAULT ''
                        );
                        CREATE TABLE IF NOT EXISTS bot_stats (
                            name  TEXT PRIMARY KEY,
                            value BIGINT NOT NULL DEFAULT 0
                        );
                        CREATE TABLE IF NOT EXISTS allowed_chats (
                            chat_id BIGINT PRIMARY KEY
                        );
                        CREATE TABLE IF NOT EXISTS chance_boosts (
                            user_id    BIGINT PRIMARY KEY,
                            multiplier DOUBLE PRECISION NOT NULL DEFAULT 1,
                            expires_at DOUBLE PRECISION NOT NULL DEFAULT 0
                        );
                        CREATE TABLE IF NOT EXISTS star_payments (
                            charge_id  TEXT PRIMARY KEY,
                            user_id    BIGINT NOT NULL,
                            amount     INTEGER NOT NULL,
                            payload    TEXT NOT NULL,
                            created_at DOUBLE PRECISION NOT NULL
                        );
                        """
                    )
                _use_pg = True
                log.info("Хранилище: PostgreSQL")
                return
            except Exception:
                log.exception("Не удалось подключиться к PostgreSQL — перехожу на JSON")
                _pool = None

    # --- JSON fallback ---
    _use_pg = False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            _json_state = json.load(f)
    except FileNotFoundError:
        _json_state = {}
    except Exception:
        log.exception("Повреждён %s — начинаю с пустого состояния", STATE_FILE)
        _json_state = {}

    _json_state.setdefault("settings", {})
    _json_state.setdefault("stats", {})
    _json_state.setdefault("allowed_chats", [])
    _json_state.setdefault("boosts", {})
    _json_state.setdefault("payments", {})
    log.info("Хранилище: JSON-файл %s", STATE_FILE)


async def close_db() -> None:
    """Закрывает пул соединений. Без этого Render оставляет висящие коннекты."""
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            log.exception("Ошибка закрытия пула БД")
        _pool = None
    if not _use_pg:
        await _flush_json()


async def _flush_json() -> None:
    async with _json_lock:
        tmp = STATE_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_json_state, f, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
        except Exception:
            log.exception("Не удалось сохранить %s", STATE_FILE)


# =========================================================
# SETTINGS
# =========================================================

async def set_setting(key: str, value: str) -> None:
    value = "" if value is None else str(value)
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute(
                "INSERT INTO bot_settings(key, value) VALUES($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value,
            )
    else:
        _json_state["settings"][key] = value
        await _flush_json()


async def get_setting(key: str, default=None):
    if _use_pg:
        async with _pool.acquire() as con:
            row = await con.fetchrow("SELECT value FROM bot_settings WHERE key = $1", key)
        if row is None:
            return default
        value = row["value"]
    else:
        if key not in _json_state["settings"]:
            return default
        value = _json_state["settings"][key]
    return default if value == "" else value


async def get_int_setting(key: str, default: int = 0) -> int:
    try:
        return int(float(await get_setting(key, default)))
    except (TypeError, ValueError):
        return default


async def get_float_setting(key: str, default: float = 0.0) -> float:
    try:
        return float(await get_setting(key, default))
    except (TypeError, ValueError):
        return default


async def get_bool_setting(key: str, default: bool = False) -> bool:
    raw = await get_setting(key, None)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on", "да")


# =========================================================
# STATS
# =========================================================

async def increment_stat(name: str, amount: int = 1) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute(
                "INSERT INTO bot_stats(name, value) VALUES($1, $2) "
                "ON CONFLICT (name) DO UPDATE SET value = bot_stats.value + $2",
                name, amount,
            )
    else:
        _json_state["stats"][name] = int(_json_state["stats"].get(name, 0)) + amount
        # JSON пишем не на каждое сообщение — иначе диск на Render не выдержит
        if int(_json_state["stats"][name]) % 20 == 0:
            await _flush_json()


async def load_stats() -> Dict[str, int]:
    if _use_pg:
        async with _pool.acquire() as con:
            rows = await con.fetch("SELECT name, value FROM bot_stats")
        return {r["name"]: int(r["value"]) for r in rows}
    return {k: int(v) for k, v in _json_state["stats"].items()}


# =========================================================
# ДОСТУПНЫЕ ЧАТЫ
# =========================================================

async def load_allowed_chats() -> List[int]:
    if _use_pg:
        async with _pool.acquire() as con:
            rows = await con.fetch("SELECT chat_id FROM allowed_chats")
        return [int(r["chat_id"]) for r in rows]
    return [int(x) for x in _json_state["allowed_chats"]]


async def add_allowed_chat(chat_id: int) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute(
                "INSERT INTO allowed_chats(chat_id) VALUES($1) ON CONFLICT DO NOTHING",
                int(chat_id),
            )
    else:
        if int(chat_id) not in _json_state["allowed_chats"]:
            _json_state["allowed_chats"].append(int(chat_id))
            await _flush_json()


async def remove_allowed_chat(chat_id: int) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute("DELETE FROM allowed_chats WHERE chat_id = $1", int(chat_id))
    else:
        _json_state["allowed_chats"] = [
            x for x in _json_state["allowed_chats"] if int(x) != int(chat_id)
        ]
        await _flush_json()


async def clear_allowed_chats() -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute("DELETE FROM allowed_chats")
    else:
        _json_state["allowed_chats"] = []
        await _flush_json()


# =========================================================
# ПОВЫШЕННЫЙ ШАНС (покупка за Stars)
# =========================================================

async def set_boost(user_id: int, multiplier: float, expires_at: float) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute(
                "INSERT INTO chance_boosts(user_id, multiplier, expires_at) VALUES($1,$2,$3) "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "multiplier = EXCLUDED.multiplier, expires_at = EXCLUDED.expires_at",
                int(user_id), float(multiplier), float(expires_at),
            )
    else:
        _json_state["boosts"][str(user_id)] = {
            "multiplier": float(multiplier),
            "expires_at": float(expires_at),
        }
        await _flush_json()


async def load_active_boosts() -> Dict[int, Dict[str, float]]:
    now = time.time()
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute("DELETE FROM chance_boosts WHERE expires_at < $1", now)
            rows = await con.fetch("SELECT user_id, multiplier, expires_at FROM chance_boosts")
        return {
            int(r["user_id"]): {
                "multiplier": float(r["multiplier"]),
                "expires_at": float(r["expires_at"]),
            }
            for r in rows
        }

    alive = {
        int(uid): data
        for uid, data in _json_state["boosts"].items()
        if float(data.get("expires_at", 0)) > now
    }
    if len(alive) != len(_json_state["boosts"]):
        _json_state["boosts"] = {str(k): v for k, v in alive.items()}
        await _flush_json()
    return alive


async def drop_boost(user_id: int) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute("DELETE FROM chance_boosts WHERE user_id = $1", int(user_id))
    else:
        _json_state["boosts"].pop(str(user_id), None)
        await _flush_json()


# =========================================================
# ПЛАТЕЖИ STARS (для возвратов)
# =========================================================

async def save_payment(charge_id: str, user_id: int, amount: int, payload: str) -> None:
    if _use_pg:
        async with _pool.acquire() as con:
            await con.execute(
                "INSERT INTO star_payments(charge_id, user_id, amount, payload, created_at) "
                "VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                charge_id, int(user_id), int(amount), payload, time.time(),
            )
    else:
        _json_state["payments"][charge_id] = {
            "user_id": int(user_id),
            "amount": int(amount),
            "payload": payload,
            "created_at": time.time(),
        }
        await _flush_json()


async def get_payment(charge_id: str) -> Optional[Dict]:
    if _use_pg:
        async with _pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT charge_id, user_id, amount, payload FROM star_payments WHERE charge_id = $1",
                charge_id,
            )
        return dict(row) if row else None
    return _json_state["payments"].get(charge_id)


async def last_payments(limit: int = 10) -> List[Dict]:
    if _use_pg:
        async with _pool.acquire() as con:
            rows = await con.fetch(
                "SELECT charge_id, user_id, amount, payload, created_at "
                "FROM star_payments ORDER BY created_at DESC LIMIT $1",
                int(limit),
            )
        return [dict(r) for r in rows]
    items = [
        dict(charge_id=k, **v) for k, v in _json_state["payments"].items()
    ]
    items.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return items[:limit]
