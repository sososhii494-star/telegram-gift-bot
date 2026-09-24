"""
Telegram Gift Bot — ОДИН ФАЙЛ, без внешних модулей.

Розыгрыш подарков + Лудка 777 + магазин за Telegram Stars.
Внутри объединены три части:
  1) хранилище состояния (PostgreSQL или JSON-файл)
  2) привязка пользовательского аккаунта через MTProto (Telethon)
  3) сам бот

Запуск: python bot.py
"""

import asyncio
import base64
import hashlib
import html
import json
import logging
import os
import random
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

from telegram import (
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    MessageEntity,
    Update,
    User,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

log = logging.getLogger("giftbot")


# =======================================================
# ЧАСТЬ 1. ХРАНИЛИЩЕ СОСТОЯНИЯ
# =======================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://polysympak_user:0PydWImCrU7bwxkWv1aH5j3xdBnOIEis@dpg-dao6m5ajnfac73apvmfg-a/polysympak",
).strip()
STATE_FILE = os.environ.get("STATE_FILE", "bot_state.json")

_db_pool = None            # asyncpg.Pool
_db_json_state: Dict = {}
_db_json_lock = asyncio.Lock()
_db_use_pg = False


# =========================================================
# ИНИЦИАЛИЗАЦИЯ
# =========================================================

async def db_init_db() -> None:
    global _db_pool, _db_use_pg, _db_json_state

    if DATABASE_URL:
        try:
            import asyncpg  # noqa
        except ImportError:
            log.warning("asyncpg не установлен — использую JSON-хранилище")
        else:
            try:
                _db_pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=1,
                    max_size=5,
                    command_timeout=30,
                )
                async with _db_pool.acquire() as con:
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
                _db_use_pg = True
                log.info("Хранилище: PostgreSQL")
                return
            except Exception:
                log.exception("Не удалось подключиться к PostgreSQL — перехожу на JSON")
                _db_pool = None

    # --- JSON fallback ---
    _db_use_pg = False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            _db_json_state = json.load(f)
    except FileNotFoundError:
        _db_json_state = {}
    except Exception:
        log.exception("Повреждён %s — начинаю с пустого состояния", STATE_FILE)
        _db_json_state = {}

    _db_json_state.setdefault("settings", {})
    _db_json_state.setdefault("stats", {})
    _db_json_state.setdefault("allowed_chats", [])
    _db_json_state.setdefault("blocked_usernames", [])
    _db_json_state.setdefault("boosts", {})
    _db_json_state.setdefault("payments", {})
    log.info("Хранилище: JSON-файл %s", STATE_FILE)


async def db_close_db() -> None:
    """Закрывает пул соединений. Без этого Render оставляет висящие коннекты."""
    global _db_pool
    if _db_pool is not None:
        try:
            await _db_pool.close()
        except Exception:
            log.exception("Ошибка закрытия пула БД")
        _db_pool = None
    if not _db_use_pg:
        await _db_flush_json()


async def _db_flush_json() -> None:
    async with _db_json_lock:
        tmp = STATE_FILE + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_db_json_state, f, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
        except Exception:
            log.exception("Не удалось сохранить %s", STATE_FILE)


# =========================================================
# SETTINGS
# =========================================================

async def db_set_setting(key: str, value: str) -> None:
    value = "" if value is None else str(value)
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute(
                "INSERT INTO bot_settings(key, value) VALUES($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value,
            )
    else:
        _db_json_state["settings"][key] = value
        await _db_flush_json()


async def db_get_setting(key: str, default=None):
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            row = await con.fetchrow("SELECT value FROM bot_settings WHERE key = $1", key)
        if row is None:
            return default
        value = row["value"]
    else:
        if key not in _db_json_state["settings"]:
            return default
        value = _db_json_state["settings"][key]
    return default if value == "" else value


async def db_get_int_setting(key: str, default: int = 0) -> int:
    try:
        return int(float(await db_get_setting(key, default)))
    except (TypeError, ValueError):
        return default


async def db_get_float_setting(key: str, default: float = 0.0) -> float:
    try:
        return float(await db_get_setting(key, default))
    except (TypeError, ValueError):
        return default


async def db_get_bool_setting(key: str, default: bool = False) -> bool:
    raw = await db_get_setting(key, None)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on", "да")


# =========================================================
# STATS
# =========================================================

async def db_increment_stat(name: str, amount: int = 1) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute(
                "INSERT INTO bot_stats(name, value) VALUES($1, $2) "
                "ON CONFLICT (name) DO UPDATE SET value = bot_stats.value + $2",
                name, amount,
            )
    else:
        _db_json_state["stats"][name] = int(_db_json_state["stats"].get(name, 0)) + amount
        # JSON пишем не на каждое сообщение — иначе диск на Render не выдержит
        if int(_db_json_state["stats"][name]) % 20 == 0:
            await _db_flush_json()


async def db_load_stats() -> Dict[str, int]:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            rows = await con.fetch("SELECT name, value FROM bot_stats")
        return {r["name"]: int(r["value"]) for r in rows}
    return {k: int(v) for k, v in _db_json_state["stats"].items()}


# =========================================================
# ДОСТУПНЫЕ ЧАТЫ
# =========================================================

async def db_load_allowed_chats() -> List[int]:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            rows = await con.fetch("SELECT chat_id FROM allowed_chats")
        return [int(r["chat_id"]) for r in rows]
    return [int(x) for x in _db_json_state["allowed_chats"]]


async def db_add_allowed_chat(chat_id: int) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute(
                "INSERT INTO allowed_chats(chat_id) VALUES($1) ON CONFLICT DO NOTHING",
                int(chat_id),
            )
    else:
        if int(chat_id) not in _db_json_state["allowed_chats"]:
            _db_json_state["allowed_chats"].append(int(chat_id))
            await _db_flush_json()


async def db_remove_allowed_chat(chat_id: int) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute("DELETE FROM allowed_chats WHERE chat_id = $1", int(chat_id))
    else:
        _db_json_state["allowed_chats"] = [
            x for x in _db_json_state["allowed_chats"] if int(x) != int(chat_id)
        ]
        await _db_flush_json()


async def db_clear_allowed_chats() -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute("DELETE FROM allowed_chats")
    else:
        _db_json_state["allowed_chats"] = []
        await _db_flush_json()


# =========================================================
# ПОВЫШЕННЫЙ ШАНС (покупка за Stars)
# =========================================================

async def db_set_boost(user_id: int, multiplier: float, expires_at: float) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute(
                "INSERT INTO chance_boosts(user_id, multiplier, expires_at) VALUES($1,$2,$3) "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "multiplier = EXCLUDED.multiplier, expires_at = EXCLUDED.expires_at",
                int(user_id), float(multiplier), float(expires_at),
            )
    else:
        _db_json_state["boosts"][str(user_id)] = {
            "multiplier": float(multiplier),
            "expires_at": float(expires_at),
        }
        await _db_flush_json()


async def db_load_active_boosts() -> Dict[int, Dict[str, float]]:
    now = time.time()
    if _db_use_pg:
        async with _db_pool.acquire() as con:
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
        for uid, data in _db_json_state["boosts"].items()
        if float(data.get("expires_at", 0)) > now
    }
    if len(alive) != len(_db_json_state["boosts"]):
        _db_json_state["boosts"] = {str(k): v for k, v in alive.items()}
        await _db_flush_json()
    return alive


async def db_drop_boost(user_id: int) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute("DELETE FROM chance_boosts WHERE user_id = $1", int(user_id))
    else:
        _db_json_state["boosts"].pop(str(user_id), None)
        await _db_flush_json()


# =========================================================
# ПЛАТЕЖИ STARS (для возвратов)
# =========================================================

async def db_save_payment(charge_id: str, user_id: int, amount: int, payload: str) -> None:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            await con.execute(
                "INSERT INTO star_payments(charge_id, user_id, amount, payload, created_at) "
                "VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                charge_id, int(user_id), int(amount), payload, time.time(),
            )
    else:
        _db_json_state["payments"][charge_id] = {
            "user_id": int(user_id),
            "amount": int(amount),
            "payload": payload,
            "created_at": time.time(),
        }
        await _db_flush_json()


async def db_get_payment(charge_id: str) -> Optional[Dict]:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT charge_id, user_id, amount, payload FROM star_payments WHERE charge_id = $1",
                charge_id,
            )
        return dict(row) if row else None
    return _db_json_state["payments"].get(charge_id)


async def db_last_payments(limit: int = 10) -> List[Dict]:
    if _db_use_pg:
        async with _db_pool.acquire() as con:
            rows = await con.fetch(
                "SELECT charge_id, user_id, amount, payload, created_at "
                "FROM star_payments ORDER BY created_at DESC LIMIT $1",
                int(limit),
            )
        return [dict(r) for r in rows]
    items = [
        dict(charge_id=k, **v) for k, v in _db_json_state["payments"].items()
    ]
    items.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return items[:limit]

async def db_load_blocked_usernames() -> List[str]:
    raw = await db_get_setting("blocked_usernames", "[]")
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(data, list):
            return sorted({str(x).strip().lstrip("@").lower() for x in data if str(x).strip()})
    except Exception:
        log.exception("Не удалось загрузить список заблокированных username")
    return []


async def db_save_blocked_usernames(items) -> None:
    clean = sorted({str(x).strip().lstrip("@").lower() for x in items if str(x).strip()})
    await db_set_setting("blocked_usernames", json.dumps(clean, ensure_ascii=False))


class db:
    """Пространство имён — заменяет отдельный модуль."""
    init_db = staticmethod(db_init_db)
    close_db = staticmethod(db_close_db)
    set_setting = staticmethod(db_set_setting)
    get_setting = staticmethod(db_get_setting)
    get_int_setting = staticmethod(db_get_int_setting)
    get_float_setting = staticmethod(db_get_float_setting)
    get_bool_setting = staticmethod(db_get_bool_setting)
    increment_stat = staticmethod(db_increment_stat)
    load_stats = staticmethod(db_load_stats)
    load_allowed_chats = staticmethod(db_load_allowed_chats)
    add_allowed_chat = staticmethod(db_add_allowed_chat)
    remove_allowed_chat = staticmethod(db_remove_allowed_chat)
    clear_allowed_chats = staticmethod(db_clear_allowed_chats)
    load_blocked_usernames = staticmethod(db_load_blocked_usernames)
    save_blocked_usernames = staticmethod(db_save_blocked_usernames)
    set_boost = staticmethod(db_set_boost)
    load_active_boosts = staticmethod(db_load_active_boosts)
    drop_boost = staticmethod(db_drop_boost)
    save_payment = staticmethod(db_save_payment)
    get_payment = staticmethod(db_get_payment)
    last_payments = staticmethod(db_last_payments)

# =======================================================
# ЧАСТЬ 2. АККАУНТ ВЫДАЧИ ПОДАРКОВ (MTProto)
# =======================================================

SESSION_KEY = "mtproto_session"
API_ID_KEY = "mtproto_api_id"
API_HASH_KEY = "mtproto_api_hash"

_ga_client = None                      # TelegramClient активного аккаунта
_ga_login: Dict[str, Any] = {}         # временное состояние входа
_ga_lock = asyncio.Lock()


# =========================================================
# ШИФРОВАНИЕ СЕССИИ
# =========================================================

def _ga_fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        return None
    secret = os.environ.get("SESSION_SECRET") or os.environ.get("BOT_TOKEN", "fallback")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
    return Fernet(key)


def _ga_encrypt(raw: str) -> str:
    f = _ga_fernet()
    if f is None:
        log.warning("cryptography не установлена — сессия хранится в открытом виде")
        return "plain:" + raw
    return "enc:" + f.encrypt(raw.encode()).decode()


def _ga_decrypt(stored: str) -> Optional[str]:
    if not stored:
        return None
    if stored.startswith("plain:"):
        return stored[6:]
    if stored.startswith("enc:"):
        f = _ga_fernet()
        if f is None:
            return None
        try:
            return f.decrypt(stored[4:].encode()).decode()
        except Exception:
            log.exception("Не удалось расшифровать сессию (сменился SESSION_SECRET?)")
            return None
    return stored


# =========================================================
# КЛИЕНТ
# =========================================================

async def ga_get_client():
    """Возвращает подключённый TelegramClient или None, если аккаунт не привязан."""
    global _ga_client

    async with _ga_lock:
        if _ga_client is not None:
            try:
                if not _ga_client.is_connected():
                    await _ga_client.connect()
                if await _ga_client.is_user_authorized():
                    return _ga_client
            except Exception:
                log.exception("Активный MTProto-клиент сломался, пересоздаю")
            await _ga_safe_disconnect(_ga_client)
            _ga_client = None

        raw = await db.get_setting(SESSION_KEY, None)
        session_str = _ga_decrypt(raw) if raw else None
        api_id = await db.get_int_setting(API_ID_KEY, 0)
        api_hash = await db.get_setting(API_HASH_KEY, None)

        if not (session_str and api_id and api_hash):
            return None

        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
        except ImportError:
            log.error("Telethon не установлен: pip install telethon")
            return None

        client = TelegramClient(StringSession(session_str), api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            log.error("Сессия MTProto недействительна — аккаунт нужно привязать заново")
            await _ga_safe_disconnect(client)
            return None

        _ga_client = client
        return _ga_client


async def _ga_safe_disconnect(client) -> None:
    if client is None:
        return
    try:
        result = client.disconnect()
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        pass


async def ga_close() -> None:
    """Вызывается при остановке бота."""
    global _ga_client
    await ga_abort_login()
    await _ga_safe_disconnect(_ga_client)
    _ga_client = None


# =========================================================
# ВХОД
# =========================================================

async def ga_start_login(api_id: int, api_hash: str, phone: str) -> str:
    """Шаг 1: отправляет код подтверждения. Возвращает phone_code_hash."""
    await ga_abort_login()

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.connect()
    sent = await client.send_code_request(phone)

    _ga_login.update(
        client=client,
        api_id=int(api_id),
        api_hash=api_hash,
        phone=phone,
        phone_code_hash=sent.phone_code_hash,
    )
    return sent.phone_code_hash


async def ga_finish_login(phone: str, code: str, phone_code_hash: str) -> Dict[str, Any]:
    """Шаг 2: вход по коду. Может вернуть {'need_password': True}."""
    client = _ga_login.get("client")
    if client is None:
        raise RuntimeError("Сессия входа истекла. Начни привязку заново.")

    from telethon.errors import SessionPasswordNeededError

    try:
        me = await client.sign_in(
            phone=phone, code=code, phone_code_hash=phone_code_hash
        )
    except SessionPasswordNeededError:
        return {"need_password": True}

    await _ga_persist_login()
    return {"need_password": False, "me": me}


async def ga_finish_login_password(password: str):
    """Шаг 3 (если включена двухэтапная аутентификация)."""
    client = _ga_login.get("client")
    if client is None:
        raise RuntimeError("Сессия входа истекла. Начни привязку заново.")

    me = await client.sign_in(password=password)
    await _ga_persist_login()
    return me


async def _ga_persist_login() -> None:
    """Сохраняет сессию в БД и делает клиента активным."""
    global _ga_client

    from telethon.sessions import StringSession

    client = _ga_login.pop("client", None)
    if client is None:
        return

    session_str = StringSession.save(client.session)
    await db.set_setting(SESSION_KEY, _ga_encrypt(session_str))
    await db.set_setting(API_ID_KEY, str(_ga_login.get("api_id", 0)))
    await db.set_setting(API_HASH_KEY, _ga_login.get("api_hash", ""))
    _ga_login.clear()

    await _ga_safe_disconnect(_ga_client)
    _ga_client = client


async def ga_abort_login() -> None:
    """Отмена привязки: закрывает временного клиента (иначе утечка соединения)."""
    client = _ga_login.pop("client", None)
    await _ga_safe_disconnect(client)
    _ga_login.clear()


async def ga_clear_account() -> None:
    """Отвязка аккаунта."""
    global _ga_client
    await ga_abort_login()
    if _ga_client is not None:
        try:
            await _ga_client.log_out()
        except Exception:
            log.exception("Не удалось выполнить log_out")
        await _ga_safe_disconnect(_ga_client)
        _ga_client = None
    await db.set_setting(SESSION_KEY, "")
    await db.set_setting(API_ID_KEY, "")
    await db.set_setting(API_HASH_KEY, "")


# =========================================================
# СТАТУС И БАЛАНС
# =========================================================

async def ga_get_balance() -> Optional[int]:
    client = await ga_get_client()
    if client is None:
        return None

    from telethon.tl import functions, types

    status = await client(
        functions.payments.GetStarsStatusRequest(peer=types.InputPeerSelf())
    )
    balance = getattr(status, "balance", None)
    # В новых слоях balance — это StarsAmount с полем amount
    return int(getattr(balance, "amount", balance) or 0)


async def ga_account_status() -> Dict[str, Any]:
    client = await ga_get_client()
    if client is None:
        return {"connected": False}

    me = await client.get_me()
    try:
        balance = await ga_get_balance()
    except Exception:
        log.exception("Не удалось получить баланс Stars")
        balance = None

    name = " ".join(
        p for p in [getattr(me, "first_name", None), getattr(me, "last_name", None)] if p
    ) or "Аккаунт"

    return {
        "connected": True,
        "user_id": me.id,
        "username": me.username,
        "name": name,
        "balance": balance,
    }


# =========================================================
# ПОДАРКИ
# =========================================================

async def ga_list_gifts() -> List[Dict[str, Any]]:
    """Список доступных обычных подарков глазами привязанного аккаунта."""
    client = await ga_get_client()
    if client is None:
        return []

    from telethon.tl import functions

    res = await client(functions.payments.GetStarGiftsRequest(hash=0))
    gifts = []
    for g in getattr(res, "gifts", []) or []:
        if getattr(g, "sold_out", False):
            continue
        gifts.append(
            {
                "id": int(g.id),
                "stars": int(getattr(g, "stars", 0) or 0),
                "limited": bool(getattr(g, "limited", False)),
            }
        )
    gifts.sort(key=lambda x: x["stars"])
    return gifts


async def _ga_resolve_peer(client, recipient, hint_chat_id=None, hint_query=None):
    """
    recipient — '@username' или числовой user_id (int либо строка из цифр).
    Строку из цифр обязательно превращаем в int: иначе Telethon принимает её
    за номер телефона и не находит человека.
    Для user_id аккаунт должен «видеть» пользователя. Если не видит — пробуем
    подтянуть диалоги и участников общего чата (hint_chat_id), и только потом
    честно сообщаем, что найти человека не вышло.
    """
    if isinstance(recipient, str) and recipient.strip().lstrip("-").isdigit():
        recipient = int(recipient.strip())

    try:
        return await client.get_input_entity(recipient)
    except Exception as first:
        if isinstance(recipient, int):
            try:
                await client.get_dialogs(limit=300)
                return await client.get_input_entity(recipient)
            except Exception:
                pass
            if hint_chat_id:
                try:
                    chat_entity = await client.get_input_entity(hint_chat_id)
                    async for member in client.iter_participants(
                        chat_entity, search=hint_query or "", limit=300
                    ):
                        if getattr(member, "id", None) == recipient:
                            return await client.get_input_entity(member)
                except Exception:
                    log.debug("Не удалось найти %s среди участников чата %s", recipient, hint_chat_id)
        raise first


def _ga_build_invoice(peer, gift_id: int, message: Optional[str]):
    from telethon.tl import types

    text = None
    if message:
        text = types.TextWithEntities(text=message[:255], entities=[])

    # Сигнатура InputInvoiceStarGift менялась между слоями Telethon,
    # поэтому пробуем известные варианты.
    attempts = [
        dict(peer=peer, gift_id=gift_id, hide_name=False,
             include_upgrade=False, message=text),
        dict(peer=peer, gift_id=gift_id, hide_name=False, message=text),
        dict(user_id=peer, gift_id=gift_id, hide_name=False, message=text),
    ]
    last = None
    for kwargs in attempts:
        try:
            return types.InputInvoiceStarGift(**kwargs)
        except TypeError as e:
            last = e
    raise RuntimeError(f"Несовместимая версия Telethon: {last}")


async def ga_send_gift(
    recipient, gift_id: int, message: Optional[str] = None,
    hint_chat_id=None, hint_query: Optional[str] = None,
):
    """Покупает и отправляет подарок победителю со Stars привязанного аккаунта."""
    client = await ga_get_client()
    if client is None:
        raise RuntimeError("Аккаунт выдачи не привязан")

    from telethon.tl import functions

    peer = await _ga_resolve_peer(client, recipient, hint_chat_id, hint_query)
    invoice = _ga_build_invoice(peer, int(gift_id), message)

    form = await client(functions.payments.GetPaymentFormRequest(invoice=invoice))
    return await client(
        functions.payments.SendStarsFormRequest(form_id=form.form_id, invoice=invoice)
    )

class gift_account:
    """Пространство имён — заменяет отдельный модуль."""
    get_client = staticmethod(ga_get_client)
    close = staticmethod(ga_close)
    start_login = staticmethod(ga_start_login)
    finish_login = staticmethod(ga_finish_login)
    finish_login_password = staticmethod(ga_finish_login_password)
    abort_login = staticmethod(ga_abort_login)
    clear_account = staticmethod(ga_clear_account)
    get_balance = staticmethod(ga_get_balance)
    account_status = staticmethod(ga_account_status)
    list_gifts = staticmethod(ga_list_gifts)
    send_gift = staticmethod(ga_send_gift)

# =======================================================
# ЧАСТЬ 3. БОТ
# =======================================================

# =========================================================
# НАСТРОЙКИ
# =========================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)

log = logging.getLogger("giftbot")

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("❌ Не задана переменная окружения BOT_TOKEN")

PORT = int(os.environ.get("PORT", 10000))
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7491572487"))

# Цены в Telegram Stars — значения по умолчанию (env), дальше всё
# переопределяется и хранится в БД через админ-панель (см. S[...]).
DEFAULT_PRICE_CLOSE_CHAT = int(os.environ.get("PRICE_CLOSE_CHAT", 300))
DEFAULT_PRICE_BOOST = int(os.environ.get("PRICE_BOOST", 1000))
DEFAULT_CLOSE_MINUTES = int(os.environ.get("CLOSE_MINUTES", 10))
DEFAULT_BOOST_MULTIPLIER = float(os.environ.get("BOOST_MULTIPLIER", 5))
DEFAULT_BOOST_HOURS = float(os.environ.get("BOOST_HOURS", 24))

MIN_CHANCE = 0.01
MAX_CHANCE = 100.0

MAX_ALLOWED_CHATS = 2

DEFAULT_WIN_TEXT = (
    "🎉 Ты выиграл!\n\n"
    "Но сейчас Telegram не позволил отправить подарок. "
    "Администратор проверит ситуацию."
)

DEFAULT_WIN_SUCCESS_TEXT = (
    "🎉 ПОЗДРАВЛЯЕМ!\n\n"
    "Ты выиграл настоящий Telegram-подарок! 🎁"
)

DEFAULT_BLOCKED_TEXT = (
    "🚫 Тебе не выдам, полусумрак тебя не одобряет.\n\n"
    "Вы заблокированы в выдачах."
)

DEFAULT_START_TEXT = (
    "🎁 Telegram Gift Bot\n\n"
    "Бот случайно разыгрывает настоящие Telegram-подарки "
    "среди сообщений в чате."
)

DEFAULT_TOP_HEADER_TEXT = (
    "🏆 ТОП ХАЛЯВЩИКОВ\n\n"
    "Лучшие по количеству полученных подарков:"
)

DEFAULT_TOP_EMPTY_TEXT = "\n\nПока никто не получил подарок. Будь первым!"

DEFAULT_PENDING_ISSUE_TEXT = (
    "🎉 Поздравляем! Твой подарок отправлен вручную администратором 🎁"
)

ANTISPAM_REASON = "спам сообщениями/фарм мишек"
DEFAULT_ANTISPAM_TEXT = "🔇 {user} получает мут на {minutes} мин.\nПричина: {reason}."
DEFAULT_MANUAL_GIFT_TEXT = "🎁 Подарок выдан вручную!\nСпасибо за участие ❤️"
DEFAULT_SUMRAK_CLOSE_TEXT = "🌑 Сумрак. Чат закрыт."
DEFAULT_SUMRAK_OPEN_TEXT = "🌕 Сумрак рассеялся. Чат открыт."
DEFAULT_NONMEMBER_TEXT = (
    "🚪 Подарки получают только участники чата. "
    "Вступи в чат и напиши снова!"
)
MAX_CHANNELS = 2

# Напоминание о правиле антиспама — редактируется целиком в админке
# (текст + фото + Premium Emoji). Плейсхолдеры: {limit} {window} {minutes} {reason}.
DEFAULT_ANTISPAM_REMINDER_TEXT = (
    "⚠️ Напоминание: больше {limit} сообщений в минуту — мут чата на {minutes} мин.\n"
    "Причина: {reason}."
)

TOP_PLACES_COUNT = 10
LEADERBOARD_INTERVAL_SECONDS = 15 * 60

DEFAULT_BTN_CLOSE_LABEL = "🔒 Закрыть чат"
DEFAULT_BTN_BOOST_LABEL = "🍀 Повысить шанс на подарок"
DEFAULT_BTN_MY_LABEL = "📊 Мои покупки"
DEFAULT_BTN_HELP_LABEL = "ℹ️ Как это работает"

ACCESS_DENIED_TEXT = (
    "🚫 БОТ НЕ РАБОТАЕТ ТУТ БРАТ\n\n"
    "ДОСТУП ПРИОБРЕТИ ТУТ @POLYSYMRAK"
)

CLOSED_PERMS = ChatPermissions(
    can_send_messages=False,
    can_send_polls=False,
    can_send_other_messages=False,
    can_add_web_page_previews=False,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)

OPEN_PERMS = ChatPermissions(
    can_send_messages=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)


# =========================================================
# СОСТОЯНИЕ (единый словарь вместо десятка global)
# =========================================================

S: Dict[str, object] = {
    "chance": float(os.environ.get("GIFT_CHANCE", "1")),
    "giveaway_enabled": True,
    "selected_gift_id": None,

    # Сообщение при успешной автоматической выдаче подарка в чате
    "win_success_text": DEFAULT_WIN_SUCCESS_TEXT,
    "win_success_photo": None,
    "win_success_entities": [],

    # Запасное сообщение, если подарок не удалось отправить автоматически
    "win_text": DEFAULT_WIN_TEXT,
    "win_photo": None,
    "win_entities": [],

    # Сообщение для заблокированных пользователей при выпадении подарка
    "blocked_text": DEFAULT_BLOCKED_TEXT,
    "blocked_photo": None,
    "blocked_entities": [],

    "ludka_enabled": False,
    "ludka_price": 1,
    "ludka_prize": "подарок какой то",
    "ludka_prize_entities": [],
    "ludka_text": "🎰 Лудка 777 запущена!",
    "ludka_photo": None,
    "ludka_entities": [],
    "ludka_chat_id": None,

    # Мини-ивент «Угадай число»
    "guess_enabled": False,
    "guess_name": "Мини-ивент «Угадай число»!",
    "guess_name_photo": None,
    "guess_name_entities": [],
    "guess_min": 1,
    "guess_max": 100,
    "guess_secret": None,
    "guess_chat_id": None,
    "guess_message_id": None,
    "guess_prize_text": "🏆 Ты угадал число и получаешь приз!",
    "guess_prize_photo": None,
    "guess_prize_entities": [],
    # ID настоящего Telegram-подарка, который автоматом уйдёт победителю
    # ивента «Угадай число» (если не задан — используется общий выбор
    # подарка из раздела «🎁 Подарки», как и в обычном розыгрыше).
    "guess_gift_id": None,

    # /start — редактируемые текст, фото и подписи кнопок магазина
    "start_text": DEFAULT_START_TEXT,
    "start_photo": None,
    "start_entities": [],
    "btn_close_label": DEFAULT_BTN_CLOSE_LABEL,
    "btn_boost_label": DEFAULT_BTN_BOOST_LABEL,
    "btn_my_label": DEFAULT_BTN_MY_LABEL,
    "btn_help_label": DEFAULT_BTN_HELP_LABEL,

    # Цены в Stars и параметры покупок из /start — всё редактируется в админке
    "price_close_chat": DEFAULT_PRICE_CLOSE_CHAT,
    "price_boost": DEFAULT_PRICE_BOOST,
    "close_minutes": DEFAULT_CLOSE_MINUTES,
    "boost_multiplier": DEFAULT_BOOST_MULTIPLIER,
    "boost_hours": DEFAULT_BOOST_HOURS,

    # Первый комментарий бота под новыми постами канала (в группе обсуждения)
    "comment_enabled": False,
    "comment_text": "🐻 Кому мишку?",
    "comment_photo": None,
    "comment_entities": [],
    "comment_chat_id": None,

    # Антиспам: мут за флуд сообщениями (в т.ч. фарм мишек кликами/сообщениями)
    "antispam_enabled": True,
    "antispam_limit": 10,          # сообщений
    "antispam_window": 60,         # секунд
    "antispam_mute_minutes": 20,   # длительность мута
    "antispam_reminder_every": 15, # напоминание о правиле раз в N сообщений чата
    "antispam_text": DEFAULT_ANTISPAM_TEXT,
    "antispam_photo": None,
    "antispam_entities": [],
    "antispam_reason": ANTISPAM_REASON,
    # Редактируемое напоминание о правиле (см. DEFAULT_ANTISPAM_REMINDER_TEXT)
    "antispam_reminder_text": DEFAULT_ANTISPAM_REMINDER_TEXT,
    "antispam_reminder_photo": None,
    "antispam_reminder_entities": [],

    # Тексты после закрытия/открытия чата (!сумрак / !сумракофф / кнопки 🔒 🔓)
    "sumrak_close_text": DEFAULT_SUMRAK_CLOSE_TEXT,
    "sumrak_close_photo": None,
    "sumrak_close_entities": [],
    "sumrak_open_text": DEFAULT_SUMRAK_OPEN_TEXT,
    "sumrak_open_photo": None,
    "sumrak_open_entities": [],

    # Подарки только участникам чата + текст для тех, кто не в чате
    "require_member": True,
    "nonmember_text": DEFAULT_NONMEMBER_TEXT,
    "nonmember_photo": None,
    "nonmember_entities": [],

    # Ручная дополнительная выдача подарка администратором
    "manual_gift_text": DEFAULT_MANUAL_GIFT_TEXT,
    "manual_gift_photo": None,
    "manual_gift_entities": [],

    # Топ халявщиков — автопостинг рейтинга получивших подарки раз в 15 минут
    "top_enabled": False,
    "top_header_text": DEFAULT_TOP_HEADER_TEXT,
    "top_header_entities": [],
    # Список из TOP_PLACES_COUNT словарей {"text": str, "entities": [...]}
    "top_place_labels": [{"text": f"{i}.", "entities": []} for i in range(1, TOP_PLACES_COUNT + 1)],

    # Текст, который уходит победителю при ручной выдаче невыданного подарка
    "pending_issue_text": DEFAULT_PENDING_ISSUE_TEXT,
    "pending_issue_entities": [],
}

stats: Dict[str, int] = {"messages": 0, "wins": 0, "gifts_sent": 0, "errors": 0}

allowed_chat_ids: set = set()

# Telegram usernames, которым запрещена выдача подарков. Храним без @, в lowercase.
blocked_usernames: set = set()

# user_id -> {"multiplier": float, "expires_at": float}
chance_boosts: Dict[int, Dict[str, float]] = {}

# ЛИЧНЫЙ ШАНС: username (без @, lowercase) -> шанс в % (0–100).
# Если username есть в словаре, вместо общего шанса берётся этот.
user_chances: Dict[str, float] = {}

# Привязанные каналы для постов (до MAX_CHANNELS): [{"id", "title", "username"}]
post_channels: List[Dict[str, Any]] = []

# АДМИНЫ БОТА (кроме владельца ADMIN_ID): id -> подпись (username/имя), плюс
# username'ы, добавленные без ID — при первом сообщении такого человека они
# привязываются к его ID (дальше админство держится на ID, а не на username).
bot_admin_ids: Dict[int, str] = {}
bot_admin_usernames: set = set()

# Все, кого бот видел в разрешённых чатах: user_id -> {"u": username, "n": имя, "ts": время}.
# Нужен, чтобы в ручной выдаче можно было указать не только @username/ID,
# но и имя, под которым человек отображается в чате. Хранится в БД.
known_users: Dict[int, Dict[str, Any]] = {}
_known_users_save_scheduled = False

# user_id -> счётчик сообщений в текущем раунде лудки (очищается, см. _prune)
ludka_progress: Dict[int, int] = {}

# chat_id -> время последнего отказа (чтобы не спамить в чужих чатах)
_denied_notice: Dict[int, float] = {}

# (chat_id, user_id) -> список timestamp'ов последних сообщений (антиспам)
_msg_activity: Dict[tuple, List[float]] = {}

# ТОП ХАЛЯВЩИКОВ: user_id (str) -> {"name": str, "username": str, "count": int}.
# Считаются только реально полученные настоящие Telegram-подарки.
gift_winners: Dict[str, Dict] = {}

# НЕВЫДАННЫЕ ПОДАРКИ: список словарей — победители, которым не удалось
# автоматически отправить подарок. Администратор вручную выдаёт/отклоняет
# их из админ-панели (раздел «🎁 Невыданные»).
pending_gifts: List[Dict] = []

# chat_id -> счётчик сообщений с последнего антиспам-напоминания
_spam_reminder_counter: Dict[int, int] = {}

# Последнее сообщение каждого пользователя — нужно, чтобы при РУЧНОЙ выдаче
# из админки отправлять текст/фото не в чат админа, а в тот чат (с ответом
# на то самое сообщение), где человек реально писал последний раз.
# username (без @, lowercase) -> {"user_id", "chat_id", "message_id", "ts"}
last_message_by_username: Dict[str, Dict] = {}
# user_id -> {"chat_id", "message_id", "ts"} — на случай ручной выдачи по ID
last_message_by_user_id: Dict[int, Dict] = {}

# фоновые задачи (хранятся, иначе GC убивает их на середине)
_tasks: set = set()

_http_server: Optional[HTTPServer] = None


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


def _prune(d: dict, limit: int = 5000) -> None:
    """Защита от роста словарей в памяти на долгоживущем процессе."""
    if len(d) > limit:
        for key in list(d.keys())[: len(d) - limit // 2]:
            d.pop(key, None)


def _remember_last_message(update: Update) -> None:
    """Запоминает, в каком чате и каким сообщением человек писал последний
    раз — чтобы потом (при ручной выдаче по @username/ID из админки) можно
    было ответить именно в этом чате, а не в чате админа."""
    user = update.effective_user
    chat = update.effective_chat
    message = update.message
    if not user or not chat or not message or user.is_bot:
        return

    info = {
        "user_id": user.id,
        "chat_id": chat.id,
        "message_id": message.message_id,
        "ts": time.time(),
    }
    last_message_by_user_id[user.id] = info
    _prune(last_message_by_user_id, 10000)

    # Каталог известных пользователей (для ручной выдачи по имени).
    uname_now = _normalize_username(user.username) if user.username else ""
    name_now = (user.full_name or "").strip()
    old = known_users.get(user.id)
    if old is None or old.get("u") != uname_now or old.get("n") != name_now:
        known_users[user.id] = {"u": uname_now, "n": name_now, "ts": time.time()}
        _prune(known_users, 8000)
        _schedule_known_users_save()
    else:
        old["ts"] = time.time()

    if user.username:
        uname = _normalize_username(user.username)
        if uname:
            last_message_by_username[uname] = info
            _prune(last_message_by_username, 10000)


def _schedule_known_users_save() -> None:
    """Сохраняет каталог пользователей в БД не чаще раза в ~30 секунд."""
    global _known_users_save_scheduled
    if _known_users_save_scheduled:
        return
    _known_users_save_scheduled = True

    async def _later() -> None:
        global _known_users_save_scheduled
        try:
            await asyncio.sleep(30)
        finally:
            _known_users_save_scheduled = False
        await _save_known_users()

    _spawn(_later())


async def _save_known_users() -> None:
    try:
        await db.set_setting(
            "known_users", json.dumps({str(k): v for k, v in known_users.items()}, ensure_ascii=False)
        )
    except Exception:
        log.exception("Не удалось сохранить список известных пользователей")


# =========================================================
# ТЕКСТ, ЖИРНЫЙ ШРИФТ И ENTITIES
# =========================================================

def esc(value) -> str:
    """Экранирование для ParseMode.HTML. Без него бот падал на тексте с < & *."""
    return html.escape(str(value), quote=False)


def b(value) -> str:
    """Жирный текст для HTML-разметки."""
    return f"<b>{esc(value)}</b>"


def _u16(text: str) -> int:
    """Длина в UTF-16 code units — именно так Telegram считает offset/length."""
    return len(text.encode("utf-16-le")) // 2


def parse_bold_markers(text: str):
    """
    Превращает **жирный** в настоящие Telegram-entities.
    Работает, если админ не использовал встроенное форматирование Telegram.
    """
    if "**" not in text:
        return text, []

    parts = text.split("**")
    if len(parts) < 3:
        return text, []

    plain = ""
    entities: List[MessageEntity] = []
    for idx, part in enumerate(parts):
        if idx % 2 == 1 and part:
            entities.append(
                MessageEntity(
                    type=MessageEntity.BOLD,
                    offset=_u16(plain),
                    length=_u16(part),
                )
            )
        plain += part
    return plain, entities


def collect_entities(message, is_caption: bool = False):
    """
    Забирает форматирование из сообщения админа (жирный, курсив, Premium Emoji).
    Если форматирования нет — пробует разобрать **звёздочки**.
    """
    raw_text = (message.caption if is_caption else message.text) or ""
    entities = list((message.caption_entities if is_caption else message.entities) or [])
    if entities:
        return raw_text, entities
    return parse_bold_markers(raw_text)


def append_bold(base_text: str, base_entities: List[MessageEntity], addition: str,
                 bold_spans: Optional[List[str]] = None):
    """
    Добавляет текст `addition` после `base_text`, сохраняя entities базового
    текста (они остаются в силе, т.к. вставка идёт строго после них) и делая
    жирными указанные подстроки внутри `addition` (первое вхождение каждой).
    Так можно безопасно склеивать «свой» текст админа (со своим
    форматированием) с текстом, который генерирует сам бот (например, с
    актуальным числовым диапазоном), не ломая offset/length существующих entities.
    """
    entities = list(base_entities or [])
    prefix_len = _u16(base_text)
    for span in bold_spans or []:
        idx = addition.find(span)
        if idx == -1:
            continue
        before = addition[:idx]
        entities.append(
            MessageEntity(
                type=MessageEntity.BOLD,
                offset=prefix_len + _u16(before),
                length=_u16(span),
            )
        )
    return base_text + addition, entities


def _entities_to_json(entities) -> str:
    if not entities:
        return "[]"
    try:
        return json.dumps([e.to_dict() for e in entities], ensure_ascii=False)
    except Exception:
        log.exception("Не удалось сериализовать entities")
        return "[]"


def _entities_from_json(raw) -> List[MessageEntity]:
    if not raw:
        return []
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        result = []
        for item in data or []:
            kwargs = {
                "type": item.get("type"),
                "offset": int(item.get("offset", 0)),
                "length": int(item.get("length", 0)),
            }
            for key in ("url", "language", "custom_emoji_id"):
                if item.get(key) is not None:
                    kwargs[key] = item[key]
            if kwargs["type"]:
                result.append(MessageEntity(**kwargs))
        return result
    except Exception:
        log.exception("Не удалось восстановить entities")
        return []


def render_template(text: str, entities, values: Dict[str, str], mention_user=None):
    """Подставляет {плейсхолдеры} в текст, сохраняя форматирование и Premium Emoji.

    Обычная замена строки ломает entities (у них сдвигаются offset'ы), поэтому
    здесь каждый offset/length пересчитывается в UTF-16. Значения вставляются
    как обычный текст (без HTML). Если передан mention_user, то на месте
    {user} ставится кликабельное упоминание (text_mention), даже без @username.
    Возвращает (текст, список entities)."""
    import re

    text = text or ""
    src_entities = list(entities or [])

    # (начало в исходнике, длина в исходнике, новый текст, это {user}?)
    repls = []
    for m in re.finditer(r"\{(\w+)\}", text):
        key = m.group(1)
        if key in values:
            repls.append((_u16(text[: m.start()]), _u16(m.group(0)), str(values[key]), key == "user"))

    out = []
    pos = 0
    for m in re.finditer(r"\{(\w+)\}", text):
        key = m.group(1)
        if key in values:
            out.append(text[pos: m.start()])
            out.append(str(values[key]))
            pos = m.end()
    out.append(text[pos:])
    new_text = "".join(out)

    def shift_at(point: int) -> int:
        """Сколько символов добавилось/убавилось до точки point исходного текста."""
        total = 0
        for start, len_old, new_val, _is_user in repls:
            if start + len_old <= point:
                total += _u16(new_val) - len_old
        return total

    new_entities: List[MessageEntity] = []
    for e in src_entities:
        start, end = e.offset, e.offset + e.length
        new_start = start + shift_at(start)
        new_end = end + shift_at(end)
        if new_end <= new_start:
            continue
        new_entities.append(_clone_entity_len(e, new_start, new_end - new_start))

    if mention_user is not None:
        # позиции {user} в новом тексте
        delta = 0
        for start, len_old, new, is_user in repls:
            new_len = _u16(new)
            if is_user and new_len:
                new_entities.append(
                    MessageEntity(
                        type=MessageEntity.TEXT_MENTION,
                        offset=start + delta,
                        length=new_len,
                        user=mention_user,
                    )
                )
            delta += new_len - len_old

    return new_text, new_entities


def _clone_entity_len(e: MessageEntity, new_offset: int, new_length: int) -> MessageEntity:
    kwargs = {"type": e.type, "offset": new_offset, "length": new_length}
    for key in ("url", "language", "custom_emoji_id"):
        val = getattr(e, key, None)
        if val is not None:
            kwargs[key] = val
    return MessageEntity(**kwargs)


def _default_reminder_entities() -> List[MessageEntity]:
    """Жирное слово «Напоминание» в стандартном тексте напоминания."""
    text = DEFAULT_ANTISPAM_REMINDER_TEXT
    idx = text.find("Напоминание")
    if idx < 0:
        return []
    return [MessageEntity(type=MessageEntity.BOLD, offset=_u16(text[:idx]), length=_u16("Напоминание"))]


# =========================================================
# ТОП ХАЛЯВЩИКОВ И НЕВЫДАННЫЕ ПОДАРКИ — ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================================================

def _clone_entity(e: MessageEntity, new_offset: int) -> MessageEntity:
    """Копирует entity с новым offset (используется при склейке нескольких
    отдельно отформатированных кусков текста в одно сообщение)."""
    kwargs = {"type": e.type, "offset": new_offset, "length": e.length}
    for key in ("url", "language", "custom_emoji_id"):
        val = getattr(e, key, None)
        if val is not None:
            kwargs[key] = val
    return MessageEntity(**kwargs)


def _concat_parts(parts) -> Tuple[str, List[MessageEntity]]:
    """Склеивает список (текст, entities) в одно сообщение, правильно
    пересчитывая offset'ы entities (в UTF-16 code units, как требует Telegram)."""
    text = ""
    entities: List[MessageEntity] = []
    for part_text, part_entities in parts:
        if not part_text:
            continue
        prefix_len = _u16(text)
        for e in part_entities or []:
            entities.append(_clone_entity(e, prefix_len + e.offset))
        text += part_text
    return text, entities


def _place_labels_to_json(labels: List[Dict]) -> str:
    try:
        data = [
            {"text": item.get("text", ""), "entities": [e.to_dict() for e in item.get("entities") or []]}
            for item in labels
        ]
        return json.dumps(data, ensure_ascii=False)
    except Exception:
        log.exception("Не удалось сериализовать подписи мест топа")
        return "[]"


def _place_labels_from_json(raw) -> List[Dict]:
    default = [{"text": f"{i}.", "entities": []} for i in range(1, TOP_PLACES_COUNT + 1)]
    if not raw:
        return default
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, list):
            return default
        result = []
        for i in range(TOP_PLACES_COUNT):
            item = data[i] if i < len(data) else {}
            if not isinstance(item, dict):
                item = {}
            text = item.get("text")
            if not text:
                text = f"{i + 1}."
            entities = _entities_from_json(json.dumps(item.get("entities") or []))
            result.append({"text": text, "entities": entities})
        return result
    except Exception:
        log.exception("Не удалось загрузить подписи мест топа")
        return default


async def _save_winners() -> None:
    try:
        await db.set_setting("gift_winners", json.dumps(gift_winners, ensure_ascii=False))
    except Exception:
        log.exception("Не удалось сохранить топ халявщиков")


async def _bump_winner(user) -> None:
    """Увеличивает счётчик полученных подарков у пользователя (для топа
    халявщиков). Вызывается только после реально успешной отправки подарка."""
    try:
        key = str(user.id)
        rec = dict(gift_winners.get(key) or {})
        rec["name"] = getattr(user, "full_name", None) or (
            f"@{user.username}" if getattr(user, "username", None) else str(user.id)
        )
        rec["username"] = getattr(user, "username", None) or ""
        rec["count"] = int(rec.get("count", 0)) + 1
        gift_winners[key] = rec
        await _save_winners()
    except Exception:
        log.exception("Не удалось обновить топ халявщиков для user_id=%s", getattr(user, "id", "?"))


def _top_winners(limit: int = TOP_PLACES_COUNT) -> List[Dict]:
    items = []
    for uid, rec in gift_winners.items():
        try:
            count = int(rec.get("count", 0))
        except (TypeError, ValueError):
            count = 0
        if count <= 0:
            continue
        items.append({
            "user_id": uid,
            "name": rec.get("name") or "Без имени",
            "username": rec.get("username") or "",
            "count": count,
        })
    items.sort(key=lambda x: -x["count"])
    return items[:limit]


def _build_leaderboard_message() -> Tuple[str, List[MessageEntity]]:
    top = _top_winners(TOP_PLACES_COUNT)
    parts = [(S["top_header_text"], S["top_header_entities"] or [])]
    if not top:
        parts.append((DEFAULT_TOP_EMPTY_TEXT, []))
    else:
        labels = S["top_place_labels"]
        for i, item in enumerate(top):
            label = labels[i] if i < len(labels) else {"text": f"{i + 1}.", "entities": []}
            name = f"@{item['username']}" if item.get("username") else f"ID {item['user_id']}"
            prefix = f"{label['text']} " if label.get("text") else ""
            parts.append(("\n\n", []))
            parts.append((label.get("text", ""), label.get("entities") or []))
            parts.append((f" {name} — {item['count']} 🎁" if prefix else f"{name} — {item['count']} 🎁", []))
    return _concat_parts(parts)


async def post_leaderboard(bot) -> None:
    """Рассылает топ халявщиков во все доступные чаты."""
    if not allowed_chat_ids:
        return
    text, entities = _build_leaderboard_message()
    if _u16(text) > 4096:
        text = text[:4000] + "…"
        entities = [e for e in entities if e.offset + e.length <= _u16(text)]
    for chat_id in list(allowed_chat_ids):
        try:
            await bot.send_message(chat_id=chat_id, text=text, entities=entities or None)
        except TelegramError:
            log.exception("Не удалось отправить топ халявщиков в чат %s", chat_id)


async def _save_pending() -> None:
    try:
        await db.set_setting("pending_gifts", json.dumps(pending_gifts, ensure_ascii=False))
    except Exception:
        log.exception("Не удалось сохранить невыданные подарки")


def _next_pending_id() -> int:
    return (max((int(x.get("id", 0)) for x in pending_gifts), default=0)) + 1


async def _add_pending(user, chat_id: int, message_id: Optional[int], gift_id) -> int:
    new_id = _next_pending_id()
    pending_gifts.append({
        "id": new_id,
        "user_id": user.id,
        "name": getattr(user, "full_name", None) or (
            f"@{user.username}" if getattr(user, "username", None) else str(user.id)
        ),
        "username": getattr(user, "username", None) or "",
        "chat_id": chat_id,
        "message_id": message_id,
        "gift_id": gift_id,
        "created_at": time.time(),
    })
    await _save_pending()
    return new_id


async def _pop_pending(pending_id: int) -> Optional[Dict]:
    for i, item in enumerate(pending_gifts):
        if int(item.get("id", -1)) == int(pending_id):
            found = pending_gifts.pop(i)
            await _save_pending()
            return found
    return None


class _PendingUserProxy:
    """Лёгкая замена telegram.User для повторной попытки выдачи подарка
    невыданному победителю (у нас есть только его id/username/имя, а не
    полноценный объект User)."""

    def __init__(self, data: Dict):
        self.id = data["user_id"]
        self.username = data.get("username") or None
        self.full_name = data.get("name") or str(data["user_id"])


# =========================================================
# СОХРАНЕНИЕ / ЗАГРУЗКА
# =========================================================

async def _db_set(key: str, value) -> None:
    try:
        await db.set_setting(key, "" if value is None else str(value))
    except Exception:
        log.exception("Не удалось сохранить настройку %s", key)


async def _db_set_entities(key: str, entities) -> None:
    try:
        await db.set_setting(key, _entities_to_json(entities))
    except Exception:
        log.exception("Не удалось сохранить entities %s", key)


async def _db_inc_stat(name: str, amount: int = 1) -> None:
    try:
        await db.increment_stat(name, amount)
    except Exception:
        log.exception("Не удалось сохранить статистику %s", name)


async def load_persistent_state() -> None:
    S["chance"] = _clamp_chance(await db.get_float_setting("chance", float(S["chance"])))
    S["giveaway_enabled"] = await db.get_bool_setting("giveaway_enabled", True)
    S["selected_gift_id"] = await db.get_setting("selected_gift_id", None)

    allowed_chat_ids.clear()
    allowed_chat_ids.update(await db.load_allowed_chats())

    blocked_usernames.clear()
    blocked_usernames.update(await db.load_blocked_usernames())

    S["ludka_enabled"] = await db.get_bool_setting("ludka_enabled", False)
    S["ludka_price"] = max(1, await db.get_int_setting("ludka_price", 1))
    S["ludka_prize"] = await db.get_setting("ludka_prize", S["ludka_prize"])
    S["ludka_prize_entities"] = _entities_from_json(
        await db.get_setting("ludka_prize_entities", "[]")
    )
    S["ludka_text"] = await db.get_setting("ludka_text", S["ludka_text"])
    S["ludka_photo"] = await db.get_setting("ludka_photo", None)
    S["ludka_entities"] = _entities_from_json(await db.get_setting("ludka_entities", "[]"))
    S["ludka_chat_id"] = await db.get_int_setting("ludka_chat_id", 0) or None

    S["guess_enabled"] = await db.get_bool_setting("guess_enabled", False)
    S["guess_name"] = await db.get_setting("guess_name", S["guess_name"])
    S["guess_name_photo"] = await db.get_setting("guess_name_photo", None)
    S["guess_name_entities"] = _entities_from_json(
        await db.get_setting("guess_name_entities", "[]")
    )
    S["guess_min"] = await db.get_int_setting("guess_min", S["guess_min"])
    S["guess_max"] = await db.get_int_setting("guess_max", S["guess_max"])
    # ВАЖНО: не используем `x or None` — загаданное число 0 это валидное
    # значение, а `0 or None` превратилось бы в None и раунд бы "терялся"
    # после перезапуска бота.
    _raw_secret = await db.get_setting("guess_secret", None)
    S["guess_secret"] = int(_raw_secret) if _raw_secret not in (None, "") else None
    S["guess_chat_id"] = await db.get_int_setting("guess_chat_id", 0) or None
    S["guess_message_id"] = await db.get_int_setting("guess_message_id", 0) or None
    S["guess_prize_text"] = await db.get_setting("guess_prize_text", S["guess_prize_text"])
    S["guess_prize_photo"] = await db.get_setting("guess_prize_photo", None)
    S["guess_prize_entities"] = _entities_from_json(
        await db.get_setting("guess_prize_entities", "[]")
    )
    S["guess_gift_id"] = await db.get_setting("guess_gift_id", None)

    S["start_text"] = await db.get_setting("start_text", S["start_text"])
    S["start_photo"] = await db.get_setting("start_photo", None)
    S["start_entities"] = _entities_from_json(await db.get_setting("start_entities", "[]"))
    S["btn_close_label"] = await db.get_setting("btn_close_label", S["btn_close_label"])
    S["btn_boost_label"] = await db.get_setting("btn_boost_label", S["btn_boost_label"])
    S["btn_my_label"] = await db.get_setting("btn_my_label", S["btn_my_label"])
    S["btn_help_label"] = await db.get_setting("btn_help_label", S["btn_help_label"])

    S["win_success_text"] = await db.get_setting("win_success_text", DEFAULT_WIN_SUCCESS_TEXT)
    S["win_success_photo"] = await db.get_setting("win_success_photo", None)
    S["win_success_entities"] = _entities_from_json(
        await db.get_setting("win_success_entities", "[]")
    )

    S["win_text"] = await db.get_setting("win_text", DEFAULT_WIN_TEXT)
    S["win_photo"] = await db.get_setting("win_photo", None)
    S["win_entities"] = _entities_from_json(await db.get_setting("win_entities", "[]"))

    S["blocked_text"] = await db.get_setting("blocked_text", DEFAULT_BLOCKED_TEXT)
    S["blocked_photo"] = await db.get_setting("blocked_photo", None)
    S["blocked_entities"] = _entities_from_json(await db.get_setting("blocked_entities", "[]"))

    S["price_close_chat"] = max(1, await db.get_int_setting("price_close_chat", DEFAULT_PRICE_CLOSE_CHAT))
    S["price_boost"] = max(1, await db.get_int_setting("price_boost", DEFAULT_PRICE_BOOST))
    S["close_minutes"] = max(1, await db.get_int_setting("close_minutes", DEFAULT_CLOSE_MINUTES))
    S["boost_multiplier"] = max(1.0, await db.get_float_setting("boost_multiplier", DEFAULT_BOOST_MULTIPLIER))
    S["boost_hours"] = max(0.1, await db.get_float_setting("boost_hours", DEFAULT_BOOST_HOURS))

    S["comment_enabled"] = await db.get_bool_setting("comment_enabled", False)
    S["comment_text"] = await db.get_setting("comment_text", S["comment_text"])
    S["comment_photo"] = await db.get_setting("comment_photo", None)
    S["comment_entities"] = _entities_from_json(await db.get_setting("comment_entities", "[]"))
    S["comment_chat_id"] = await db.get_int_setting("comment_chat_id", 0) or None

    S["antispam_enabled"] = await db.get_bool_setting("antispam_enabled", True)
    S["antispam_limit"] = max(1, await db.get_int_setting("antispam_limit", S["antispam_limit"]))
    S["antispam_window"] = max(5, await db.get_int_setting("antispam_window", S["antispam_window"]))
    S["antispam_mute_minutes"] = max(
        1, await db.get_int_setting("antispam_mute_minutes", S["antispam_mute_minutes"])
    )
    S["antispam_reminder_every"] = max(
        0, await db.get_int_setting("antispam_reminder_every", S["antispam_reminder_every"])
    )
    S["antispam_text"] = await db.get_setting("antispam_text", DEFAULT_ANTISPAM_TEXT)
    S["antispam_photo"] = await db.get_setting("antispam_photo", None)
    S["antispam_entities"] = _entities_from_json(await db.get_setting("antispam_entities", "[]"))
    S["antispam_reason"] = await db.get_setting("antispam_reason", ANTISPAM_REASON)

    _rem_raw = await db.get_setting("antispam_reminder_text", None)
    S["antispam_reminder_text"] = _rem_raw or DEFAULT_ANTISPAM_REMINDER_TEXT
    S["antispam_reminder_photo"] = await db.get_setting("antispam_reminder_photo", None)
    if _rem_raw is None:
        S["antispam_reminder_entities"] = _default_reminder_entities()
    else:
        S["antispam_reminder_entities"] = _entities_from_json(
            await db.get_setting("antispam_reminder_entities", "[]")
        )

    for _pref, _default in (
        ("sumrak_close", DEFAULT_SUMRAK_CLOSE_TEXT),
        ("sumrak_open", DEFAULT_SUMRAK_OPEN_TEXT),
        ("nonmember", DEFAULT_NONMEMBER_TEXT),
    ):
        S[f"{_pref}_text"] = await db.get_setting(f"{_pref}_text", _default) or _default
        S[f"{_pref}_photo"] = await db.get_setting(f"{_pref}_photo", None) or None
        S[f"{_pref}_entities"] = _entities_from_json(await db.get_setting(f"{_pref}_entities", "[]"))
    S["require_member"] = await db.get_bool_setting("require_member", True)

    post_channels.clear()
    try:
        raw_ch = await db.get_setting("post_channels", "[]")
        data_ch = json.loads(raw_ch) if isinstance(raw_ch, str) else raw_ch
        if isinstance(data_ch, list):
            for ch in data_ch[:MAX_CHANNELS]:
                if isinstance(ch, dict) and ch.get("id"):
                    post_channels.append({"id": int(ch["id"]), "title": str(ch.get("title") or ch["id"]),
                                          "username": str(ch.get("username") or "")})
    except Exception:
        log.exception("Не удалось загрузить каналы")

    bot_admin_ids.clear()
    bot_admin_usernames.clear()
    try:
        raw_ba = await db.get_setting("bot_admins", "{}")
        data_ba = json.loads(raw_ba) if isinstance(raw_ba, str) else raw_ba
        if isinstance(data_ba, dict):
            for k, v in (data_ba.get("ids") or {}).items():
                bot_admin_ids[int(k)] = str(v or "")
            for name in data_ba.get("usernames") or []:
                name = _normalize_username(str(name))
                if name:
                    bot_admin_usernames.add(name)
    except Exception:
        log.exception("Не удалось загрузить админов бота")

    user_chances.clear()
    try:
        raw_uc = await db.get_setting("user_chances", "{}")
        data_uc = json.loads(raw_uc) if isinstance(raw_uc, str) else raw_uc
        if isinstance(data_uc, dict):
            for k, v in data_uc.items():
                name = _normalize_username(str(k))
                if name:
                    user_chances[name] = max(0.0, min(float(v), MAX_CHANCE))
    except Exception:
        log.exception("Не удалось загрузить личные шансы")

    known_users.clear()
    try:
        raw_ku = await db.get_setting("known_users", "{}")
        data_ku = json.loads(raw_ku) if isinstance(raw_ku, str) else raw_ku
        if isinstance(data_ku, dict):
            for k, v in data_ku.items():
                if isinstance(v, dict):
                    known_users[int(k)] = v
    except Exception:
        log.exception("Не удалось загрузить список известных пользователей")

    S["manual_gift_text"] = await db.get_setting("manual_gift_text", DEFAULT_MANUAL_GIFT_TEXT)
    S["manual_gift_photo"] = await db.get_setting("manual_gift_photo", None)
    S["manual_gift_entities"] = _entities_from_json(await db.get_setting("manual_gift_entities", "[]"))

    S["top_enabled"] = await db.get_bool_setting("top_enabled", False)
    S["top_header_text"] = await db.get_setting("top_header_text", DEFAULT_TOP_HEADER_TEXT)
    S["top_header_entities"] = _entities_from_json(await db.get_setting("top_header_entities", "[]"))
    S["top_place_labels"] = _place_labels_from_json(await db.get_setting("top_place_labels", None))

    S["pending_issue_text"] = await db.get_setting("pending_issue_text", DEFAULT_PENDING_ISSUE_TEXT)
    S["pending_issue_entities"] = _entities_from_json(
        await db.get_setting("pending_issue_entities", "[]")
    )

    gift_winners.clear()
    try:
        raw_winners = await db.get_setting("gift_winners", "{}")
        data_winners = json.loads(raw_winners) if isinstance(raw_winners, str) else raw_winners
        if isinstance(data_winners, dict):
            gift_winners.update(data_winners)
    except Exception:
        log.exception("Не удалось загрузить топ халявщиков")

    pending_gifts.clear()
    try:
        raw_pending = await db.get_setting("pending_gifts", "[]")
        data_pending = json.loads(raw_pending) if isinstance(raw_pending, str) else raw_pending
        if isinstance(data_pending, list):
            pending_gifts.extend(data_pending)
    except Exception:
        log.exception("Не удалось загрузить невыданные подарки")

    stats.update(await db.load_stats())
    chance_boosts.clear()
    chance_boosts.update(await db.load_active_boosts())

    log.info(
        "Состояние загружено: шанс=%s%%, розыгрыш=%s, чатов=%s, бустов=%s",
        S["chance"], S["giveaway_enabled"], len(allowed_chat_ids), len(chance_boosts),
    )


# =========================================================
# ШАНС
# =========================================================

def _clamp_chance(value: float) -> float:
    """Ограничивает базовый шанс диапазоном 0.01% – 100%."""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return MIN_CHANCE
    return max(MIN_CHANCE, min(value, MAX_CHANCE))


def fmt_num(value) -> str:
    """Компактный вывод числа: 1.0 -> '1', 0.50 -> '0.5', 0.01 -> '0.01'."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)
    if f == int(f):
        return str(int(f))
    return f"{f:g}"


def boost_multiplier(user_id: int) -> float:
    boost = chance_boosts.get(user_id)
    if not boost:
        return 1.0
    if boost["expires_at"] <= time.time():
        chance_boosts.pop(user_id, None)
        return 1.0
    return float(boost["multiplier"])


def get_chance(user_id: Optional[int] = None, username: Optional[str] = None) -> float:
    """Шанс выигрыша для человека. Если для его @username задан личный шанс
    (раздел «👤 Личный шанс») — берётся он вместо общего; купленный буст
    умножается поверх в обоих случаях."""
    base = float(S["chance"])
    if username:
        personal = user_chances.get(_normalize_username(username))
        if personal is not None:
            base = float(personal)
    if user_id is not None:
        base *= boost_multiplier(user_id)
    return max(0.0, min(base, 100.0))


# =========================================================
# HTTP SERVER ДЛЯ RENDER
# =========================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Telegram Gift Bot is running!")

    def log_message(self, fmt, *args):
        pass


def run_web_server() -> None:
    global _http_server
    try:
        HTTPServer.allow_reuse_address = True
        _http_server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        _http_server.serve_forever()
    except Exception:
        log.exception("HTTP-сервер остановлен")


# =========================================================
# АВТО-ПОДДЕРЖАНИЕ РАБОТЫ (каждые 20 минут)
# =========================================================

KEEPALIVE_INTERVAL = 20 * 60  # 20 минут

# URL, который бот сам себе пингует, чтобы Render/аналоги не усыпляли
# бесплатный веб-сервис из-за отсутствия внешних запросов.
SELF_URL = (os.environ.get("SELF_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip()


async def _keepalive_once(application) -> None:
    # 1) Проверяем, что соединение с Telegram живо.
    try:
        await application.bot.get_me()
    except Exception:
        log.exception("Keep-alive: бот не отвечает Telegram")

    # 2) Самопинг, чтобы хостинг не усыплял процесс из-за неактивности.
    if SELF_URL:
        try:
            import urllib.request
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: urllib.request.urlopen(SELF_URL, timeout=15)
            )
        except Exception:
            log.warning("Keep-alive: не удалось выполнить self-ping %s", SELF_URL)

    log.info("💓 Keep-alive: бот активен")


async def keepalive_loop(application) -> None:
    """Раз в 20 минут подтверждает, что бот жив, и пингует себя, чтобы
    бесплатный хостинг не усыплял процесс из-за отсутствия трафика."""
    while True:
        try:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            await _keepalive_once(application)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка в цикле keep-alive")


async def leaderboard_loop(application) -> None:
    """Раз в 15 минут постит топ халявщиков во все доступные чаты
    (если раздел включён в админ-панели)."""
    while True:
        try:
            await asyncio.sleep(LEADERBOARD_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        try:
            if S.get("top_enabled"):
                await post_leaderboard(application.bot)
        except Exception:
            log.exception("Ошибка автопостинга топа халявщиков")


# =========================================================
# ПРОВЕРКА АДМИНА
# =========================================================

def is_admin(update: Update) -> bool:
    """Владелец бота (ADMIN_ID): админ-панель и все настройки."""
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID)


def is_bot_admin_user(user) -> bool:
    """Владелец ИЛИ добавленный им админ бота. Админ, добавленный по @username,
    при первом же сообщении привязывается к своему ID."""
    if not user:
        return False
    if user.id == ADMIN_ID or user.id in bot_admin_ids:
        return True
    uname = _normalize_username(user.username) if user.username else ""
    if uname and uname in bot_admin_usernames:
        bot_admin_usernames.discard(uname)
        bot_admin_ids[user.id] = uname
        _spawn(_save_bot_admins())
        return True
    return False


async def _save_bot_admins() -> None:
    await _db_set("bot_admins", json.dumps(
        {"ids": {str(k): v for k, v in bot_admin_ids.items()},
         "usernames": sorted(bot_admin_usernames)},
        ensure_ascii=False,
    ))


# =========================================================
# /START — магазин за Telegram Stars
# =========================================================

def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(
                f"{S['btn_close_label']} — {S['price_close_chat']} ⭐",
                callback_data="buy:close",
            )],
            [InlineKeyboardButton(
                f"{S['btn_boost_label']} — {S['price_boost']} ⭐",
                callback_data="buy:boost",
            )],
            [InlineKeyboardButton(str(S["btn_my_label"]), callback_data="buy:my")],
            [InlineKeyboardButton(str(S["btn_help_label"]), callback_data="buy:help")],
        ]
    )


def _build_start_message():
    """Собирает текст /start: просто текст/фото, заданные админом, без
    автоматически дописанной информации о шансе и ценах магазина."""
    base_text = S["start_text"] or DEFAULT_START_TEXT
    base_entities = S["start_entities"] or []
    return base_text, base_entities


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    text, entities = _build_start_message()
    try:
        if S["start_photo"]:
            await update.message.reply_photo(
                photo=S["start_photo"],
                caption=text,
                caption_entities=entities or None,
                reply_markup=start_keyboard(),
            )
        else:
            await update.message.reply_text(
                text, entities=entities or None, reply_markup=start_keyboard(),
            )
    except TelegramError:
        log.exception("Ошибка отправки /start")


async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data
    user = update.effective_user

    if data == "buy:help":
        await query.edit_message_text(
            f"ℹ️ {b('КАК ЭТО РАБОТАЕТ')}\n\n"
            f"🔒 {b('Закрыть чат')} — бот запрещает писать всем участникам "
            f"выбранного чата на {fmt_num(S['close_minutes'])} минут, затем сам открывает его обратно. "
            "Бот должен быть администратором чата с правом ограничивать участников.\n\n"
            f"🍀 {b('Повысить шанс')} — твой личный шанс выиграть подарок "
            f"умножается на {fmt_num(S['boost_multiplier'])} и действует {fmt_num(S['boost_hours'])} часов.\n\n"
            "Оплата проходит через Telegram Stars. Если действие не удалось "
            "выполнить, звёзды возвращаются автоматически.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="buy:menu")]]
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "buy:menu":
        await query.edit_message_text(
            f"🎁 {b('МАГАЗИН')}\n\nВыбери действие:",
            reply_markup=start_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "buy:my":
        mult = boost_multiplier(user.id)
        if mult > 1:
            left = int((chance_boosts[user.id]["expires_at"] - time.time()) / 60)
            text = (
                f"🍀 Активен буст ×{mult:g}\n"
                f"⏳ Осталось: {b(str(left) + ' мин')}\n"
                f"🎯 Твой шанс: {b(f'{get_chance(user.id, user.username):.2f}%')}"
            )
        else:
            text = (
                "У тебя нет активных покупок.\n\n"
                f"🎯 Твой шанс: {b(f'{get_chance(user.id, user.username):.2f}%')}"
            )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="buy:menu")]]
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "buy:boost":
        await send_star_invoice(
            context,
            chat_id=query.message.chat_id,
            title="Повышенный шанс на подарок",
            description=(
                f"Шанс выиграть подарок умножается на {fmt_num(S['boost_multiplier'])} "
                f"на {fmt_num(S['boost_hours'])} часов."
            ),
            payload="boost",
            amount=int(S["price_boost"]),
            label="Повышенный шанс",
        )
        return

    if data == "buy:close":
        if not allowed_chat_ids:
            await query.edit_message_text(
                "❌ Ни один чат ещё не подключён к боту.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data="buy:menu")]]
                ),
            )
            return

        if len(allowed_chat_ids) == 1:
            chat_id = next(iter(allowed_chat_ids))
            await _invoice_close_chat(context, query.message.chat_id, chat_id)
            return

        buttons = []
        for chat_id in list(allowed_chat_ids)[:MAX_ALLOWED_CHATS]:
            title = str(chat_id)
            try:
                chat = await context.bot.get_chat(chat_id)
                title = chat.title or title
            except TelegramError:
                pass
            buttons.append(
                [InlineKeyboardButton(f"🔒 {title}", callback_data=f"buy:close:{chat_id}")]
            )
        buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="buy:menu")])

        await query.edit_message_text(
            "Выбери чат, который нужно закрыть:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    if data.startswith("buy:close:"):
        try:
            chat_id = int(data.split(":")[2])
        except (IndexError, ValueError):
            await query.answer("❌ Некорректный чат", show_alert=True)
            return
        await _invoice_close_chat(context, query.message.chat_id, chat_id)
        return


async def _invoice_close_chat(context, to_chat_id: int, target_chat_id: int) -> None:
    await send_star_invoice(
        context,
        chat_id=to_chat_id,
        title="Закрытие чата",
        description=f"Чат будет закрыт на {fmt_num(S['close_minutes'])} минут.",
        payload=f"close:{target_chat_id}",
        amount=int(S["price_close_chat"]),
        label=f"Закрытие чата на {fmt_num(S['close_minutes'])} мин",
    )


async def send_star_invoice(
    context, chat_id: int, title: str, description: str,
    payload: str, amount: int, label: str,
) -> None:
    try:
        await context.bot.send_invoice(
            chat_id=chat_id,
            title=title,
            description=description,
            payload=payload,
            provider_token="",          # для Telegram Stars токен не нужен
            currency="XTR",
            prices=[LabeledPrice(label=label, amount=amount)],
        )
    except TelegramError:
        log.exception("Не удалось выставить счёт (%s)", payload)
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Не удалось создать счёт. Попробуй позже.",
        )


# =========================================================
# ПЛАТЕЖИ
# =========================================================

async def precheckout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    payload = query.invoice_payload or ""

    if payload == "boost" or payload.startswith("close:"):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Товар больше не доступен.")


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    payment = update.message.successful_payment
    user = update.effective_user
    payload = payment.invoice_payload or ""
    charge_id = payment.telegram_payment_charge_id

    try:
        await db.save_payment(charge_id, user.id, payment.total_amount, payload)
    except Exception:
        log.exception("Не удалось сохранить платёж")

    # ---------- ПОВЫШЕННЫЙ ШАНС ----------
    if payload == "boost":
        boost_mult = float(S["boost_multiplier"])
        boost_hours = float(S["boost_hours"])
        expires = time.time() + boost_hours * 3600
        chance_boosts[user.id] = {"multiplier": boost_mult, "expires_at": expires}
        _prune(chance_boosts)
        await db.set_boost(user.id, boost_mult, expires)

        await update.message.reply_text(
            f"✅ {b('Буст активирован!')}\n\n"
            f"🍀 Шанс ×{fmt_num(boost_mult)} на {fmt_num(boost_hours)} часов\n"
            f"🎯 Твой шанс сейчас: {b(f'{get_chance(user.id, user.username):.2f}%')}",
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------- ЗАКРЫТИЕ ЧАТА ----------
    if payload.startswith("close:"):
        try:
            chat_id = int(payload.split(":", 1)[1])
        except ValueError:
            await _refund(context, user.id, charge_id, update.message)
            return

        try:
            await context.bot.set_chat_permissions(chat_id, CLOSED_PERMS)
        except TelegramError as e:
            log.exception("Не удалось закрыть чат %s", chat_id)
            await update.message.reply_text(
                f"❌ Не удалось закрыть чат: {esc(e)}\n\n"
                "Скорее всего у бота нет прав администратора. "
                "Возвращаю звёзды.",
                parse_mode=ParseMode.HTML,
            )
            await _refund(context, user.id, charge_id, update.message)
            return

        close_minutes = float(S["close_minutes"])
        await db.set_setting(f"closed_until:{chat_id}", str(time.time() + close_minutes * 60))
        _spawn(_reopen_later(context, chat_id, close_minutes * 60))

        await update.message.reply_text(
            f"🔒 {b('Чат закрыт')} на {fmt_num(close_minutes)} минут.",
            parse_mode=ParseMode.HTML,
        )
        try:
            await context.bot.send_message(
                chat_id,
                f"🔒 {b('ЧАТ ЗАКРЫТ')}\n\n"
                f"⏳ На {fmt_num(close_minutes)} минут\n"
                f"⭐ Оплачено: {S['price_close_chat']} звёзд",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
        return

    log.warning("Неизвестный payload платежа: %s", payload)


async def _refund(context, user_id: int, charge_id: str, message=None) -> None:
    try:
        await context.bot.refund_star_payment(user_id, charge_id)
        if message:
            await message.reply_text("💸 Звёзды возвращены.")
    except TelegramError:
        log.exception("Не удалось вернуть звёзды по %s", charge_id)


async def _reopen_later(context, chat_id: int, delay: float) -> None:
    try:
        await asyncio.sleep(max(0.0, delay))
        # Если чат уже открыли/закрыли вручную (кнопка, !сумрак/!сумракофф) или
        # таймер перезапущен покупкой — этот старый таймер ничего не делает.
        raw = await db.get_setting(f"closed_until:{chat_id}", None)
        try:
            if not raw or float(raw) > time.time() + 5:
                return
        except ValueError:
            return
        await _set_chat_closed(context.bot, chat_id, False, announce=True)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Не удалось открыть чат %s", chat_id)


async def restore_closed_chats(application) -> None:
    """После рестарта бота возвращаем отложенное открытие чатов."""
    class _Ctx:
        bot = application.bot

    for chat_id in list(allowed_chat_ids):
        raw = await db.get_setting(f"closed_until:{chat_id}", None)
        if not raw:
            continue
        try:
            until = float(raw)
        except ValueError:
            continue
        _spawn(_reopen_later(_Ctx(), chat_id, until - time.time()))


# =========================================================
# /BOLD — жирный текст
# =========================================================

async def bold_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    text = " ".join(context.args or []).strip()
    if not text and update.message.reply_to_message:
        text = update.message.reply_to_message.text or ""

    if not text:
        await update.message.reply_text(
            f"✏️ {b('ЖИРНЫЙ ТЕКСТ')}\n\n"
            "Использование:\n"
            "<code>/bold привет мир</code>\n\n"
            "Либо ответь этой командой на любое сообщение.\n\n"
            "В настройках бота можно писать <code>**вот так**</code> — "
            "текст между звёздочками станет жирным.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(b(text), parse_mode=ParseMode.HTML)


# =========================================================
# АДМИН-ПАНЕЛЬ
# =========================================================

def admin_keyboard() -> InlineKeyboardMarkup:
    status = "🟢 ВКЛЮЧЕН" if S["giveaway_enabled"] else "🔴 ВЫКЛЮЧЕН"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🎯 Шанс", callback_data="chance"),
                InlineKeyboardButton("🎁 Подарки", callback_data="gifts"),
            ],
            [
                InlineKeyboardButton("💰 Stars", callback_data="balance"),
                InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            ],
            [InlineKeyboardButton(f"🎲 Розыгрыш: {status}", callback_data="toggle")],
            [InlineKeyboardButton(
                f"👤 Личный шанс ({len(user_chances)})", callback_data="userchance",
            )],
            [InlineKeyboardButton(
                f"👮 Админы бота ({len(bot_admin_ids) + len(bot_admin_usernames)})",
                callback_data="botadmins",
            )],
            [InlineKeyboardButton("🎰 Лудка 777", callback_data="ludka")],
            [InlineKeyboardButton("🔢 Угадай число", callback_data="guess")],
            [InlineKeyboardButton("💬 Комментарий к постам", callback_data="comment")],
            [InlineKeyboardButton(
                "🛡 Антиспам: " + ("🟢 ВКЛ" if S["antispam_enabled"] else "🔴 ВЫКЛ"),
                callback_data="antispam",
            )],
            [InlineKeyboardButton(
                f"📢 Каналы и посты ({len(post_channels)}/{MAX_CHANNELS})", callback_data="channels",
            )],
            [InlineKeyboardButton("🌗 Сумрак: тексты чата", callback_data="sumrak_menu")],
            [InlineKeyboardButton(
                "🚪 Подарки только участникам: " + ("🟢 ВКЛ" if S["require_member"] else "🔴 ВЫКЛ"),
                callback_data="members",
            )],
            [InlineKeyboardButton("🎁 Ручная выдача", callback_data="manual_gift")],
            [InlineKeyboardButton("🏠 /start: текст и кнопки", callback_data="startedit")],
            [InlineKeyboardButton("✏️ Сообщения о подарке", callback_data="winmessage")],
            [InlineKeyboardButton("💵 Цены и параметры", callback_data="prices")],
            [InlineKeyboardButton(
                f"🔐 Доступные чаты ({len(allowed_chat_ids)}/{MAX_ALLOWED_CHATS})",
                callback_data="access",
            )],
            [InlineKeyboardButton(
                f"🚫 Блокировка выдач ({len(blocked_usernames)})",
                callback_data="blocked",
            )],
            [InlineKeyboardButton("👤 Аккаунт выдачи", callback_data="account")],
            [InlineKeyboardButton(
                "🏆 Топ халявщиков: " + ("🟢 ВКЛ" if S["top_enabled"] else "🔴 ВЫКЛ"),
                callback_data="top",
            )],
            [InlineKeyboardButton(
                f"🎁 Невыданные ({len(pending_gifts)})", callback_data="pending",
            )],
            [InlineKeyboardButton("🔄 Обновить подарки", callback_data="refresh")],
        ]
    )


def back_kb(target: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Назад", callback_data=target)]]
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("❌ Админ-панель доступна только владельцу бота.")
        return

    await update.message.reply_text(
        f"🛠 {b('АДМИН-ПАНЕЛЬ')}\n\nВыбери действие ниже.",
        reply_markup=admin_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query

    if not is_admin(update):
        await query.answer("❌ Только для администратора.", show_alert=True)
        return

    await query.answer()
    data = query.data

    # ---------------- ГЛАВНОЕ МЕНЮ ----------------
    if data == "main":
        await query.edit_message_text(
            f"🛠 {b('АДМИН-ПАНЕЛЬ')}\n\nВыбери действие:",
            reply_markup=admin_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- ШАНС ----------------
    if data == "chance":
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("0.01%", callback_data="setchance:0.01"),
                    InlineKeyboardButton("0.1%", callback_data="setchance:0.1"),
                ],
                [
                    InlineKeyboardButton("0.5%", callback_data="setchance:0.5"),
                    InlineKeyboardButton("1%", callback_data="setchance:1"),
                ],
                [
                    InlineKeyboardButton("2%", callback_data="setchance:2"),
                    InlineKeyboardButton("5%", callback_data="setchance:5"),
                ],
                [
                    InlineKeyboardButton("10%", callback_data="setchance:10"),
                    InlineKeyboardButton("100%", callback_data="setchance:100"),
                ],
                [InlineKeyboardButton("✏️ Своё значение", callback_data="chance_custom")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="main")],
            ]
        )
        await query.edit_message_text(
            f"🎯 {b('НАСТРОЙКА ШАНСА')}\n\n"
            f"Сейчас: {b(fmt_num(S['chance']) + '%')}\n\n"
            f"Диапазон: от {MIN_CHANCE}% до {fmt_num(MAX_CHANCE)}%.\n"
            "Или команда: <code>/chance 0.5</code>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("setchance:"):
        try:
            value = _clamp_chance(float(data.split(":", 1)[1]))
        except ValueError:
            await query.answer("❌ Некорректное значение", show_alert=True)
            return

        S["chance"] = value
        await _db_set("chance", value)
        await query.edit_message_text(
            f"✅ {b(f'Шанс установлен: {fmt_num(value)}%')}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "chance_custom":
        _clear_waiting(context)
        context.user_data["waiting_chance"] = True
        await query.edit_message_text(
            f"✏️ {b('СВОЁ ЗНАЧЕНИЕ ШАНСА')}\n\n"
            f"Отправь число от {MIN_CHANCE} до {fmt_num(MAX_CHANCE)} (можно с точкой), "
            "например: <code>0.01</code>, <code>3.25</code>, <code>17</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("chance"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- АККАУНТ ВЫДАЧИ ----------------
    if data == "account":
        try:
            status = await gift_account.account_status()
        except Exception as e:
            log.exception("Ошибка статуса аккаунта")
            status = {"connected": False, "error": str(e)}

        if not status.get("connected"):
            text = (
                f"👤 {b('АККАУНТ ВЫДАЧИ')}\n\n"
                "🔴 Аккаунт не привязан.\n\n"
                "После привязки подарки победителям будут покупаться "
                "со Stars этого аккаунта через MTProto.\n\n"
                "⚠️ Сессия хранится в базе в зашифрованном виде."
            )
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔐 Привязать аккаунт", callback_data="account:connect")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="main")],
                ]
            )
        else:
            username = f"@{status['username']}" if status.get("username") else "без username"
            balance = status.get("balance")
            text = (
                f"👤 {b('АККАУНТ ВЫДАЧИ')}\n\n"
                "🟢 Подключен\n"
                f"👤 {esc(status.get('name', 'Аккаунт'))}\n"
                f"🔗 {esc(username)}\n"
                f"🆔 <code>{status.get('user_id')}</code>\n"
                f"⭐ Баланс: {b(balance if balance is not None else 'не удалось получить')}"
            )
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Обновить баланс", callback_data="account:refresh")],
                    [InlineKeyboardButton("🔓 Отвязать аккаунт", callback_data="account:disconnect")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="main")],
                ]
            )

        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        return

    if data == "account:connect":
        _clear_waiting(context)
        context.user_data["account_step"] = "api_id"
        await query.edit_message_text(
            f"🔐 {b('ПРИВЯЗКА АККАУНТА')}\n\n"
            f"Шаг 1/5 — отправь {b('API ID')} с сайта my.telegram.org.\n\n"
            "⚠️ Не отправляй сюда BOT_TOKEN.\n"
            "❌ /cancel — отменить.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Отмена", callback_data="account:cancel")]]
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "account:refresh":
        try:
            status = await gift_account.account_status()
        except Exception as e:
            await query.edit_message_text(
                f"❌ Не удалось получить баланс:\n<code>{esc(e)}</code>",
                reply_markup=back_kb("account"),
                parse_mode=ParseMode.HTML,
            )
            return

        if not status.get("connected"):
            await query.edit_message_text("🔴 Аккаунт не привязан.", reply_markup=back_kb())
            return

        await query.edit_message_text(
            f"👤 {b('АККАУНТ ВЫДАЧИ')}\n\n"
            f"👤 {esc(status.get('name', 'Аккаунт'))}\n"
            f"⭐ Баланс: {b(status.get('balance', 'ошибка'))} Stars",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Обновить", callback_data="account:refresh")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="account")],
                ]
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "account:disconnect":
        await gift_account.clear_account()
        _clear_waiting(context)
        await query.edit_message_text(
            f"🔓 {b('Аккаунт отвязан.')}",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔐 Привязать новый", callback_data="account:connect")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="main")],
                ]
            ),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "account:cancel":
        await gift_account.abort_login()
        _clear_waiting(context)
        await query.edit_message_text("❌ Привязка отменена.", reply_markup=back_kb("account"))
        return

    # ---------------- ПОДАРКИ ----------------
    if data in ("gifts", "refresh"):
        await show_gifts(query, context)
        return

    # ---------------- БАЛАНС БОТА ----------------
    if data == "balance":
        try:
            balance = await context.bot.get_my_star_balance()
            amount = getattr(balance, "amount", balance)
            await query.edit_message_text(
                f"💰 {b('БАЛАНС БОТА')}\n\n⭐ Stars: {b(amount)}\n\n"
                "Это звёзды, полученные от продаж в /start.",
                reply_markup=back_kb(),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            log.exception("Ошибка получения баланса")
            await query.edit_message_text(
                f"❌ Не удалось получить баланс Stars.\n\n<code>{esc(e)}</code>",
                reply_markup=back_kb(),
                parse_mode=ParseMode.HTML,
            )
        return

    # ---------------- СТАТИСТИКА ----------------
    if data == "stats":
        await query.edit_message_text(
            f"📊 {b('СТАТИСТИКА')}\n\n"
            f"💬 Сообщений: {b(stats.get('messages', 0))}\n"
            f"🎯 Срабатываний: {b(stats.get('wins', 0))}\n"
            f"🎁 Подарков отправлено: {b(stats.get('gifts_sent', 0))}\n"
            f"❌ Ошибок: {b(stats.get('errors', 0))}\n\n"
            f"🎯 Базовый шанс: {b(fmt_num(S['chance']) + '%')}\n"
            f"🍀 Активных бустов: {b(len(chance_boosts))}\n"
            f"🎁 Подарок: {b(S['selected_gift_id'] or 'Авто')}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- ВКЛ/ВЫКЛ ----------------
    if data == "toggle":
        S["giveaway_enabled"] = not S["giveaway_enabled"]
        await _db_set("giveaway_enabled", S["giveaway_enabled"])
        status = "🟢 ВКЛЮЧЕН" if S["giveaway_enabled"] else "🔴 ВЫКЛЮЧЕН"
        await query.edit_message_text(
            f"🎲 {b('Розыгрыш ' + status)}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- ТОП ХАЛЯВЩИКОВ ----------------
    if data == "top":
        await show_top_menu(query)
        return

    if data == "top_toggle":
        S["top_enabled"] = not S["top_enabled"]
        await _db_set("top_enabled", S["top_enabled"])
        await show_top_menu(query)
        return

    if data == "top_header":
        _clear_waiting(context)
        context.user_data["waiting_top_header"] = True
        await query.edit_message_text(
            f"✏️ {b('ЗАГОЛОВОК ТОПА')}\n\n"
            f"Сейчас:\n{esc(S['top_header_text'])}\n\n"
            "Отправь новый текст заголовка (можно с Premium Emoji и "
            "форматированием — жирный, курсив и т.п. сохранятся как есть).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("top"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "top_places":
        await show_top_places_menu(query)
        return

    if data.startswith("top_place:"):
        try:
            place_n = int(data.split(":", 1)[1])
            if not 1 <= place_n <= TOP_PLACES_COUNT:
                raise ValueError
        except ValueError:
            await query.answer("❌ Некорректное место", show_alert=True)
            return
        _clear_waiting(context)
        context.user_data["waiting_top_place"] = place_n
        current = S["top_place_labels"][place_n - 1].get("text", "")
        await query.edit_message_text(
            f"✏️ {b(f'МЕСТО {place_n}')}\n\n"
            f"Сейчас: {esc(current)}\n\n"
            "Отправь новый текст-подпись для этого места (можно с Premium "
            "Emoji и форматированием). Имя игрока и количество подарков "
            "бот допишет сам после этого текста.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("top_places"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "top_preview":
        text, entities = _build_leaderboard_message()
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id, text=text, entities=entities or None,
            )
        except TelegramError:
            log.exception("Не удалось отправить предпросмотр топа")
        return

    if data == "top_send_now":
        if not allowed_chat_ids:
            await show_top_menu(query)
            return
        await post_leaderboard(context.bot)
        await show_top_menu(query)
        return

    if data == "top_reset":
        gift_winners.clear()
        await _save_winners()
        await show_top_menu(query)
        return

    # ---------------- РУЧНАЯ ВЫДАЧА ПОДАРКА ----------------
    if data == "manual_gift":
        await query.edit_message_text(
            f"🎁 {b('РУЧНАЯ ВЫДАЧА')}\n\n"
            f"Подарок: <code>{esc(S['selected_gift_id'] or 'Авто')}</code>\n"
            "Можно выдать подарок любому получателю по @username или ID.\n\n"
            "После успешной выдачи бот отправит настроенное сообщение в текущий чат.\n\n"
            f"Сообщение: {'📷 + текст' if S.get('manual_gift_photo') else '📝 текст'}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎁 Выдать сейчас", callback_data="manual_gift_start")],
                [InlineKeyboardButton("📝 Изменить текст/фото", callback_data="manual_gift_text")],
                [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
            ]), parse_mode=ParseMode.HTML,
        )
        return

    if data == "manual_gift_start":
        _clear_waiting(context)
        context.user_data["waiting_manual_gift_recipient"] = True
        context.user_data["manual_gift_chat_id"] = query.message.chat_id if query.message else None
        await query.edit_message_text(
            f"🎁 {b('ПОЛУЧАТЕЛЬ')}\n\n"
            "Отправь @username, числовой Telegram ID или имя, как человек виден в чате.\n\n"
            f"Текущий подарок: <code>{esc(S['selected_gift_id'] or 'Авто')}</code>\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("manual_gift"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "manual_gift_text":
        _clear_waiting(context)
        context.user_data["waiting_manual_gift_message"] = True
        await query.edit_message_text(
            f"📝 {b('СООБЩЕНИЕ РУЧНОЙ ВЫДАЧИ')}\n\nОтправь текст или фото с подписью. Premium Emoji и форматирование сохранятся.\nПодстановки: <code>{{recipient}}</code> — как ты указал получателя, <code>{{user}}</code> — его имя в чате.\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("manual_gift"), parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- НЕВЫДАННЫЕ ПОДАРКИ ----------------
    if data == "pending":
        await show_pending_menu(query)
        return

    if data == "pending_text":
        _clear_waiting(context)
        context.user_data["waiting_pending_text"] = True
        await query.edit_message_text(
            f"✏️ {b('ТЕКСТ ПРИ РУЧНОЙ ВЫДАЧЕ')}\n\n"
            "Отправляется победителю (в чат, где он выиграл) в тот момент, "
            "когда ты нажимаешь «Выдать» в списке невыданных подарков.\n\n"
            "Отправь новый текст (можно с Premium Emoji и форматированием).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("pending"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("pending_issue:"):
        try:
            pending_id = int(data.split(":", 1)[1])
        except ValueError:
            await show_pending_menu(query)
            return

        item = next((x for x in pending_gifts if int(x.get("id", -1)) == pending_id), None)
        if item is None:
            await show_pending_menu(query, note="ℹ️ Уже обработано.")
            return

        fake_user = _PendingUserProxy(item)
        sent = await _send_gift_to_user(fake_user, context, gift_id=item.get("gift_id"))

        if not sent:
            await show_pending_menu(
                query,
                note=(
                    f"❌ Не удалось отправить подарок #{pending_id} (нет "
                    "username/общего чата, не хватает Stars и т.п.). "
                    "Проверь аккаунт выдачи и попробуй ещё раз."
                ),
            )
            return

        await _pop_pending(pending_id)
        await _bump_winner(fake_user)

        try:
            await context.bot.send_message(
                chat_id=item["chat_id"],
                text=S["pending_issue_text"],
                entities=S["pending_issue_entities"] or None,
                reply_to_message_id=item.get("message_id"),
            )
        except TelegramError:
            log.exception("Не удалось отправить сообщение о ручной выдаче")

        await show_pending_menu(query, note=f"✅ Подарок #{pending_id} выдан.")
        return

    if data.startswith("pending_decline:"):
        try:
            pending_id = int(data.split(":", 1)[1])
        except ValueError:
            await show_pending_menu(query)
            return
        item = await _pop_pending(pending_id)
        if item is None:
            await show_pending_menu(query, note="ℹ️ Уже обработано.")
        else:
            await show_pending_menu(query, note=f"🗑 Заявка #{pending_id} отклонена.")
        return

    # ---------------- КОММЕНТАРИЙ К ПОСТАМ КАНАЛА ----------------
    if data == "comment":
        await show_comment_menu(query)
        return

    if data == "comment_toggle":
        if not S["comment_chat_id"] and not S["comment_enabled"]:
            await query.answer(
                "❌ Сначала выбери чат обсуждения кнопкой ниже.", show_alert=True,
            )
            return
        S["comment_enabled"] = not S["comment_enabled"]
        await _db_set("comment_enabled", S["comment_enabled"])
        await show_comment_menu(query)
        return

    if data == "comment_edit":
        _clear_waiting(context)
        context.user_data["waiting_comment_text"] = True
        await query.edit_message_text(
            f"✏️ {b('ТЕКСТ КОММЕНТАРИЯ')}\n\n"
            "Отправь текст или фото с подписью — это будет первым "
            "комментарием под каждым новым постом (отправится жирным "
            "шрифтом автоматически, форматировать вручную не нужно).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("comment"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("comment_setchat:"):
        try:
            chat_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("❌ Некорректный чат", show_alert=True)
            return
        S["comment_chat_id"] = chat_id
        await _db_set("comment_chat_id", chat_id)
        await show_comment_menu(query)
        return

    # ---------------- АНТИСПАМ ----------------
    # ---------------- ПОДАРКИ ТОЛЬКО УЧАСТНИКАМ ----------------
    if data == "members":
        _clear_waiting(context)
        await show_members_menu(query)
        return

    if data == "members_toggle":
        S["require_member"] = not S["require_member"]
        await _db_set("require_member", S["require_member"])
        await show_members_menu(query)
        return

    if data == "members_msg":
        _clear_waiting(context)
        context.user_data["waiting_nonmember_msg"] = True
        await query.edit_message_text(
            f"📝 {b('ТЕКСТ ДЛЯ НЕ ВСТУПИВШИХ В ЧАТ')}\n\n"
            "Отправь текст или фото с подписью. Premium Emoji и форматирование сохранятся.\n"
            "Подстановка: <code>{user}</code> — упоминание человека. Ссылку на чат можно "
            "вставить прямо в текст.\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("members"), parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- СУМРАК: ТЕКСТЫ ----------------
    if data == "sumrak_menu":
        _clear_waiting(context)
        await show_sumrak_menu(query)
        return

    if data in ("sumrak_close_msg", "sumrak_open_msg"):
        _clear_waiting(context)
        closing = data == "sumrak_close_msg"
        context.user_data["waiting_" + data] = True
        await query.edit_message_text(
            f"📝 {b('ТЕКСТ ПОСЛЕ ' + ('ЗАКРЫТИЯ' if closing else 'ОТКРЫТИЯ') + ' ЧАТА')}\n\n"
            "Отправь текст или фото с подписью. Premium Emoji, жирный шрифт и другое "
            "форматирование сохранятся.\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("sumrak_menu"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "sumrak_reset":
        _clear_waiting(context)
        for pref, default in (("sumrak_close", DEFAULT_SUMRAK_CLOSE_TEXT), ("sumrak_open", DEFAULT_SUMRAK_OPEN_TEXT)):
            S[f"{pref}_text"], S[f"{pref}_photo"], S[f"{pref}_entities"] = default, None, []
            await _db_set(f"{pref}_text", default)
            await _db_set(f"{pref}_photo", "")
            await _db_set_entities(f"{pref}_entities", [])
        await query.answer("Стандартные тексты возвращены")
        await show_sumrak_menu(query)
        return

    # ---------------- КАНАЛЫ И ПОСТЫ ----------------
    if data == "channels":
        _clear_waiting(context)
        await show_channels_menu(query)
        return

    if data == "channels:add":
        if len(post_channels) >= MAX_CHANNELS:
            await query.answer(f"Максимум {MAX_CHANNELS} канала", show_alert=True)
            return
        _clear_waiting(context)
        context.user_data["waiting_channel_add"] = True
        await query.edit_message_text(
            f"➕ {b('ПРИВЯЗАТЬ КАНАЛ')}\n\n"
            "1. Добавь бота в администраторы канала с правом «Публикация сообщений».\n"
            "2. Перешли сюда любой пост из канала — или отправь @username / ID канала.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("channels"), parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("channels:del:"):
        try:
            ch_id = int(data.rsplit(":", 1)[1])
        except ValueError:
            await query.answer("❌ Неверный ID", show_alert=True)
            return
        post_channels[:] = [c for c in post_channels if c["id"] != ch_id]
        await _save_channels()
        await query.answer("Канал отвязан")
        await show_channels_menu(query)
        return

    if data == "post:new":
        if not post_channels:
            await query.answer("Сначала привяжи канал", show_alert=True)
            return
        _clear_waiting(context)
        context.user_data.pop("post_draft", None)
        context.user_data["waiting_post_content"] = True
        await query.edit_message_text(
            f"📝 {b('НОВЫЙ ПОСТ')}\n\n"
            "Отправь сообщение, которое нужно опубликовать: текст, фото, видео, гифка или файл "
            "с подписью. Жирный шрифт, ссылки и Premium Emoji сохранятся как есть. "
            "Потом сможешь добавить кнопку и выбрать канал.\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("channels"), parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("post:"):
        draft = context.user_data.get("post_draft")
        if not draft:
            await query.answer("Черновик потерян, создай пост заново", show_alert=True)
            return
        action = data[5:]

        async def redraw():
            await _refresh_post_preview(context.bot, draft)
            await query.edit_message_text(
                _post_panel_text(draft), reply_markup=_post_panel_kb(draft), parse_mode=ParseMode.HTML,
            )

        if action.startswith("t:"):
            ch_id = int(action[2:])
            if ch_id in draft["targets"]:
                draft["targets"].remove(ch_id)
            else:
                draft["targets"].append(ch_id)
            await redraw()
            return
        if action == "btn":
            _clear_waiting(context)
            context.user_data["waiting_post_btn_text"] = True
            await query.edit_message_text(
                f"🔘 {b('ТЕКСТ КНОПКИ')}\n\n"
                "Отправь текст кнопки.\n\n"
                "• Premium Emoji в тексте станет иконкой перед надписью кнопки.\n"
                "• Жирный шрифт: Telegram не умеет форматирование внутри кнопок, поэтому "
                "жирным станут только латиница и цифры (Unicode-жирные буквы). Кириллицу "
                "сделать жирной в кнопке нельзя.\n"
                "• Цвет выбирается на следующем экране.\n\n❌ /cancel — отменить.",
                parse_mode=ParseMode.HTML,
            )
            return
        if action == "btn_del":
            draft["btn"] = None
            await redraw()
            return
        if action.startswith("c:") and draft.get("btn"):
            style = action[2:]
            draft["btn"]["style"] = None if style == "none" else style
            await redraw()
            return
        if action == "go":
            await _post_publish(query, context)
            return
        if action == "cancel":
            context.user_data.pop("post_draft", None)
            if draft.get("preview_id"):
                try:
                    await context.bot.delete_message(draft["src_chat"], draft["preview_id"])
                except TelegramError:
                    pass
            await query.edit_message_text("❌ Пост отменён.", reply_markup=back_kb("channels"))
            return
        return

    if data == "botadmins":
        _clear_waiting(context)
        await show_botadmins_menu(query)
        return

    if data == "botadmins:add":
        _clear_waiting(context)
        context.user_data["waiting_botadmin_add"] = True
        await query.edit_message_text(
            f"➕ {b('ДОБАВИТЬ АДМИНОВ БОТА')}\n\n"
            "Отправь @username или ID. Можно сразу много — через пробел, запятую или с новой строки:\n"
            "<code>@vasya 123456789 @petya</code>\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("botadmins"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "botadmins:del":
        _clear_waiting(context)
        context.user_data["waiting_botadmin_del"] = True
        await query.edit_message_text(
            f"🗑 {b('УБРАТЬ АДМИНОВ БОТА')}\n\nОтправь @username или ID (можно несколько).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("botadmins"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "userchance":
        _clear_waiting(context)
        await show_userchance_menu(query)
        return

    if data == "userchance:set":
        _clear_waiting(context)
        context.user_data["waiting_userchance_set"] = True
        await query.edit_message_text(
            f"➕ {b('ЛИЧНЫЙ ШАНС')}\n\n"
            "Отправь @username и шанс в процентах через пробел, например:\n"
            "<code>@vasya 25</code>\n<code>@petya 0.5</code>\n<code>@spammer 0</code>\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("userchance"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "userchance:del":
        _clear_waiting(context)
        context.user_data["waiting_userchance_del"] = True
        await query.edit_message_text(
            f"🗑 {b('УБРАТЬ ЛИЧНЫЙ ШАНС')}\n\nОтправь @username (можно несколько через пробел).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("userchance"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "antispam":
        await show_antispam_menu(query)
        return

    if data == "antispam_toggle":
        S["antispam_enabled"] = not S["antispam_enabled"]
        await _db_set("antispam_enabled", S["antispam_enabled"])
        await show_antispam_menu(query)
        return

    if data == "antispam_message":
        _clear_waiting(context)
        context.user_data["waiting_antispam_message"] = True
        await query.edit_message_text(
            f"📝 {b('СООБЩЕНИЕ О МУТЕ')}\n\nОтправь текст или фото с подписью. Premium Emoji и форматирование сохранятся.\nПодстановки: <code>{{user}}</code>, <code>{{minutes}}</code>, <code>{{reason}}</code>, <code>{{limit}}</code>, <code>{{window}}</code>.\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("antispam"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "antispam_reminder_msg":
        _clear_waiting(context)
        context.user_data["waiting_antispam_reminder_msg"] = True
        await query.edit_message_text(
            f"🔔 {b('НАПОМИНАНИЕ О ПРАВИЛЕ')}\n\n"
            "Отправь новый текст (или фото с подписью) — он заменит напоминание целиком. "
            "Premium Emoji, жирный, курсив и другое форматирование сохранятся.\n\n"
            "Подстановки (необязательно):\n"
            "<code>{limit}</code> — лимит сообщений\n"
            "<code>{window}</code> — окно в секундах\n"
            "<code>{minutes}</code> — длительность мута\n"
            "<code>{reason}</code> — причина мута\n\n"
            "Можно писать числа и причину прямо в тексте, без подстановок.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("♻️ Вернуть стандартный текст", callback_data="antispam_reminder_reset")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="antispam")],
            ]),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "antispam_reminder_reset":
        _clear_waiting(context)
        S["antispam_reminder_text"] = DEFAULT_ANTISPAM_REMINDER_TEXT
        S["antispam_reminder_photo"] = None
        S["antispam_reminder_entities"] = _default_reminder_entities()
        await _db_set("antispam_reminder_text", DEFAULT_ANTISPAM_REMINDER_TEXT)
        await _db_set("antispam_reminder_photo", "")
        await _db_set_entities("antispam_reminder_entities", S["antispam_reminder_entities"])
        await show_antispam_menu(query)
        return

    if data == "antispam_reason":
        _clear_waiting(context)
        context.user_data["waiting_antispam_reason"] = True
        await query.edit_message_text(
            f"✏️ {b('ПРИЧИНА МУТА')}\n\nСейчас: {esc(S['antispam_reason'])}\n\nОтправь новую причину.",
            reply_markup=back_kb("antispam"), parse_mode=ParseMode.HTML,
        )
        return

    if data == "antispam_window":
        _clear_waiting(context)
        context.user_data["waiting_antispam_window"] = True
        await query.edit_message_text(
            f"⏱ {b('ОКНО АНТИСПАМА')}\n\nСейчас: {S['antispam_window']} сек.\n\nОтправь количество секунд от 5 до 3600.",
            reply_markup=back_kb("antispam"), parse_mode=ParseMode.HTML,
        )
        return

    if data in ("antispam_limit", "antispam_mute", "antispam_reminder"):
        _clear_waiting(context)
        context.user_data[f"waiting_{data}"] = True
        prompts = {
            "antispam_limit": (
                f"✏️ {b('ЛИМИТ СООБЩЕНИЙ')}\n\n"
                f"Сейчас: {b(S['antispam_limit'])} сообщений за "
                f"{int(S['antispam_window'])} сек.\n\n"
                "Отправь новое число (целое, например <code>10</code>)."
            ),
            "antispam_mute": (
                f"✏️ {b('ДЛИТЕЛЬНОСТЬ МУТА')}\n\n"
                f"Сейчас: {b(S['antispam_mute_minutes'])} мин.\n\n"
                "Отправь новое количество минут (целое число)."
            ),
            "antispam_reminder": (
                f"✏️ {b('НАПОМИНАНИЕ О ПРАВИЛЕ')}\n\n"
                f"Сейчас: раз в {b(S['antispam_reminder_every'])} сообщений чата.\n\n"
                "Отправь новое число (0 — выключить напоминания)."
            ),
        }
        await query.edit_message_text(
            prompts[data] + "\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("antispam"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- ЦЕНЫ И ПАРАМЕТРЫ ----------------
    if data == "prices":
        await show_prices_menu(query)
        return

    if data in (
        "price_close_chat", "price_boost", "close_minutes",
        "boost_multiplier", "boost_hours",
    ):
        _clear_waiting(context)
        context.user_data[f"waiting_{data}"] = True
        prompts = {
            "price_close_chat": (
                f"🔒 {b('ЦЕНА ЗАКРЫТИЯ ЧАТА')}\n\n"
                f"Сейчас: {b(S['price_close_chat'])} ⭐\n\n"
                "Отправь новую цену в Stars (целое число, например <code>300</code>)."
            ),
            "price_boost": (
                f"🍀 {b('ЦЕНА ПОВЫШЕННОГО ШАНСА')}\n\n"
                f"Сейчас: {b(S['price_boost'])} ⭐\n\n"
                "Отправь новую цену в Stars (целое число, например <code>1000</code>)."
            ),
            "close_minutes": (
                f"⏳ {b('ДЛИТЕЛЬНОСТЬ ЗАКРЫТИЯ ЧАТА')}\n\n"
                f"Сейчас: {b(fmt_num(S['close_minutes']))} мин\n\n"
                "Отправь новое количество минут (целое число)."
            ),
            "boost_multiplier": (
                f"✖️ {b('МНОЖИТЕЛЬ ШАНСА (БУСТ)')}\n\n"
                f"Сейчас: ×{b(fmt_num(S['boost_multiplier']))}\n\n"
                "Отправь новый множитель, например <code>5</code> или <code>3.5</code>."
            ),
            "boost_hours": (
                f"🕐 {b('ДЛИТЕЛЬНОСТЬ БУСТА')}\n\n"
                f"Сейчас: {b(fmt_num(S['boost_hours']))} ч\n\n"
                "Отправь новое количество часов, например <code>24</code>."
            ),
        }
        await query.edit_message_text(
            prompts[data] + "\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("prices"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- БЛОКИРОВКА ВЫДАЧ ----------------
    if data == "blocked":
        await show_blocked_menu(query)
        return

    if data == "blocked:add":
        _clear_waiting(context)
        context.user_data["waiting_blocked_add"] = True
        await query.edit_message_text(
            f"🚫 {b('ДОБАВЛЕНИЕ В БЛОКИРОВКУ')}\n\n"
            "Отправь username пользователя — с @ или без @.\n"
            "Можно отправить сразу несколько через пробел, запятую или с новой строки.\n\n"
            "Пример: <code>@user1 @user2</code>\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("blocked"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "blocked:remove":
        _clear_waiting(context)
        context.user_data["waiting_blocked_remove"] = True
        await query.edit_message_text(
            f"🟢 {b('СНЯТИЕ БАНА')}\n\n"
            "Отправь username пользователя, которого нужно разблокировать.\n"
            "Можно несколько сразу.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("blocked"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "blocked:list":
        await show_blocked_list(query)
        return

    if data == "blocked:clear":
        blocked_usernames.clear()
        await db.save_blocked_usernames(blocked_usernames)
        await query.edit_message_text(
            f"✅ {b('Список блокировки очищен.')}\n\n"
            "Все пользователи снова могут получать подарки.",
            reply_markup=back_kb("blocked"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- ДОСТУПНЫЕ ЧАТЫ ----------------
    if data == "access":
        await show_access_menu(query)
        return

    if data == "access_add_current":
        chat = query.message.chat if query.message else None
        if not chat or chat.type not in ("group", "supergroup"):
            await query.answer(
                "Открой /admin прямо в нужной группе, чтобы добавить её.", show_alert=True
            )
            return
        if chat.id not in allowed_chat_ids and len(allowed_chat_ids) >= MAX_ALLOWED_CHATS:
            await query.answer("❌ Уже добавлены 2 чата.", show_alert=True)
            return
        allowed_chat_ids.add(chat.id)
        await db.add_allowed_chat(chat.id)
        await query.answer("✅ Чат добавлен")
        await show_access_menu(query)
        return

    if data == "access_add_username":
        _clear_waiting(context)
        context.user_data["waiting_access_chat"] = True
        await query.edit_message_text(
            f"➕ {b('ДОБАВЛЕНИЕ ЧАТА')}\n\n"
            "Отправь @username группы или её chat ID.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("access"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("access_remove:"):
        try:
            chat_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("❌ Неверный chat ID", show_alert=True)
            return
        allowed_chat_ids.discard(chat_id)
        await db.remove_allowed_chat(chat_id)
        await query.answer("🗑 Чат удалён")
        await show_access_menu(query)
        return

    if data == "access_clear":
        allowed_chat_ids.clear()
        await db.clear_allowed_chats()
        await query.answer("🧹 Список очищен")
        await show_access_menu(query)
        return

    if data.startswith("admin_close:") or data.startswith("admin_open:"):
        try:
            chat_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("❌ Неверный chat ID", show_alert=True)
            return

        closing = data.startswith("admin_close:")
        try:
            # Без ограничения по времени — снимается вручную кнопкой «Открыть».
            await _set_chat_closed(context.bot, chat_id, closing, announce=True)
        except TelegramError as e:
            await query.answer(f"❌ Не удалось: {e}", show_alert=True)
            return

        await query.answer("🔒 Чат закрыт" if closing else "🔓 Чат открыт")
        await show_access_menu(query)
        return

    # ---------------- ЛУДКА ----------------
    if data == "ludka":
        await show_ludka_menu(query)
        return

    if data in ("ludka_price", "ludka_prize", "ludka_message"):
        _clear_waiting(context)
        context.user_data[f"waiting_{data}"] = True
        prompts = {
            "ludka_price": (
                f"💰 {b('ЦЕНА ЛУДКИ 777')}\n\n"
                "Сколько сообщений нужно для одного вращения?\n"
                "Например: <code>1</code>, <code>5</code>, <code>10</code>"
            ),
            "ludka_prize": (
                f"🎁 {b('ПРИЗ ЛУДКИ 777')}\n\n"
                "Отправь текст приза. Работает жирный шрифт, Premium Emoji "
                "и <code>**звёздочки**</code>."
            ),
            "ludka_message": (
                f"📝 {b('СООБЩЕНИЕ ЛУДКИ 777')}\n\n"
                "Отправь текст или фото с подписью. Форматирование сохраняется."
            ),
        }
        await query.edit_message_text(
            prompts[data] + "\n\n❌ /cancel — отменить.",
            reply_markup=back_kb("ludka"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "ludka_launch":
        await launch_ludka(query, context)
        return

    if data == "ludka_stop":
        await stop_ludka(query)
        return

    # ---------------- УГАДАЙ ЧИСЛО ----------------
    if data == "guess":
        await show_guess_menu(query)
        return

    if data == "guess_name":
        _clear_waiting(context)
        context.user_data["waiting_guess_name"] = True
        await query.edit_message_text(
            f"✏️ {b('НАЗВАНИЕ ИВЕНТА')}\n\n"
            "Отправь текст или фото с подписью — это будет объявление ивента.\n"
            "Диапазон чисел бот допишет к этому тексту сам.\n\n"
            f"{b('Форматирование:')} жирный, курсив, Premium Emoji сохраняются "
            "как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("guess"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data in ("guess_min", "guess_max"):
        _clear_waiting(context)
        context.user_data[f"waiting_{data}"] = True
        label = "МИНИМАЛЬНОЕ" if data == "guess_min" else "МАКСИМАЛЬНОЕ"
        await query.edit_message_text(
            f"🔢 {b(label + ' ЧИСЛО ДИАПАЗОНА')}\n\n"
            f"Сейчас: {b(S[data])}\n\n"
            "Отправь целое число.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("guess"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "guess_prize":
        _clear_waiting(context)
        context.user_data["waiting_guess_prize"] = True
        await query.edit_message_text(
            f"🎁 {b('ПРИЗ ПОБЕДИТЕЛЮ')}\n\n"
            "Отправляется тому, кто первым угадает число — в ответ на его "
            "сообщение.\n\n"
            "Отправь одно из:\n📝 текст\n📷 фото с подписью\n\n"
            f"{b('Форматирование:')} жирный, курсив, Premium Emoji сохраняются "
            "как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("guess"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "guess_launch":
        await launch_guess_event(query, context)
        return

    if data == "guess_stop":
        await stop_guess_event(query, context)
        return

    if data == "guess_secret":
        _clear_waiting(context)
        context.user_data["waiting_guess_secret"] = True
        await query.edit_message_text(
            f"🎯 {b('СВОЁ ЧИСЛО')}\n\n"
            "Отправь число, которое хочешь загадать сам (можно любое, даже "
            "вне текущего диапазона «От/До» — диапазон подстроится "
            "автоматически). Например: <code>777</code>.\n\n"
            "Ивент запустится сразу с этим числом.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("guess"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "guess_gift":
        _clear_waiting(context)
        context.user_data["waiting_guess_gift_id"] = True
        current = S["guess_gift_id"] or "не задан (используется общий выбор подарка)"
        await query.edit_message_text(
            f"🎁 {b('ID ПРИЗА ДЛЯ ПОБЕДИТЕЛЯ')}\n\n"
            f"Сейчас: <code>{esc(current)}</code>\n\n"
            "Отправь ID настоящего Telegram-подарка (посмотреть ID можно в "
            "разделе «🎁 Подарки» главного меню — он показывается после "
            "выбора). Этот подарок автоматически уйдёт победителю ивента "
            "«Угадай число».\n\n"
            "Отправь <code>off</code>, чтобы использовать общий выбор "
            "подарка вместо отдельного.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("guess"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("guess_setchat:"):
        try:
            chat_id = int(data.split(":", 1)[1])
        except ValueError:
            await query.answer("❌ Некорректный чат.", show_alert=True)
            return
        S["guess_chat_id"] = chat_id
        await _db_set("guess_chat_id", chat_id)
        await show_guess_menu(query)
        return

    # ---------------- /START: ТЕКСТ И КНОПКИ ----------------
    if data == "startedit":
        await show_startedit_menu(query)
        return

    if data == "start_text":
        _clear_waiting(context)
        context.user_data["waiting_start_text"] = True
        await query.edit_message_text(
            f"📝 {b('ТЕКСТ /START')}\n\n"
            "Отправь текст или фото с подписью — это будет вступительный "
            "текст команды /start. Цены и шанс бот допишет сам.\n\n"
            f"{b('Форматирование:')} жирный, курсив, Premium Emoji сохраняются "
            "как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("startedit"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data in ("btn_close", "btn_boost", "btn_my", "btn_help"):
        _clear_waiting(context)
        context.user_data[f"waiting_{data}"] = True
        key = f"{data}_label"
        names = {
            "btn_close": "«Закрыть чат»",
            "btn_boost": "«Повысить шанс»",
            "btn_my": "«Мои покупки»",
            "btn_help": "«Как это работает»",
        }
        await query.edit_message_text(
            f"🔘 {b('КНОПКА ' + names[data])}\n\n"
            f"Сейчас: {esc(S[key])}\n\n"
            "Отправь новый текст кнопки (до 60 символов).\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("startedit"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- СООБЩЕНИЯ О ПОДАРКЕ ----------------
    if data == "winmessage":
        await show_winmessage_menu(query)
        return

    if data == "win_success_message":
        _clear_waiting(context)
        context.user_data["waiting_win_success_message"] = True
        await query.edit_message_text(
            f"🎉 {b('ТЕКСТ ПРИ УДАЧНОЙ ВЫДАЧЕ')}\n\n"
            "Отправляется в чат, когда бот успешно купил и выдал "
            "победителю настоящий Telegram-подарок.\n\n"
            "Отправь одно из:\n"
            "📝 текст\n"
            "📷 фото с подписью\n\n"
            f"{b('Форматирование:')} жирный шрифт, курсив и Premium Emoji "
            "сохраняются как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("winmessage"),
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "win_fail_message":
        _clear_waiting(context)
        context.user_data["waiting_win_message"] = True
        await query.edit_message_text(
            f"⚠️ {b('ЗАПАСНОЙ ТЕКСТ (ЕСЛИ ВЫДАЧА НЕ УДАЛАСЬ)')}\n\n"
            "Отправляется, если подарок выпал, но бот не смог "
            "автоматически купить и отправить его (нет привязанного "
            "аккаунта, не хватает Stars и т.п.).\n\n"
            "Отправь одно из:\n"
            "📝 текст\n"
            "📷 фото с подписью\n\n"
            f"{b('Форматирование:')} жирный шрифт, курсив и Premium Emoji "
            "сохраняются как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("winmessage"),
            parse_mode=ParseMode.HTML,
        )
        return

    # ---------------- СООБЩЕНИЕ О БЛОКИРОВКЕ ----------------
    if data == "blocked_message":
        _clear_waiting(context)
        context.user_data["waiting_blocked_message"] = True
        await query.edit_message_text(
            f"✏️ {b('СООБЩЕНИЕ О БЛОКИРОВКЕ')}\n\n"
            "Отправляется заблокированному пользователю вместо подарка.\n\n"
            "Отправь одно из:\n"
            "📝 текст\n"
            "📷 фото с подписью\n\n"
            f"{b('Форматирование:')} жирный шрифт, курсив и Premium Emoji "
            "сохраняются как есть. Можно также писать <code>**жирный**</code>.\n\n"
            "❌ /cancel — отменить.",
            reply_markup=back_kb("blocked"),
            parse_mode=ParseMode.HTML,
        )
        return


def _clear_waiting(context) -> None:
    for key in list(context.user_data.keys()):
        if key.startswith(("waiting_", "account_")):
            context.user_data.pop(key, None)


# =========================================================
# БЛОКИРОВКА ВЫДАЧ — МЕНЮ
# =========================================================

def blocked_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Заблокировать username", callback_data="blocked:add")],
        [InlineKeyboardButton("🟢 Снять бан", callback_data="blocked:remove")],
        [InlineKeyboardButton("📋 Список заблокированных", callback_data="blocked:list")],
        [InlineKeyboardButton("✏️ Сообщение о блокировке", callback_data="blocked_message")],
        [InlineKeyboardButton("🗑 Снять все баны", callback_data="blocked:clear")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_blocked_menu(query) -> None:
    await query.edit_message_text(
        f"🚫 {b('БЛОКИРОВКА ВЫДАЧ')}\n\n"
        f"Заблокировано username: {b(len(blocked_usernames))}\n\n"
        "Заблокированный пользователь при выпадении подарка "
        "получит сообщение о блокировке (текст и/или фото — настраивается), "
        "а подарок ему не отправится.\n\n"
        "Количество username не ограничено.",
        reply_markup=blocked_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# СООБЩЕНИЯ О ПОДАРКЕ — МЕНЮ
# =========================================================

def winmessage_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎉 Текст удачной выдачи", callback_data="win_success_message")],
        [InlineKeyboardButton("⚠️ Запасной текст (выдача не удалась)", callback_data="win_fail_message")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_winmessage_menu(query) -> None:
    await query.edit_message_text(
        f"✏️ {b('СООБЩЕНИЯ О ПОДАРКЕ')}\n\n"
        "Когда в чате выпадает подарок, бот отправляет один из двух текстов:\n\n"
        f"🎉 {b('Удачная выдача')} — подарок куплен и отправлен победителю автоматически.\n"
        f"⚠️ {b('Запасной текст')} — подарок выпал, но автоматическая отправка "
        "не удалась (нет привязанного аккаунта, не хватает Stars и т.п.).\n\n"
        "Выбери, что редактировать:",
        reply_markup=winmessage_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# ТОП ХАЛЯВЩИКОВ — МЕНЮ
# =========================================================

def top_menu_keyboard() -> InlineKeyboardMarkup:
    toggle_text = "⛔ Выключить" if S["top_enabled"] else "🚀 Включить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_text, callback_data="top_toggle")],
        [InlineKeyboardButton("✏️ Заголовок", callback_data="top_header")],
        [InlineKeyboardButton("🥇 Места 1–10", callback_data="top_places")],
        [InlineKeyboardButton("👁 Предпросмотр", callback_data="top_preview")],
        [InlineKeyboardButton("📨 Отправить сейчас", callback_data="top_send_now")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_top_menu(query) -> None:
    status = "🟢 ВКЛЮЧЕН (постится каждые 15 минут)" if S["top_enabled"] else "🔴 ВЫКЛЮЧЕН"
    preview = esc(S["top_header_text"])[:250] or "—"
    await query.edit_message_text(
        f"🏆 {b('ТОП ХАЛЯВЩИКОВ')}\n\n"
        f"Статус: {status}\n"
        f"Чатов для рассылки: {b(len(allowed_chat_ids))}\n"
        f"Игроков в топе: {b(len(_top_winners(1000)))}\n\n"
        f"Заголовок сейчас:\n{preview}\n\n"
        "В топ попадают все, кто реально получил настоящий Telegram-подарок "
        "(из обычного розыгрыша, «Угадай число» и ручной выдачи). "
        "Показываются лучшие 10 по количеству подарков.\n\n"
        "«Места 1–10» — это подписи перед именем и количеством подарков "
        "(туда же можно вставить Premium Emoji), а @username игрока и счётчик "
        "бот подставляет сам.",
        reply_markup=top_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


def top_places_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, TOP_PLACES_COUNT, 2):
        row = [InlineKeyboardButton(f"Место {i + 1}", callback_data=f"top_place:{i + 1}")]
        if i + 1 < TOP_PLACES_COUNT:
            row.append(InlineKeyboardButton(f"Место {i + 2}", callback_data=f"top_place:{i + 2}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="top")])
    return InlineKeyboardMarkup(rows)


async def show_top_places_menu(query) -> None:
    labels = S["top_place_labels"]
    preview_lines = "\n".join(
        f"{i + 1}. {esc(item.get('text', ''))}" for i, item in enumerate(labels)
    )
    await query.edit_message_text(
        f"🥇 {b('МЕСТА В ТОПЕ')}\n\n"
        f"Текущие подписи:\n{preview_lines}\n\n"
        "Выбери место, чтобы изменить его текст — можно вставлять "
        "Premium Emoji и форматирование. После твоего текста бот сам "
        "допишет имя игрока и количество подарков.",
        reply_markup=top_places_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# НЕВЫДАННЫЕ ПОДАРКИ — МЕНЮ
# =========================================================

def pending_menu_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for item in pending_gifts[:15]:
        pid = item.get("id")
        rows.append([
            InlineKeyboardButton(f"✅ Выдать #{pid}", callback_data=f"pending_issue:{pid}"),
            InlineKeyboardButton(f"❌ Отказать #{pid}", callback_data=f"pending_decline:{pid}"),
        ])
    rows.append([InlineKeyboardButton("✏️ Текст при ручной выдаче", callback_data="pending_text")])
    rows.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(rows)


async def show_pending_menu(query, note: str = "") -> None:
    if not pending_gifts:
        body = "Пусто — все подарки выданы автоматически. ✅"
    else:
        lines = []
        for item in pending_gifts[:15]:
            uname = f"@{item['username']}" if item.get("username") else f"id {item.get('user_id')}"
            lines.append(f"#{item.get('id')} — {esc(item.get('name', ''))} ({esc(uname)})")
        body = "\n".join(lines)
        if len(pending_gifts) > 15:
            body += f"\n\n…и ещё {len(pending_gifts) - 15}."

    note_line = f"{note}\n\n" if note else ""
    await query.edit_message_text(
        f"🎁 {b('НЕВЫДАННЫЕ ПОДАРКИ')}\n\n{note_line}{body}\n\n"
        "Победитель, которому не удалось автоматически отправить подарок "
        "(например, не хватило Stars или не привязан аккаунт), попадает "
        "сюда. «Выдать» — бот заново попробует отправить настоящий "
        "Telegram-подарок и напишет победителю заданный текст. "
        "«Отказать» — просто убрать из списка без отправки.",
        reply_markup=pending_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# КОММЕНТАРИЙ К ПОСТАМ КАНАЛА — МЕНЮ
# =========================================================

def comment_menu_keyboard() -> InlineKeyboardMarkup:
    toggle_text = "⛔ Выключить" if S["comment_enabled"] else "🚀 Включить"
    rows = [
        [InlineKeyboardButton(toggle_text, callback_data="comment_toggle")],
        [InlineKeyboardButton("✏️ Текст комментария (текст/фото)", callback_data="comment_edit")],
    ]
    for chat_id in list(allowed_chat_ids)[:MAX_ALLOWED_CHATS]:
        mark = "✅ " if S["comment_chat_id"] == chat_id else ""
        rows.append([InlineKeyboardButton(
            f"{mark}💬 Чат обсуждения: {chat_id}", callback_data=f"comment_setchat:{chat_id}",
        )])
    rows.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(rows)


async def show_comment_menu(query) -> None:
    status = "🟢 ВКЛЮЧЕН" if S["comment_enabled"] else "🔴 ВЫКЛЮЧЕН"
    chat_line = (
        f"\n💬 Чат обсуждения: <code>{S['comment_chat_id']}</code>"
        if S["comment_chat_id"] else
        "\n💬 Чат обсуждения: ❌ не выбран (сначала добавь группу обсуждения "
        "канала в «🔐 Доступные чаты» — открой /admin прямо в ней, — потом "
        "выбери её кнопкой ниже)"
    )
    preview = esc(S["comment_text"])[:300] or "—"
    await query.edit_message_text(
        f"💬 {b('КОММЕНТАРИЙ К ПОСТАМ')}\n\n"
        f"Статус: {status}{chat_line}\n\n"
        f"Текст сейчас: {preview}\n\n"
        "Когда в привязанном канале выходит новый пост и Telegram "
        "автоматически пересылает его в группу обсуждения, бот один раз "
        "отвечает на него этим текстом жирным шрифтом (и фото, если "
        "задано) — как первый комментарий под постом.\n\n"
        "⚠️ Группа обсуждения должна быть привязана к каналу в настройках "
        "самого Telegram-канала, а бот — добавлен в неё администратором.",
        reply_markup=comment_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# АНТИСПАМ — МЕНЮ
# =========================================================

def antispam_menu_keyboard() -> InlineKeyboardMarkup:
    toggle_text = "⛔ Выключить" if S["antispam_enabled"] else "🚀 Включить"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_text, callback_data="antispam_toggle")],
        [InlineKeyboardButton(
            f"✏️ Лимит сообщений: {S['antispam_limit']}/мин", callback_data="antispam_limit",
        )],
        [InlineKeyboardButton(
            f"✏️ Длительность мута: {S['antispam_mute_minutes']} мин", callback_data="antispam_mute",
        )],
        [InlineKeyboardButton(
            f"✏️ Напоминание раз в: {S['antispam_reminder_every']} сообщ.",
            callback_data="antispam_reminder",
        )],
        [InlineKeyboardButton(f"⏱ Окно: {S['antispam_window']} сек", callback_data="antispam_window")],
        [InlineKeyboardButton("📝 Сообщение о муте (текст/фото/эмодзи)", callback_data="antispam_message")],
        [InlineKeyboardButton("🔔 Напоминание о правиле (текст/фото/эмодзи)", callback_data="antispam_reminder_msg")],
        [InlineKeyboardButton(f"✏️ Причина мута: {S['antispam_reason'][:30]}", callback_data="antispam_reason")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_antispam_menu(query) -> None:
    status = "🟢 ВКЛЮЧЕН" if S["antispam_enabled"] else "🔴 ВЫКЛЮЧЕН"
    await query.edit_message_text(
        f"🛡 {b('АНТИСПАМ')}\n\n"
        f"Статус: {status}\n\n"
        f"Если участник отправляет больше {b(S['antispam_limit'])} сообщений "
        f"за {b(int(S['antispam_window']))} сек — бот мутит его в чате на "
        f"{b(S['antispam_mute_minutes'])} мин с причиной «{esc(S['antispam_reason'])}».\n\n"
        f"Каждые {b(S['antispam_reminder_every'])} сообщений в чате бот "
        "также напоминает об этом правиле (0 — не напоминать). Текст напоминания "
        "редактируется целиком — кнопка «🔔 Напоминание».\n\n"
        f"Снять мут раньше срока: ответь на сообщение участника прямо "
        f"в группе текстом {b('«снять спам»')}, либо напиши "
        f"«снять спам @username» или «снять спам ID».",
        reply_markup=antispam_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# /START — МЕНЮ РЕДАКТИРОВАНИЯ ТЕКСТА И КНОПОК
# =========================================================

def startedit_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Текст и фото /start", callback_data="start_text")],
        [InlineKeyboardButton(f"🔘 {S['btn_close_label']}", callback_data="btn_close")],
        [InlineKeyboardButton(f"🔘 {S['btn_boost_label']}", callback_data="btn_boost")],
        [InlineKeyboardButton(f"🔘 {S['btn_my_label']}", callback_data="btn_my")],
        [InlineKeyboardButton(f"🔘 {S['btn_help_label']}", callback_data="btn_help")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_startedit_menu(query) -> None:
    await query.edit_message_text(
        f"🏠 {b('НАСТРОЙКА /START')}\n\n"
        "Здесь можно поменять вступительный текст (+ фото) команды /start "
        "и подписи всех 4 кнопок магазина.\n\n"
        f"{b('Про Premium Emoji:')} они сохраняются как есть в тексте и "
        "подписях к фото — просто вставь их в сообщение как обычно. "
        "В самих кнопках Telegram технически не поддерживает анимированные "
        "Premium Emoji (ограничение платформы) — в подписи кнопки можно "
        "вставить обычный текст/эмодзи.\n\n"
        "Цены и проценты бот больше не дописывает сам — в /start уходит "
        "ровно тот текст (и фото), который ты задашь здесь.",
        reply_markup=startedit_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def show_blocked_list(query) -> None:
    if not blocked_usernames:
        text = f"📋 {b('СПИСОК ЗАБЛОКИРОВАННЫХ')}\n\nСписок пуст."
    else:
        names = sorted(blocked_usernames)
        lines = [f"📋 {b('ЗАБЛОКИРОВАННЫЕ USERNAME')}", "", f"Всего: {b(len(names))}", ""]
        # Telegram limit — показываем столько, сколько помещается.
        for i, name in enumerate(names, 1):
            line = f"{i}. @{esc(name)}"
            if sum(len(x) + 1 for x in lines) + len(line) > 3900:
                lines.append(f"… и ещё {len(names) - i + 1} username")
                break
            lines.append(line)
        text = "\n".join(lines)

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Снять бан", callback_data="blocked:remove")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="blocked")],
        ]),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# АДМИНЫ БОТА
# =========================================================

def botadmins_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить", callback_data="botadmins:add")],
        [InlineKeyboardButton("🗑 Убрать", callback_data="botadmins:del")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


def _botadmins_text(note: str = "") -> str:
    lines = [f"👮 {b('АДМИНЫ БОТА')}", ""]
    if note:
        lines += [note, ""]
    lines.append(f"Владелец: <code>{ADMIN_ID}</code>")
    lines.append("")
    if not bot_admin_ids and not bot_admin_usernames:
        lines.append("Других админов пока нет.")
    else:
        for uid, label in sorted(bot_admin_ids.items()):
            extra = f" — {esc(label)}" if label else ""
            line = f"• <code>{uid}</code>{extra}"
            if sum(len(x) + 1 for x in lines) + len(line) > 3400:
                lines.append("… список обрезан")
                break
            lines.append(line)
        for name in sorted(bot_admin_usernames):
            line = f"• @{esc(name)} ⏳ (привяжется к ID при первом сообщении)"
            if sum(len(x) + 1 for x in lines) + len(line) > 3600:
                lines.append("… список обрезан")
                break
            lines.append(line)
    lines += [
        "",
        f"Админы бота могут открывать и закрывать привязанный чат командами "
        f"{b(SUMRAK_ON_WORD)} / {b(SUMRAK_OFF_WORD)} (в т.ч. лично: "
        f"{b(SUMRAK_OFF_WORD + ' @user')}) и не попадают под антиспам. "
        "Админ-панель остаётся только у владельца.",
        "",
        "Добавлять можно без ограничения: @username или ID, несколько сразу — через пробел, запятую или с новой строки.",
        "Команды: <code>/botadmin @user 123456</code>, <code>/botadminoff @user</code>, <code>/botadmins</code>",
    ]
    return "\n".join(lines)


async def show_botadmins_menu(query, note: str = "") -> None:
    await query.edit_message_text(
        _botadmins_text(note), reply_markup=botadmins_menu_keyboard(), parse_mode=ParseMode.HTML,
    )


def _split_tokens(text: str) -> List[str]:
    return [t for t in re.split(r"[\s,;]+", (text or "").strip()) if t]


async def _add_bot_admin(token: str, bot) -> str:
    v = _clean_recipient_input(token)
    if v.lstrip("@").isdigit():
        uid = int(v.lstrip("@"))
        if uid == ADMIN_ID:
            return f"ℹ️ <code>{uid}</code> — это владелец, он и так админ."
        if uid in bot_admin_ids:
            return f"ℹ️ <code>{uid}</code> уже админ."
        d = known_users.get(uid, {})
        bot_admin_ids[uid] = d.get("u") and f"@{d['u']}" or d.get("n") or ""
        return f"✅ <code>{uid}</code> добавлен."

    name = _normalize_username(v)
    if not name or not _is_latin_username(name):
        return f"❌ «{esc(token)}» — не похоже на @username или ID."
    if name in bot_admin_usernames or any(
        (lbl or "").lstrip("@").lower() == name for lbl in bot_admin_ids.values()
    ):
        return f"ℹ️ @{esc(name)} уже админ."

    uid = next((u for u, d in known_users.items() if d.get("u") == name), None)
    if uid is None:
        try:
            chat = await bot.get_chat(f"@{name}")
            if getattr(chat, "type", "") == "private":
                uid = chat.id
        except TelegramError:
            uid = None

    if uid is not None:
        if uid == ADMIN_ID:
            return f"ℹ️ @{esc(name)} — это владелец, он и так админ."
        bot_admin_ids[uid] = f"@{name}"
        return f"✅ @{esc(name)} добавлен (ID <code>{uid}</code>)."

    bot_admin_usernames.add(name)
    return (f"✅ @{esc(name)} добавлен по username. Как только он напишет боту или в "
            "привязанный чат, бот привяжет его к ID.")


def _remove_bot_admin(token: str) -> str:
    v = _clean_recipient_input(token)
    if v.lstrip("@").isdigit():
        uid = int(v.lstrip("@"))
        if uid == ADMIN_ID:
            return "❌ Владельца убрать нельзя."
        if bot_admin_ids.pop(uid, None) is None:
            return f"ℹ️ <code>{uid}</code> не в списке админов."
        return f"✅ <code>{uid}</code> убран."

    name = _normalize_username(v)
    if not name:
        return f"❌ «{esc(token)}» — не похоже на @username или ID."
    removed = name in bot_admin_usernames
    bot_admin_usernames.discard(name)
    for uid in [u for u, lbl in bot_admin_ids.items()
                if (lbl or "").lstrip("@").lower() == name or known_users.get(u, {}).get("u") == name]:
        bot_admin_ids.pop(uid, None)
        removed = True
    return f"✅ @{esc(name)} убран." if removed else f"ℹ️ @{esc(name)} не в списке админов."


async def _botadmins_apply(text: str, bot, remove: bool) -> str:
    tokens = _split_tokens(text)
    if not tokens:
        return "❌ Отправь @username или ID."
    results = []
    for t in tokens:
        results.append(_remove_bot_admin(t) if remove else await _add_bot_admin(t, bot))
    await _save_bot_admins()
    return "\n".join(results)


async def botadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text(_botadmins_text(), parse_mode=ParseMode.HTML)
        return
    text = await _botadmins_apply(" ".join(context.args), context.bot, remove=False)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def botadminoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Укажи, кого убрать: <code>/botadminoff @user 123456</code>",
                                        parse_mode=ParseMode.HTML)
        return
    text = await _botadmins_apply(" ".join(context.args), context.bot, remove=True)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def botadmins_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    await update.message.reply_text(_botadmins_text(), parse_mode=ParseMode.HTML)


# =========================================================
# !СУМРАК / !СУМРАКОФФ — ОТКРЫТЬ/ЗАКРЫТЬ ПРИВЯЗАННЫЙ ЧАТ
# =========================================================

# Какое слово что делает. Поменять местами — достаточно переключить флаг.
SUMRAK_ON_WORD = "!сумрак"
SUMRAK_OFF_WORD = "!сумракофф"
SUMRAK_CLOSES_CHAT = True   # True: !сумрак закрывает чат, !сумракофф открывает

_SUMRAK_RE = re.compile(r"^!\s*сумрак(?:\s*(офф|off))?(?:\s+(\S+))?\s*[.!]*$", re.IGNORECASE)


def _full_perms() -> ChatPermissions:
    """Все права True — снимает персональные ограничения/исключения с человека."""
    try:
        return ChatPermissions.all_permissions()
    except AttributeError:
        return ChatPermissions(
            can_send_messages=True, can_send_polls=True, can_send_other_messages=True,
            can_add_web_page_previews=True, can_change_info=True, can_invite_users=True,
            can_pin_messages=True,
        )


async def _send_saved_message(
    bot, chat_id: int, prefix: str, default_text: str, reply_to: Optional[int] = None,
    values: Optional[Dict[str, str]] = None, mention_user=None,
) -> None:
    """Отправляет редактируемое сообщение из настроек (текст/фото/Premium Emoji)."""
    text = str(S.get(f"{prefix}_text") or default_text)
    ents = S.get(f"{prefix}_entities")
    if values is not None or mention_user is not None:
        text, ents = render_template(text, ents, values or {}, mention_user=mention_user)
    photo = S.get(f"{prefix}_photo")
    try:
        if photo:
            await bot.send_photo(chat_id=chat_id, photo=photo, caption=text,
                                 caption_entities=ents or None, reply_to_message_id=reply_to)
        else:
            await bot.send_message(chat_id=chat_id, text=text, entities=ents or None,
                                   reply_to_message_id=reply_to)
    except TelegramError:
        log.exception("Не удалось отправить сообщение «%s» в чат %s", prefix, chat_id)


def _guests_key(chat_id: int) -> str:
    return f"sumrak_guests:{chat_id}"


async def _load_guests(chat_id: int) -> set:
    raw = await db.get_setting(_guests_key(chat_id), "[]")
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return {int(x) for x in data}
    except Exception:
        return set()


async def _save_guests(chat_id: int, guests: set) -> None:
    await db.set_setting(_guests_key(chat_id), json.dumps(sorted(guests)))


async def _chat_is_closed(bot, chat_id: int) -> bool:
    try:
        chat = await bot.get_chat(chat_id)
        perms = getattr(chat, "permissions", None)
        return bool(perms and perms.can_send_messages is False)
    except TelegramError:
        return False


async def _set_chat_closed(bot, chat_id: int, close: bool, announce: bool = False,
                           reply_to: Optional[int] = None) -> None:
    """Закрывает/открывает чат без ограничения по времени.
    При закрытии админы бота и «гости» (кому открыли чат лично) получают
    исключение и продолжают писать. При открытии исключения снимаются."""
    await bot.set_chat_permissions(chat_id, CLOSED_PERMS if close else OPEN_PERMS)
    # Ручное управление отменяет отложенное открытие после покупки.
    await db.set_setting(f"closed_until:{chat_id}", "")

    guests = await _load_guests(chat_id)
    people = {ADMIN_ID, *bot_admin_ids.keys()} | guests
    for uid in people:
        try:
            await bot.restrict_chat_member(
                chat_id=chat_id, user_id=uid,
                permissions=OPEN_PERMS if close else _full_perms(),
            )
        except TelegramError:
            pass  # админ/создатель чата и так пишет; либо человека нет в чате
    if not close and guests:
        await _save_guests(chat_id, set())

    if announce:
        await _send_saved_message(
            bot, chat_id,
            "sumrak_close" if close else "sumrak_open",
            DEFAULT_SUMRAK_CLOSE_TEXT if close else DEFAULT_SUMRAK_OPEN_TEXT,
            reply_to=reply_to,
        )


async def _resolve_chat_user(bot, token: str) -> Tuple[int, str]:
    """Находит человека для личного доступа: ID, @username или имя из чата."""
    v = _clean_recipient_input(token)
    if v.lstrip("@").isdigit():
        uid = int(v.lstrip("@"))
        d = known_users.get(uid, {})
        return uid, (f"@{d['u']}" if d.get("u") else d.get("n") or str(uid))

    q = v.lstrip("@").lower()
    exact = [(u, d) for u, d in known_users.items()
             if (d.get("u") or "") == q or (d.get("n") or "").lower() == q]
    if len(exact) == 1:
        uid, d = exact[0]
        return uid, (f"@{d['u']}" if d.get("u") else d.get("n") or str(uid))
    if len(exact) > 1:
        lines = [f"• {d.get('n') or '—'} (ID {u})" for u, d in exact[:6]]
        raise RuntimeError("Под этим именем несколько человек, укажи @username или ID:\n" + "\n".join(lines))

    if _is_latin_username(v):
        try:
            chat = await bot.get_chat(f"@{q}")
            if getattr(chat, "type", "") == "private":
                return chat.id, f"@{q}"
        except TelegramError:
            pass
    raise RuntimeError(
        f"Не знаю пользователя «{v}». Пусть он напишет что-нибудь в чат "
        "(бот его запомнит) или укажи числовой ID."
    )


async def _sumrak_personal(bot, chat_id: int, token: str, open_for_user: bool) -> str:
    """!сумракофф @user — открыть чат лично; !сумрак @user — вернуть закрытие."""
    try:
        uid, label = await _resolve_chat_user(bot, token)
    except RuntimeError as e:
        return f"❌ {e}"

    if uid == ADMIN_ID or uid in bot_admin_ids:
        return f"ℹ️ {label} — админ бота, ему чат открыт всегда."

    guests = await _load_guests(chat_id)
    if open_for_user:
        try:
            await bot.restrict_chat_member(chat_id=chat_id, user_id=uid, permissions=OPEN_PERMS)
        except TelegramError as e:
            return f"❌ Не удалось открыть чат для {label}: {e}\nЧеловек должен быть участником чата."
        guests.add(uid)
        await _save_guests(chat_id, guests)
        note = "" if await _chat_is_closed(bot, chat_id) else \
            "\nℹ️ Сейчас чат и так открыт для всех — личный доступ сработает, когда его закроют."
        return f"✅ Для {label} чат открыт лично, для остальных остаётся закрытым.{note}"

    try:
        await bot.restrict_chat_member(chat_id=chat_id, user_id=uid, permissions=_full_perms())
    except TelegramError as e:
        return f"❌ Не удалось изменить доступ для {label}: {e}"
    guests.discard(uid)
    await _save_guests(chat_id, guests)
    return f"🔒 Личный доступ {label} убран — он снова пишет по общим правилам чата."


async def sumrak_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat or user.is_bot or not message.text:
        return

    m = _SUMRAK_RE.match(message.text.strip())
    if not m:
        return

    # Не админ бота — молча игнорируем, сообщение идёт дальше как обычное.
    if not is_bot_admin_user(user):
        return

    is_off = bool(m.group(1))
    arg = m.group(2)
    close = (not is_off) if SUMRAK_CLOSES_CHAT else is_off
    in_group = chat.type in ("group", "supergroup")

    if in_group:
        if chat.id not in allowed_chat_ids:
            await message.reply_text("❌ Этот чат не привязан к боту.")
            raise ApplicationHandlerStop
        targets = [chat.id]
    else:
        if len(allowed_chat_ids) == 1:
            targets = list(allowed_chat_ids)
        elif not allowed_chat_ids:
            await message.reply_text("❌ К боту не привязан ни один чат.")
            raise ApplicationHandlerStop
        else:
            await message.reply_text(
                f"У бота несколько привязанных чатов — напиши команду прямо в нужном чате."
            )
            raise ApplicationHandlerStop

    chat_id = targets[0]

    # Личный доступ: «!сумракофф @user» открывает чат человеку, «!сумрак @user» — закрывает обратно.
    if arg:
        # «открыть» = команда, которая по настройке открывает чат (сумракофф по умолчанию)
        open_for_user = not close
        result = await _sumrak_personal(context.bot, chat_id, arg, open_for_user)
        await message.reply_text(result)
        raise ApplicationHandlerStop

    try:
        await _set_chat_closed(
            context.bot, chat_id, close, announce=True,
            reply_to=message.message_id if in_group else None,
        )
    except TelegramError as e:
        log.exception("Сумрак: не удалось изменить права чата %s", chat_id)
        await message.reply_text(
            f"❌ Не удалось {'закрыть' if close else 'открыть'} чат: {e}\n"
            "Боту нужны права администратора с правом ограничивать участников."
        )
        raise ApplicationHandlerStop

    if not in_group:
        await message.reply_text(f"{'🌑 Чат закрыт' if close else '🌕 Чат открыт'} ({chat_id}).")
    raise ApplicationHandlerStop


# =========================================================
# ПОДАРКИ ТОЛЬКО УЧАСТНИКАМ ЧАТА
# =========================================================

async def _is_chat_member(bot, chat_id: int, user_id: int) -> bool:
    """True, если человек состоит в чате. Если проверить нельзя (у бота нет прав
    видеть участников) — не блокируем выдачу и пишем предупреждение в лог."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except TelegramError as e:
        low = str(e).lower()
        if "user not found" in low or "participant" in low:
            return False
        log.warning("Не удалось проверить участие %s в чате %s: %s", user_id, chat_id, e)
        return True
    status = member.status
    if status in ("member", "administrator", "creator"):
        return True
    if status == "restricted":
        return bool(getattr(member, "is_member", True))
    return False


async def _reply_nonmember(message, user) -> None:
    text, ents = render_template(
        str(S.get("nonmember_text") or DEFAULT_NONMEMBER_TEXT),
        S.get("nonmember_entities"),
        {"user": user.full_name or (f"@{user.username}" if user.username else str(user.id))},
        mention_user=user,
    )
    try:
        if S.get("nonmember_photo"):
            await message.reply_photo(photo=S["nonmember_photo"], caption=text, caption_entities=ents or None)
        else:
            await message.reply_text(text, entities=ents or None)
    except TelegramError:
        log.exception("Не удалось отправить текст для не вступивших в чат")


async def show_sumrak_menu(query) -> None:
    def kind(prefix):
        return "📷 + текст" if S.get(f"{prefix}_photo") else "📝 текст"
    await query.edit_message_text(
        f"🌗 {b('СУМРАК — ТЕКСТЫ ЧАТА')}\n\n"
        f"{b(SUMRAK_ON_WORD)} — закрыть чат, {b(SUMRAK_OFF_WORD)} — открыть.\n"
        f"{b(SUMRAK_OFF_WORD + ' @user')} — открыть чат лично одному человеку "
        f"(для остальных закрыт), {b(SUMRAK_ON_WORD + ' @user')} — вернуть ему закрытие.\n"
        "Команды доступны только админам бота.\n\n"
        "Тексты ниже отправляются в чат после закрытия/открытия — командами, кнопками 🔒/🔓 "
        "в админке и при автоматическом открытии по таймеру.\n\n"
        f"🔒 После закрытия: {kind('sumrak_close')}\n"
        f"🔓 После открытия: {kind('sumrak_open')}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔒 Текст после закрытия", callback_data="sumrak_close_msg")],
            [InlineKeyboardButton("🔓 Текст после открытия", callback_data="sumrak_open_msg")],
            [InlineKeyboardButton("♻️ Вернуть стандартные тексты", callback_data="sumrak_reset")],
            [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
        ]), parse_mode=ParseMode.HTML,
    )


def members_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "⛔ Выключить проверку" if S["require_member"] else "🚀 Включить проверку",
            callback_data="members_toggle",
        )],
        [InlineKeyboardButton("📝 Текст для не вступивших", callback_data="members_msg")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


async def show_members_menu(query) -> None:
    status = "🟢 ВКЛЮЧЕНА" if S["require_member"] else "🔴 ВЫКЛЮЧЕНА"
    await query.edit_message_text(
        f"🚪 {b('ПОДАРКИ ТОЛЬКО УЧАСТНИКАМ ЧАТА')}\n\n"
        f"Проверка: {b(status)}\n\n"
        "Когда человек выигрывает подарок, бот проверяет, состоит ли он в чате. "
        "Если нет — подарок не уходит, а в ответ приходит текст, который ты задаёшь ниже "
        "(его можно с фото, жирным шрифтом и Premium Emoji; подстановка "
        "<code>{user}</code> — упоминание человека).\n\n"
        "Работает и в розыгрыше, и в «Угадай число». Для проверки бот должен быть "
        "администратором чата.\n\n"
        f"Сообщение: {'📷 + текст' if S.get('nonmember_photo') else '📝 текст'}",
        reply_markup=members_menu_keyboard(), parse_mode=ParseMode.HTML,
    )


# =========================================================
# КАНАЛЫ И ПОСТЫ С КНОПКОЙ
# =========================================================

_STYLE_LABELS = {None: "⚪ обычная", "primary": "🔵 синяя", "success": "🟢 зелёная", "danger": "🔴 красная"}


def _make_bold_map() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for i in range(26):
        m[chr(ord("A") + i)] = chr(0x1D5D4 + i)
        m[chr(ord("a") + i)] = chr(0x1D5EE + i)
    for i in range(10):
        m[chr(ord("0") + i)] = chr(0x1D7EC + i)
    return m


_BOLD_MAP = _make_bold_map()


def _parse_button_text(message) -> Tuple[str, Optional[str], bool]:
    """Разбирает текст кнопки из сообщения админа.
    Telegram не умеет форматирование внутри кнопки, поэтому:
      • Premium Emoji → становится иконкой кнопки (icon_custom_emoji_id, берётся первое);
      • жирный шрифт → Unicode-жирные буквы (только латиница и цифры).
    Возвращает (текст, id иконки, были ли жирные символы, которые нельзя преобразовать)."""
    raw, entities = collect_entities(message)
    remove, bold = [], []
    icon: Optional[str] = None
    for e in entities:
        if e.type == MessageEntity.CUSTOM_EMOJI:
            remove.append((e.offset, e.length))
            icon = icon or e.custom_emoji_id
        elif e.type == MessageEntity.BOLD:
            bold.append((e.offset, e.length))

    out, pos, unbold = [], 0, False
    for ch in raw:
        width = 2 if ord(ch) > 0xFFFF else 1
        if not any(st <= pos < st + ln for st, ln in remove):
            if any(st <= pos < st + ln for st, ln in bold):
                if ch in _BOLD_MAP:
                    ch = _BOLD_MAP[ch]
                elif ch.isalpha():
                    unbold = True
            out.append(ch)
        pos += width
    return "".join(out).strip(), icon, unbold


def _normalize_button_url(raw: str) -> Optional[str]:
    v = (raw or "").strip()
    if not v or " " in v:
        return None
    if v.startswith("@") and len(v) > 2:
        return f"https://t.me/{v[1:]}"
    low = v.lower()
    if low.startswith(("http://", "https://", "tg://")):
        return v
    if "." in v:
        return "https://" + v
    return None


def _post_markup(btn: Optional[Dict[str, Any]], with_icon: bool = True) -> Optional[InlineKeyboardMarkup]:
    if not btn:
        return None
    extra: Dict[str, Any] = {}
    if btn.get("style") in ("primary", "success", "danger"):
        extra["style"] = btn["style"]
    if with_icon and btn.get("icon"):
        extra["icon_custom_emoji_id"] = str(btn["icon"])
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(btn["text"], url=btn["url"], api_kwargs=extra or None)
    ]])


def channels_menu_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for ch in post_channels:
        rows.append([InlineKeyboardButton(f"🗑 Отвязать: {ch['title'][:28]}", callback_data=f"channels:del:{ch['id']}")])
    if len(post_channels) < MAX_CHANNELS:
        rows.append([InlineKeyboardButton("➕ Привязать канал", callback_data="channels:add")])
    if post_channels:
        rows.append([InlineKeyboardButton("📝 Создать пост", callback_data="post:new")])
    rows.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(rows)


async def show_channels_menu(query, note: str = "") -> None:
    lines = [f"📢 {b('КАНАЛЫ И ПОСТЫ')}", ""]
    if note:
        lines += [note, ""]
    if not post_channels:
        lines.append("Каналов пока нет.")
    else:
        for i, ch in enumerate(post_channels, 1):
            uname = f" (@{esc(ch['username'])})" if ch.get("username") else ""
            lines.append(f"{i}. {esc(ch['title'])}{uname}")
    lines += [
        "",
        f"Занято: {b(f'{len(post_channels)}/{MAX_CHANNELS}')}",
        "",
        "Чтобы привязать канал, добавь бота в администраторы канала с правом "
        "«Публикация сообщений», затем нажми «➕ Привязать» и перешли любой пост канала "
        "или отправь @username / ID канала.",
    ]
    await query.edit_message_text(
        "\n".join(lines), reply_markup=channels_menu_keyboard(), parse_mode=ParseMode.HTML,
    )


async def _save_channels() -> None:
    await db.set_setting("post_channels", json.dumps(post_channels, ensure_ascii=False))


async def _handle_channel_add(message, context) -> None:
    """Привязка канала: пересланный пост канала, @username, ссылка t.me или ID."""
    chat_ref = None
    fwd = getattr(message, "forward_origin", None)
    if fwd is not None and getattr(fwd, "chat", None) is not None:
        chat_ref = fwd.chat.id
    elif getattr(message, "forward_from_chat", None) is not None:
        chat_ref = message.forward_from_chat.id
    else:
        v = _clean_recipient_input(message.text or "")
        if v.lstrip("-").isdigit():
            chat_ref = int(v)
        elif v:
            chat_ref = "@" + v.lstrip("@")

    if chat_ref is None:
        await message.reply_text("❌ Перешли пост канала или отправь @username / ID канала.")
        return

    try:
        chat = await context.bot.get_chat(chat_ref)
    except TelegramError as e:
        await message.reply_text(f"❌ Не нашёл канал: {e}")
        return
    if chat.type != "channel":
        await message.reply_text("❌ Это не канал. Нужен именно канал.")
        return
    if any(ch["id"] == chat.id for ch in post_channels):
        await message.reply_text("ℹ️ Этот канал уже привязан.")
        return
    if len(post_channels) >= MAX_CHANNELS:
        await message.reply_text(f"❌ Уже привязано максимум каналов ({MAX_CHANNELS}). Сначала отвяжи один.")
        return

    try:
        me = await context.bot.get_chat_member(chat.id, context.bot.id)
    except TelegramError as e:
        await message.reply_text(f"❌ Бот не видит участников канала: {e}\nДобавь бота админом канала.")
        return
    can_post = getattr(me, "can_post_messages", None)
    if me.status != "administrator" or can_post is False:
        await message.reply_text(
            "❌ Бот не админ канала или у него нет права «Публикация сообщений». "
            "Выдай право и повтори."
        )
        return

    post_channels.append({"id": chat.id, "title": chat.title or str(chat.id), "username": chat.username or ""})
    await _save_channels()
    context.user_data.pop("waiting_channel_add", None)
    await message.reply_text(f"✅ Канал «{chat.title}» привязан ({len(post_channels)}/{MAX_CHANNELS}).")


def _post_panel_text(draft: Dict[str, Any]) -> str:
    lines = [f"📝 {b('ПОСТ')}", "", "Выше — предпросмотр. Выбери каналы и при желании добавь кнопку.", ""]
    btn = draft.get("btn")
    if btn:
        lines.append(f"🔘 Кнопка: «{esc(btn['text'])}» → {esc(btn['url'])}")
        lines.append(f"Цвет: {_STYLE_LABELS.get(btn.get('style'))}")
        if btn.get("icon"):
            lines.append("Иконка: Premium Emoji ✅")
    else:
        lines.append("🔘 Кнопки нет.")
    return "\n".join(lines)


def _post_panel_kb(draft: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows = []
    for ch in post_channels:
        mark = "✅" if ch["id"] in draft["targets"] else "⬜"
        rows.append([InlineKeyboardButton(f"{mark} {ch['title'][:32]}", callback_data=f"post:t:{ch['id']}")])
    rows.append([InlineKeyboardButton(
        "🔘 Изменить кнопку" if draft.get("btn") else "🔘 Добавить кнопку", callback_data="post:btn",
    )])
    if draft.get("btn"):
        cur = draft["btn"].get("style")
        def cb(label, style):
            mark = "•" if cur == style else ""
            return InlineKeyboardButton(f"{mark}{label}", callback_data=f"post:c:{style or 'none'}")
        rows.append([cb("⚪", None), cb("🔵", "primary"), cb("🟢", "success"), cb("🔴", "danger")])
        rows.append([InlineKeyboardButton("🗑 Убрать кнопку", callback_data="post:btn_del")])
    rows.append([InlineKeyboardButton("🚀 Опубликовать", callback_data="post:go")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="post:cancel")])
    return InlineKeyboardMarkup(rows)


async def _refresh_post_preview(bot, draft: Dict[str, Any]) -> None:
    if not draft.get("preview_id"):
        return
    try:
        await bot.edit_message_reply_markup(
            chat_id=draft["src_chat"], message_id=draft["preview_id"],
            reply_markup=_post_markup(draft.get("btn")),
        )
    except TelegramError as e:
        if "not modified" not in str(e).lower():
            log.warning("Не удалось обновить предпросмотр поста: %s", e)


async def _send_post_panel(bot, draft: Dict[str, Any]) -> None:
    await bot.send_message(
        chat_id=draft["src_chat"], text=_post_panel_text(draft),
        reply_markup=_post_panel_kb(draft), parse_mode=ParseMode.HTML,
    )


async def _post_capture_content(message, context) -> None:
    """Админ прислал содержимое поста — делаем предпросмотр и панель."""
    draft: Dict[str, Any] = {
        "src_chat": message.chat_id, "src_msg": message.message_id,
        "btn": None, "targets": [c["id"] for c in post_channels], "preview_id": None,
    }
    try:
        preview = await context.bot.copy_message(
            chat_id=message.chat_id, from_chat_id=message.chat_id, message_id=message.message_id,
        )
        draft["preview_id"] = preview.message_id
    except TelegramError as e:
        await message.reply_text(f"❌ Не удалось сделать предпросмотр: {e}")
        return
    context.user_data["post_draft"] = draft
    context.user_data.pop("waiting_post_content", None)
    await _send_post_panel(context.bot, draft)


async def _post_handle_button_text(message, context) -> None:
    draft = context.user_data.get("post_draft")
    if not draft or not message.text:
        await message.reply_text("❌ Отправь текст кнопки.")
        return
    text, icon, unbold = _parse_button_text(message)
    if not text:
        await message.reply_text("❌ Текст кнопки пустой.")
        return
    if len(text) > 60:
        await message.reply_text("❌ Слишком длинный текст кнопки (до 60 символов).")
        return
    draft["_btn_text"] = text
    draft["_btn_icon"] = icon
    context.user_data.pop("waiting_post_btn_text", None)
    context.user_data["waiting_post_btn_url"] = True

    notes = []
    if icon:
        notes.append("Premium Emoji станет иконкой перед текстом кнопки.")
    if unbold:
        notes.append("Кириллица в кнопке жирной не бывает — Telegram не поддерживает форматирование в кнопках, "
                     "жирным стали только латиница и цифры.")
    await message.reply_text(
        "✅ Текст кнопки принят: «" + text + "»" + ("\n" + "\n".join(notes) if notes else "")
        + "\n\nТеперь отправь ссылку для кнопки (https://…, @username или t.me/…)."
    )


async def _post_handle_button_url(message, context) -> None:
    draft = context.user_data.get("post_draft")
    url = _normalize_button_url(message.text or "")
    if not draft or not url:
        await message.reply_text("❌ Не похоже на ссылку. Пример: https://t.me/username")
        return
    old_style = (draft.get("btn") or {}).get("style")
    draft["btn"] = {"text": draft.pop("_btn_text"), "url": url, "style": old_style,
                    "icon": draft.pop("_btn_icon", None)}
    context.user_data.pop("waiting_post_btn_url", None)
    await _refresh_post_preview(context.bot, draft)
    await _send_post_panel(context.bot, draft)


async def _post_publish(query, context) -> None:
    draft = context.user_data.get("post_draft")
    if not draft:
        await query.answer("Черновик потерян, создай пост заново", show_alert=True)
        return
    targets = [c for c in post_channels if c["id"] in draft["targets"]]
    if not targets:
        await query.answer("Выбери хотя бы один канал", show_alert=True)
        return
    await query.answer("Публикую…")

    btn = draft.get("btn")
    results = []
    for ch in targets:
        try:
            await context.bot.copy_message(
                chat_id=ch["id"], from_chat_id=draft["src_chat"], message_id=draft["src_msg"],
                reply_markup=_post_markup(btn),
            )
            results.append(f"✅ {esc(ch['title'])}")
        except TelegramError as e:
            if btn and btn.get("icon"):
                # Канал не принял Premium-иконку кнопки (бывает без доп. username на Fragment) —
                # публикуем без иконки.
                try:
                    await context.bot.copy_message(
                        chat_id=ch["id"], from_chat_id=draft["src_chat"], message_id=draft["src_msg"],
                        reply_markup=_post_markup(btn, with_icon=False),
                    )
                    results.append(f"✅ {esc(ch['title'])} — ⚠️ без Premium-иконки кнопки (канал её не принял)")
                    continue
                except TelegramError as e2:
                    e = e2
            log.exception("Не удалось опубликовать пост в %s", ch["id"])
            results.append(f"❌ {esc(ch['title'])}: {esc(e)}")

    context.user_data.pop("post_draft", None)
    await query.edit_message_text(
        f"📢 {b('РЕЗУЛЬТАТ ПУБЛИКАЦИИ')}\n\n" + "\n".join(results),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Ещё пост", callback_data="post:new")],
            [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
        ]),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# ЛИЧНЫЙ ШАНС ДЛЯ КОНКРЕТНОГО @USERNAME
# =========================================================

def userchance_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Задать / изменить", callback_data="userchance:set")],
        [InlineKeyboardButton("🗑 Убрать личный шанс", callback_data="userchance:del")],
        [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
    ])


def _userchance_text(note: str = "") -> str:
    lines = [f"👤 {b('ЛИЧНЫЙ ШАНС')}", ""]
    if note:
        lines += [note, ""]
    lines.append(f"Общий шанс для всех остальных: {b(fmt_num(S['chance']) + '%')}")
    lines.append("")
    if not user_chances:
        lines.append("Личных шансов пока нет.")
    else:
        lines.append(f"Заданы для {b(len(user_chances))}:")
        for i, (name, value) in enumerate(sorted(user_chances.items()), 1):
            line = f"{i}. @{esc(name)} — {b(fmt_num(value) + '%')}"
            if sum(len(x) + 1 for x in lines) + len(line) > 3600:
                lines.append(f"… и ещё {len(user_chances) - i + 1}")
                break
            lines.append(line)
    lines += [
        "",
        "Личный шанс заменяет общий только для этого @username "
        "(купленный буст умножается поверх). 0 — человек никогда не выиграет.",
        "",
        "Команда: <code>/userchance @username 25</code>\n"
        "Убрать: <code>/userchance @username off</code>",
    ]
    return "\n".join(lines)


async def show_userchance_menu(query, note: str = "") -> None:
    await query.edit_message_text(
        _userchance_text(note),
        reply_markup=userchance_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


_USERCHANCE_OFF_WORDS = {"off", "del", "delete", "remove", "reset", "-", "сброс", "убрать", "удалить", "снять", "нет"}


async def _apply_userchance(args_text: str) -> Tuple[bool, str]:
    """Разбирает «@username 25» / «@username off» и применяет. Возвращает (ok, сообщение HTML)."""
    parts = args_text.replace(",", ".").split()
    if len(parts) != 2:
        return False, (
            "❌ Формат: <code>@username 25</code> (шанс в %) или "
            "<code>@username off</code>, чтобы убрать."
        )
    name = _normalize_username(_clean_recipient_input(parts[0]))
    if not name:
        return False, "❌ Некорректный @username."

    word = parts[1].lower()
    if word in _USERCHANCE_OFF_WORDS:
        if name not in user_chances:
            return False, f"ℹ️ У @{esc(name)} нет личного шанса."
        user_chances.pop(name, None)
        await _save_user_chances()
        return True, f"✅ Личный шанс @{esc(name)} убран — снова общий {b(fmt_num(S['chance']) + '%')}."

    try:
        value = float(word.rstrip("%"))
        if not 0 <= value <= MAX_CHANCE:
            raise ValueError
    except ValueError:
        return False, f"❌ Шанс — число от 0 до {fmt_num(MAX_CHANCE)} (можно с точкой)."

    user_chances[name] = value
    await _save_user_chances()
    return True, f"✅ Личный шанс @{esc(name)}: {b(fmt_num(value) + '%')}."


async def _save_user_chances() -> None:
    await _db_set("user_chances", json.dumps(user_chances, ensure_ascii=False))


async def userchance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("❌ Только администратор может менять шанс.")
        return
    if not context.args:
        await update.message.reply_text(_userchance_text(), parse_mode=ParseMode.HTML)
        return
    ok, text = await _apply_userchance(" ".join(context.args))
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


def _normalize_username(value: str) -> str:
    value = value.strip().lstrip("@").lower()
    # Telegram username: 5–32 символа, буквы/цифры/underscore.
    if not value or len(value) > 32 or not all(c.isalnum() or c == "_" for c in value):
        return ""
    return value


def _parse_usernames(text: str) -> List[str]:
    import re
    raw = re.split(r"[\s,;]+", text.strip())
    return [name for name in (_normalize_username(x) for x in raw) if name]


# =========================================================
# ДОСТУПНЫЕ ЧАТЫ — МЕНЮ
# =========================================================

def access_menu_keyboard(current_chat=None) -> InlineKeyboardMarkup:
    buttons = []
    if current_chat and current_chat.type in ("group", "supergroup"):
        if current_chat.id in allowed_chat_ids or len(allowed_chat_ids) < MAX_ALLOWED_CHATS:
            buttons.append(
                [InlineKeyboardButton("➕ Привязать этот чат", callback_data="access_add_current")]
            )
    buttons.append([InlineKeyboardButton("➕ По @username / ID", callback_data="access_add_username")])
    for chat_id in list(allowed_chat_ids)[:MAX_ALLOWED_CHATS]:
        buttons.append([
            InlineKeyboardButton(f"🔒 Закрыть {chat_id}", callback_data=f"admin_close:{chat_id}"),
            InlineKeyboardButton(f"🔓 Открыть {chat_id}", callback_data=f"admin_open:{chat_id}"),
        ])
        buttons.append(
            [InlineKeyboardButton(f"🗑 Удалить {chat_id}", callback_data=f"access_remove:{chat_id}")]
        )
    if allowed_chat_ids:
        buttons.append([InlineKeyboardButton("🧹 Очистить всё", callback_data="access_clear")])
    buttons.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(buttons)


async def show_access_menu(query) -> None:
    current_chat = query.message.chat if query.message else None
    lines = [f"🔐 {b('ДОСТУПНЫЕ ЧАТЫ')}", "", "Бот работает только в этих чатах:"]
    if not allowed_chat_ids:
        lines.append("❌ Пока ни одного чата нет.")
    else:
        for i, chat_id in enumerate(list(allowed_chat_ids)[:MAX_ALLOWED_CHATS], 1):
            lines.append(f"{i}. <code>{chat_id}</code>")
    lines += [
        "",
        f"📊 Занято: {b(f'{len(allowed_chat_ids)}/{MAX_ALLOWED_CHATS}')}",
        "",
        "🔒/🔓 — закрыть или открыть чат прямо сейчас, бесплатно, без Stars.",
        "",
        "Для приватной группы открой /admin прямо в ней.",
    ]
    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=access_menu_keyboard(current_chat),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# ЦЕНЫ И ПАРАМЕТРЫ — МЕНЮ
# =========================================================

def prices_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(
                f"🔒 Цена закрытия чата: {S['price_close_chat']} ⭐",
                callback_data="price_close_chat",
            )],
            [InlineKeyboardButton(
                f"⏳ Длительность закрытия: {fmt_num(S['close_minutes'])} мин",
                callback_data="close_minutes",
            )],
            [InlineKeyboardButton(
                f"🍀 Цена буста: {S['price_boost']} ⭐",
                callback_data="price_boost",
            )],
            [InlineKeyboardButton(
                f"✖️ Множитель буста: ×{fmt_num(S['boost_multiplier'])}",
                callback_data="boost_multiplier",
            )],
            [InlineKeyboardButton(
                f"🕐 Длительность буста: {fmt_num(S['boost_hours'])} ч",
                callback_data="boost_hours",
            )],
            [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
        ]
    )


async def show_prices_menu(query) -> None:
    await query.edit_message_text(
        f"💵 {b('ЦЕНЫ И ПАРАМЕТРЫ')}\n\n"
        "Здесь можно поменять все цены Stars из магазина /start "
        "и параметры покупок. Нажми на нужный пункт.",
        reply_markup=prices_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# ЛУДКА 777 — МЕНЮ
# =========================================================

def ludka_menu_keyboard() -> InlineKeyboardMarkup:
    toggle_text = "⛔ Остановить" if S["ludka_enabled"] else "🎰 Запустить"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(
                toggle_text,
                callback_data="ludka_stop" if S["ludka_enabled"] else "ludka_launch",
            )],
            [
                InlineKeyboardButton("💰 Цена", callback_data="ludka_price"),
                InlineKeyboardButton("🎁 Приз", callback_data="ludka_prize"),
            ],
            [InlineKeyboardButton("📝 Сообщение", callback_data="ludka_message")],
            [InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")],
        ]
    )


async def show_ludka_menu(query) -> None:
    status = "🟢 ВКЛЮЧЕНА" if S["ludka_enabled"] else "🔴 ВЫКЛЮЧЕНА"
    await query.edit_message_text(
        f"🎰 {b('НАСТРОЙКИ ЛУДКИ 777')}\n\n"
        f"Статус: {status}\n"
        f"🎁 Приз: {esc(S['ludka_prize'])}\n"
        f"💰 Цена 1 вращения: {b(S['ludka_price'])} соо",
        reply_markup=ludka_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def launch_ludka(query, context) -> None:
    if not query.message:
        return

    S["ludka_enabled"] = True
    ludka_progress.clear()
    await _db_set("ludka_enabled", True)

    if query.message.chat.type in ("group", "supergroup"):
        S["ludka_chat_id"] = query.message.chat_id
        await _db_set("ludka_chat_id", S["ludka_chat_id"])

    chat_id = S["ludka_chat_id"] or query.message.chat_id

    try:
        if S["ludka_photo"]:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=S["ludka_photo"],
                caption=S["ludka_text"],
                caption_entities=S["ludka_entities"] or None,
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=S["ludka_text"],
                entities=S["ludka_entities"] or None,
            )
        await query.edit_message_text(
            f"✅ {b('Лудка 777 запущена!')}\n\n"
            f"Каждые {b(S['ludka_price'])} сообщений участника — одно вращение.",
            reply_markup=ludka_menu_keyboard(),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        log.exception("Ошибка запуска лудки")
        await query.edit_message_text(
            f"❌ Не удалось запустить лудку.\n\n<code>{esc(e)}</code>",
            reply_markup=ludka_menu_keyboard(),
            parse_mode=ParseMode.HTML,
        )


async def stop_ludka(query) -> None:
    S["ludka_enabled"] = False
    ludka_progress.clear()
    await _db_set("ludka_enabled", False)
    await query.edit_message_text(
        f"⛔ {b('Лудка 777 остановлена.')}",
        reply_markup=ludka_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# УГАДАЙ ЧИСЛО — МЕНЮ И ЛОГИКА ИВЕНТА
# =========================================================

def guess_menu_keyboard() -> InlineKeyboardMarkup:
    toggle_text = "⛔ Остановить" if S["guess_enabled"] else "🚀 Запустить (случайное)"
    rows = [
        [InlineKeyboardButton(
            toggle_text,
            callback_data="guess_stop" if S["guess_enabled"] else "guess_launch",
        )],
    ]
    if not S["guess_enabled"]:
        rows.append([InlineKeyboardButton("🎯 Своё число (например 777)", callback_data="guess_secret")])
    rows += [
        [InlineKeyboardButton("✏️ Название ивента (текст/фото)", callback_data="guess_name")],
        [
            InlineKeyboardButton(f"🔽 От: {S['guess_min']}", callback_data="guess_min"),
            InlineKeyboardButton(f"🔼 До: {S['guess_max']}", callback_data="guess_max"),
        ],
        [InlineKeyboardButton("🎁 Приз победителю (текст/фото)", callback_data="guess_prize")],
        [InlineKeyboardButton(
            f"🎁 ID подарка: {S['guess_gift_id'] or 'общий выбор'}", callback_data="guess_gift",
        )],
    ]
    # Быстрый выбор чата, если запускаем не прямо из группы — иначе
    # ивент "не запускается" из личных сообщений с ботом.
    for chat_id in list(allowed_chat_ids)[:MAX_ALLOWED_CHATS]:
        mark = "✅ " if S["guess_chat_id"] == chat_id else ""
        rows.append([InlineKeyboardButton(
            f"{mark}💬 Чат для запуска: {chat_id}", callback_data=f"guess_setchat:{chat_id}",
        )])
    rows.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(rows)


async def show_guess_menu(query) -> None:
    status = "🟢 ИДЁТ" if S["guess_enabled"] else "🔴 ОСТАНОВЛЕН"
    chat_line = (
        f"\n💬 Чат запуска: <code>{S['guess_chat_id']}</code>"
        if S["guess_chat_id"] else
        "\n💬 Чат запуска: ❌ не выбран (открой /admin прямо в группе или "
        "выбери чат кнопкой ниже, если он уже привязан в «🔐 Доступные чаты»)"
    )
    gift_line = f"\n🎁 Подарок победителю: {b(S['guess_gift_id'])}" if S["guess_gift_id"] else ""
    await query.edit_message_text(
        f"🔢 {b('УГАДАЙ ЧИСЛО')}\n\n"
        f"Статус: {status}{chat_line}\n"
        f"🔽 Диапазон: {b(S['guess_min'])} – {b(S['guess_max'])}\n"
        f"🎁 Приз (текст): {esc(S['guess_prize_text'])[:200]}{gift_line}\n\n"
        "🚀 «Запустить» — бот сам загадывает случайное число из диапазона.\n"
        "🎯 «Своё число» — ты сам вводишь число (например 777), и оно "
        "становится правильным ответом.\n\n"
        "Объявление отправляется в чат запуска и закрепляется. Первый, кто "
        "напишет загаданное число, получает приз (текст/фото и, если задан "
        "ID подарка, — настоящий Telegram-подарок).",
        reply_markup=guess_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


def _build_guess_announcement():
    """Собирает текст объявления: свой текст/фото админа + дописанный ботом
    жирный диапазон чисел. Entities склеиваются безопасно через append_bold."""
    name_text = S["guess_name"] or "Мини-ивент «Угадай число»!"
    name_entities = S["guess_name_entities"] or []
    lo, hi = str(S["guess_min"]), str(S["guess_max"])
    addition = (
        f"\n\n🔢 Я загадал число от {lo} до {hi}.\n"
        "✍️ Пишите свои варианты прямо в чат!\n\n"
        "🏆 Первый, кто угадает, получит приз!"
    )
    return append_bold(name_text, name_entities, addition, bold_spans=[lo, hi])


async def _do_launch_guess(context, chat_id: int, secret: int):
    """Общая логика запуска ивента «Угадай число»: публикует объявление,
    закрепляет его и сохраняет состояние. Возвращает (sent_message, error)."""
    # Если задано ручное число вне текущего диапазона — расширяем диапазон,
    # чтобы объявление честно показывало пользователям верхнюю/нижнюю границу.
    if secret < int(S["guess_min"]):
        S["guess_min"] = secret
        await _db_set("guess_min", secret)
    if secret > int(S["guess_max"]):
        S["guess_max"] = secret
        await _db_set("guess_max", secret)

    # Если предыдущий раунд не был явно завершён — снимаем закрепление
    # старого сообщения, чтобы в чате не копились закреплённые объявления.
    if S["guess_chat_id"] and S["guess_message_id"]:
        try:
            await context.bot.unpin_chat_message(
                chat_id=S["guess_chat_id"], message_id=S["guess_message_id"],
            )
        except TelegramError:
            pass

    text, entities = _build_guess_announcement()
    try:
        if S["guess_name_photo"]:
            sent = await context.bot.send_photo(
                chat_id=chat_id,
                photo=S["guess_name_photo"],
                caption=text,
                caption_entities=entities or None,
            )
        else:
            sent = await context.bot.send_message(
                chat_id=chat_id, text=text, entities=entities or None,
            )
    except Exception as e:
        log.exception("Ошибка публикации ивента «Угадай число»")
        return None, e

    S["guess_chat_id"] = chat_id
    S["guess_enabled"] = True
    S["guess_secret"] = secret
    S["guess_message_id"] = sent.message_id
    await _db_set("guess_chat_id", chat_id)
    await _db_set("guess_enabled", True)
    await _db_set("guess_secret", secret)
    await _db_set("guess_message_id", sent.message_id)

    try:
        await context.bot.pin_chat_message(
            chat_id=chat_id, message_id=sent.message_id, disable_notification=False,
        )
    except TelegramError:
        log.warning("Не удалось закрепить сообщение ивента (нет прав?)")

    return sent, None


async def launch_guess_event(query, context) -> None:
    if not query.message:
        return

    if S["guess_min"] >= S["guess_max"]:
        await query.answer("❌ «От» должно быть меньше «До».", show_alert=True)
        return

    if query.message.chat.type in ("group", "supergroup"):
        S["guess_chat_id"] = query.message.chat_id
        await _db_set("guess_chat_id", S["guess_chat_id"])

    chat_id = S["guess_chat_id"]
    if not chat_id:
        await query.answer(
            "❌ Чат не выбран. Открой /admin прямо в нужной группе, либо "
            "сначала привяжи чат в «🔐 Доступные чаты» и выбери его кнопкой "
            "в этом меню.",
            show_alert=True,
        )
        return

    secret = random.randint(int(S["guess_min"]), int(S["guess_max"]))
    sent, err = await _do_launch_guess(context, chat_id, secret)

    if err is not None:
        await query.edit_message_text(
            f"❌ Не удалось запустить ивент.\n\n<code>{esc(err)}</code>",
            reply_markup=guess_menu_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    await query.edit_message_text(
        f"✅ {b('Ивент запущен и закреплён!')}\n\n"
        f"🔢 Диапазон: {S['guess_min']} – {S['guess_max']}",
        reply_markup=guess_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def _unpin_guess_message(context) -> None:
    if S["guess_chat_id"] and S["guess_message_id"]:
        try:
            await context.bot.unpin_chat_message(
                chat_id=S["guess_chat_id"], message_id=S["guess_message_id"],
            )
        except TelegramError:
            pass


async def stop_guess_event(query, context) -> None:
    await _unpin_guess_message(context)
    S["guess_enabled"] = False
    S["guess_secret"] = None
    await _db_set("guess_enabled", False)
    await _db_set("guess_secret", "")
    await query.edit_message_text(
        f"⛔ {b('Ивент «Угадай число» остановлен.')}",
        reply_markup=guess_menu_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def process_guess_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Проверяет, не угадал ли участник загаданное число. Работает только
    в чате, привязанном к текущему ивенту."""
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat:
        return
    if not S["guess_enabled"] or S["guess_secret"] is None:
        return
    if S["guess_chat_id"] and chat.id != S["guess_chat_id"]:
        return

    raw = (message.text or "").strip()
    if not raw:
        return
    try:
        guess = int(raw)
    except ValueError:
        return

    if guess != int(S["guess_secret"]):
        return

    # Не участник чата — приз не выдаём, раунд продолжается.
    if S.get("require_member") and not await _is_chat_member(context.bot, chat.id, user.id):
        await _reply_nonmember(message, user)
        return

    # Число угадано — завершаем раунд сразу, чтобы приз не ушёл дважды
    # (в т.ч. если два человека напишут верный ответ почти одновременно).
    S["guess_enabled"] = False
    winning_number = S["guess_secret"]
    S["guess_secret"] = None
    await _db_set("guess_enabled", False)
    await _db_set("guess_secret", "")

    await _unpin_guess_message(context)

    try:
        await context.bot.send_message(
            chat_id=chat.id,
            text=(
                f"🎉 <a href=\"tg://user?id={user.id}\">{esc(user.full_name)}</a> "
                f"{b('угадал(а) число ' + str(winning_number) + '!')}"
            ),
            parse_mode=ParseMode.HTML,
        )
    except TelegramError:
        pass

    blocked = bool(user.username and _normalize_username(user.username) in blocked_usernames)

    # Если для ивента задан конкретный подарок — отправляем его победителю
    # по-настоящему (через привязанный аккаунт), как в обычном розыгрыше.
    if S["guess_gift_id"] and not blocked:
        sent_gift = await _send_gift_to_user(
            user, context, gift_id=S["guess_gift_id"],
            congrats_text="🎁 Поздравляем! Ты угадал число и выиграл подарок!",
        )
        if sent_gift:
            await _bump_winner(user)
        else:
            await _add_pending(user, chat.id, message.message_id, S["guess_gift_id"])
            await _notify_admin(
                context,
                f"⚠️ Не удалось отправить подарок победителю «Угадай число» "
                f"(id {user.id}). Победитель добавлен в «🎁 Невыданные» в "
                "админ-панели — проверь аккаунт выдачи и баланс.",
            )
    elif blocked:
        await _send_blocked_message(message)
        return

    try:
        if S["guess_prize_photo"]:
            await message.reply_photo(
                photo=S["guess_prize_photo"],
                caption=S["guess_prize_text"],
                caption_entities=S["guess_prize_entities"] or None,
            )
        else:
            await message.reply_text(
                S["guess_prize_text"], entities=S["guess_prize_entities"] or None,
            )
    except Exception:
        log.exception("Ошибка отправки приза победителю «Угадай число»")


# =========================================================
# СПИСОК ПОДАРКОВ
# =========================================================

async def _available_gifts(context) -> List[Dict]:
    """Список подарков: сначала Bot API, при ошибке — через привязанный аккаунт."""
    try:
        gifts = await context.bot.get_available_gifts()
        return [
            {"id": int(g.id), "stars": int(g.star_count)}
            for g in gifts.gifts
            if not getattr(g, "remaining_count", None) == 0
        ]
    except Exception:
        log.warning("Bot API не отдал подарки, пробую MTProto")
        return await gift_account.list_gifts()


async def show_gifts(query, context) -> None:
    try:
        gifts = await _available_gifts(context)
    except Exception as e:
        log.exception("Ошибка получения подарков")
        await query.edit_message_text(
            f"❌ Не удалось получить список подарков.\n\n<code>{esc(e)}</code>",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    if not gifts:
        await query.edit_message_text(
            "❌ Доступных подарков нет.", reply_markup=back_kb()
        )
        return

    gifts.sort(key=lambda g: g["stars"])
    buttons = []
    for g in gifts[:20]:
        mark = "✅ " if str(g["id"]) == str(S["selected_gift_id"]) else ""
        buttons.append(
            [InlineKeyboardButton(f"{mark}🎁 {g['stars']} ⭐", callback_data=f"gift:{g['id']}")]
        )
    buttons.append([InlineKeyboardButton("🤖 Автоматический выбор", callback_data="gift:auto")])
    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="main")])

    await query.edit_message_text(
        f"🎁 {b('ВЫБОР ПОДАРКА')}\n\n"
        f"Сейчас: {b(S['selected_gift_id'] or 'Авто (самый дешёвый)')}\n\n"
        "Подарок оплачивается со Stars привязанного аккаунта.",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


async def select_gift(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query

    if not is_admin(update):
        await query.answer("❌ Нет доступа.", show_alert=True)
        return

    await query.answer()

    if query.data == "gift:auto":
        S["selected_gift_id"] = None
        await _db_set("selected_gift_id", "")
        await query.edit_message_text(
            f"🤖 {b('Автоматический выбор включен.')}\n\n"
            "Бот будет брать самый дешёвый доступный подарок.",
            reply_markup=back_kb("gifts"),
            parse_mode=ParseMode.HTML,
        )
        return

    S["selected_gift_id"] = query.data.split(":", 1)[1]
    await _db_set("selected_gift_id", S["selected_gift_id"])
    await query.edit_message_text(
        f"✅ {b('Подарок выбран!')}\n\n🎁 ID: <code>{esc(S['selected_gift_id'])}</code>",
        reply_markup=back_kb("gifts"),
        parse_mode=ParseMode.HTML,
    )


# =========================================================
# ВЫДАЧА ПОДАРКА ПОБЕДИТЕЛЮ
# =========================================================

async def _send_blocked_message(message) -> None:
    """Отправляет заблокированному пользователю настраиваемое сообщение
    (текст и/или фото — задаётся в админ-панели)."""
    try:
        if S["blocked_photo"]:
            await message.reply_photo(
                photo=S["blocked_photo"],
                caption=S["blocked_text"],
                caption_entities=S["blocked_entities"] or None,
            )
        else:
            await message.reply_text(
                S["blocked_text"], entities=S["blocked_entities"] or None
            )
    except Exception:
        log.exception("Ошибка отправки сообщения о блокировке")


async def _send_gift_to_user(
    user, context: ContextTypes.DEFAULT_TYPE,
    gift_id: Optional[str] = None,
    congrats_text: str = "🎁 Поздравляем! Ты выиграл подарок!",
) -> bool:
    """Общая логика отправки настоящего Telegram-подарка через MTProto.
    Если gift_id не задан — берётся общий выбор (S['selected_gift_id']) или
    самый дешёвый доступный. Используется и обычным розыгрышем, и ивентом
    «Угадай число» (с собственным ID подарка, см. S['guess_gift_id'])."""
    try:
        gifts = await _available_gifts(context)
        if not gifts:
            log.error("Нет доступных подарков для выдачи")
            stats["errors"] += 1
            await _db_inc_stat("errors")
            return False

        gift = None
        wanted_id = gift_id if gift_id is not None else S["selected_gift_id"]
        if wanted_id:
            gift = next((g for g in gifts if str(g["id"]) == str(wanted_id)), None)
            if gift is None:
                log.warning("Указанный ID подарка %s недоступен — беру самый дешёвый", wanted_id)
        if gift is None:
            gift = min(gifts, key=lambda g: g["stars"])

        # Аккаунт-отправитель проще всего находит получателя по @username,
        # но если его нет — пробуем числовой user_id.
        recipient = f"@{user.username}" if user.username else user.id

        await gift_account.send_gift(
            recipient=recipient,
            gift_id=int(gift["id"]),
            message=congrats_text,
        )

        stats["gifts_sent"] += 1
        await _db_inc_stat("gifts_sent")
        return True

    except Exception as e:
        log.exception("Ошибка отправки подарка через MTProto")
        stats["errors"] += 1
        await _db_inc_stat("errors")

        text = str(e)
        if "BALANCE_TOO_LOW" in text:
            log.error("На привязанном аккаунте не хватает Stars")
            await _notify_admin(context, "⚠️ Не хватает Stars на аккаунте выдачи.")
        elif "не привязан" in text:
            await _notify_admin(context, "⚠️ Аккаунт выдачи не привязан — подарок не отправлен.")
        elif "PEER_ID_INVALID" in text or "Cannot find any entity" in text:
            await _notify_admin(
                context,
                f"⚠️ Аккаунт выдачи не видит победителя (id {user.id}). "
                "Нужен @username или общий чат.",
            )
        return False


_RESOLVE_ERRORS = (
    "Cannot find any entity", "Could not find the input entity", "PEER_ID_INVALID",
    "USERNAME_NOT_OCCUPIED", "USERNAME_INVALID", "No user has", "Nobody is using this username",
)


def _clean_recipient_input(raw: str) -> str:
    """Убирает пробелы, t.me-ссылки и хвосты вроде ?start=..."""
    v = (raw or "").strip()
    low = v.lower()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/", "https://telegram.me/", "telegram.me/"):
        if low.startswith(prefix):
            v = v[len(prefix):]
            break
    return v.split("?")[0].strip().strip("/").strip()


def _is_latin_username(value: str) -> bool:
    name = _normalize_username(value)
    return bool(name) and all(ord(c) < 128 for c in name) and len(name) >= 4


def _find_known_users(query: str) -> List[Tuple[int, Dict[str, Any]]]:
    """Ищет в каталоге известных пользователей по @username или имени в чате.
    Сначала точные совпадения, если их нет — частичные. Свежие — первыми."""
    q = query.strip().lstrip("@").lower()
    if not q:
        return []
    exact, partial = [], []
    for uid, d in known_users.items():
        uname = (d.get("u") or "").lower()
        name = (d.get("n") or "").lower()
        if q == uname or q == name:
            exact.append((uid, d))
        elif q in name or (uname and q in uname):
            partial.append((uid, d))
    found = exact or partial
    found.sort(key=lambda x: x[1].get("ts", 0), reverse=True)
    return found


def _manual_candidates(raw: str) -> List[Dict[str, Any]]:
    """Превращает ввод админа в список кандидатов на получателя.
    Принимает: @username, username, t.me/username, числовой ID, а также имя,
    под которым человек виден в чате (например «мм»)."""
    v = _clean_recipient_input(raw)
    if not v:
        raise RuntimeError("Получатель не указан.")

    cands: List[Dict[str, Any]] = []

    def add(uid=None, username="", name=""):
        targets: List[Any] = []
        if username:
            targets.append(f"@{username}")
        if uid:
            targets.append(int(uid))
        if not targets:
            return
        key = int(uid) if uid else username
        if any(c["key"] == key for c in cands):
            return
        cands.append({"key": key, "user_id": int(uid) if uid else None,
                      "username": username, "name": name, "targets": targets})

    if v.lstrip("@").isdigit():
        uid = int(v.lstrip("@"))
        d = known_users.get(uid, {})
        add(uid, d.get("u", ""), d.get("n", ""))
        return cands

    if _is_latin_username(v):
        uname = _normalize_username(v)
        matches = [(u, d) for u, d in known_users.items() if d.get("u") == uname]
        if matches:
            uid, d = matches[0]
            add(uid, uname, d.get("n", ""))
        else:
            add(None, uname, "")

    # Имя (в т.ч. кириллица), как человек отображается в чате. Автоматически
    # берём только ТОЧНОЕ совпадение — чтобы не отправить подарок не тому.
    if not cands:
        q = v.lstrip("@").lower()
        found = _find_known_users(v)
        exact = [(u, d) for u, d in found
                 if q == (d.get("n") or "").lower() or q == (d.get("u") or "").lower()]
        if len(exact) == 1:
            uid, d = exact[0]
            add(uid, d.get("u", ""), d.get("n", ""))
        elif found:
            lines = []
            for uid, d in (exact or found)[:6]:
                uname = f"@{d['u']}" if d.get("u") else "без username"
                lines.append(f"• {d.get('n') or '—'} ({uname}, ID {uid})")
            head = (f"Под «{v}» подходит несколько человек" if len(exact) > 1
                    else f"Точного совпадения с «{v}» нет, но похожие есть")
            raise RuntimeError(head + ", уточни @username или ID:\n" + "\n".join(lines))

    if not cands:
        raise RuntimeError(
            f"Не нашёл «{v}». Telegram-аккаунт ищется только по @username (латиницей) "
            "или числовому ID, а имя работает лишь если человек уже писал в разрешённом "
            "чате при работающем боте.\n\nОтправь @username или ID — их видно в профиле "
            "или в «Невыданных подарках»."
        )
    return cands


async def _send_manual_gift(recipient: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, str]:
    """Ручная дополнительная выдача из админ-панели.
    Получатель: @username, ID, t.me-ссылка или имя из чата.
    Возвращает (успех, текст ошибки)."""
    try:
        cands = _manual_candidates(recipient)

        gifts = await _available_gifts(context)
        if not gifts:
            raise RuntimeError("Нет доступных Telegram-подарков (проверь аккаунт выдачи и баланс Stars).")
        wanted = S.get("selected_gift_id")
        if wanted:
            gift = next((g for g in gifts if str(g["id"]) == str(wanted)), None)
            if gift is None:
                raise RuntimeError(f"Подарок ID {wanted} сейчас недоступен — выбери другой в «🎁 Подарки».")
        else:
            gift = min(gifts, key=lambda g: g["stars"])

        target_info: Optional[Dict[str, Any]] = None
        last_resolve_error = ""
        for cand in cands:
            hint_chat = None
            if cand["user_id"]:
                hint_chat = (last_message_by_user_id.get(cand["user_id"]) or {}).get("chat_id")
            for target in cand["targets"]:
                try:
                    await gift_account.send_gift(
                        target, int(gift["id"]), None,
                        hint_chat_id=hint_chat,
                        hint_query=cand.get("name") or cand.get("username") or None,
                    )
                    target_info = cand
                    break
                except Exception as e:
                    if any(x in str(e) for x in _RESOLVE_ERRORS):
                        last_resolve_error = str(e)
                        continue
                    raise
            if target_info:
                break

        if target_info is None:
            raise RuntimeError(
                f"Аккаунт выдачи не нашёл получателя «{_clean_recipient_input(recipient)}» "
                "в Telegram. Проверь @username (он должен существовать) или укажи числовой ID "
                "человека, который уже писал в чате, где состоит аккаунт выдачи."
                + (f"\n\nДетали: {last_resolve_error}" if last_resolve_error else "")
            )
    except Exception as e:
        log.exception("Ручная выдача не удалась")
        text = str(e)
        if "BALANCE_TOO_LOW" in text:
            text = "Не хватает Stars на аккаунте выдачи."
        elif "не привязан" in text:
            text = "Аккаунт выдачи не привязан — привяжи его в «👤 Аккаунт выдачи»."
        stats["errors"] += 1
        await _db_inc_stat("errors")
        return False, text

    label = target_info["name"] or (f"@{target_info['username']}" if target_info["username"] else recipient)
    values = {"recipient": _clean_recipient_input(recipient), "user": label}
    mention = None
    if target_info["user_id"]:
        mention = User(id=target_info["user_id"], first_name=label or "user", is_bot=False)
    text, entities = render_template(
        str(S.get("manual_gift_text") or DEFAULT_MANUAL_GIFT_TEXT),
        S.get("manual_gift_entities"),
        values,
        mention_user=mention,
    )
    entities = entities or None
    photo = S.get("manual_gift_photo")

    # Сообщение о выдаче уходит в чат, где человек писал последний раз (ответом
    # на его последнее сообщение), а не в чат админа.
    info = None
    if target_info["user_id"]:
        info = last_message_by_user_id.get(target_info["user_id"])
    if info is None and target_info["username"]:
        info = last_message_by_username.get(target_info["username"])

    sent_to_user_chat = False
    if info:
        try:
            if photo:
                await context.bot.send_photo(
                    chat_id=info["chat_id"], photo=photo, caption=text,
                    caption_entities=entities, reply_to_message_id=info.get("message_id"),
                )
            else:
                await context.bot.send_message(
                    chat_id=info["chat_id"], text=text, entities=entities,
                    reply_to_message_id=info.get("message_id"),
                )
            sent_to_user_chat = True
        except TelegramError:
            log.exception("Не удалось отправить сообщение о ручной выдаче в чат пользователя")

    # Фолбэк: чат человека неизвестен — шлём в чат админа.
    if not sent_to_user_chat:
        chat_id = context.user_data.get("manual_gift_chat_id")
        if chat_id:
            try:
                if photo:
                    await context.bot.send_photo(chat_id=chat_id, photo=photo, caption=text, caption_entities=entities)
                else:
                    await context.bot.send_message(chat_id=chat_id, text=text, entities=entities)
            except TelegramError:
                log.exception("Не удалось отправить сообщение о ручной выдаче")

    stats["gifts_sent"] += 1
    await _db_inc_stat("gifts_sent")
    return True, ""


async def give_gift(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if not user:
        return False

    if user.username and _normalize_username(user.username) in blocked_usernames:
        if update.message:
            await _send_blocked_message(update.message)
        return False

    return await _send_gift_to_user(user, context)


async def _notify_admin(context, text: str) -> None:
    try:
        await context.bot.send_message(ADMIN_ID, text)
    except TelegramError:
        pass


# =========================================================
# ВВОД АДМИНА (настройки, привязка аккаунта)
# =========================================================

async def admin_content_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update) or not update.message:
        return

    message = update.message

    # ---------- «СНЯТЬ СПАМ» — ручное снятие антиспам-мута ----------
    text_raw = (message.text or "").strip()
    if text_raw.lower().startswith("снять спам"):
        await _handle_unspam_command(message, context, text_raw)
        raise ApplicationHandlerStop

    # ---------- ПРИВЯЗКА MTProto ----------
    account_step = context.user_data.get("account_step")
    if account_step:
        await _handle_account_step(account_step, message, context)
        raise ApplicationHandlerStop

    # ---------- ДОБАВЛЕНИЕ В БЛОКИРОВКУ ----------
    if context.user_data.get("waiting_blocked_add"):
        names = _parse_usernames(message.text or "")
        if not names:
            await message.reply_text("❌ Не найдено ни одного корректного username.")
            raise ApplicationHandlerStop

        before = len(blocked_usernames)
        blocked_usernames.update(names)
        await db.save_blocked_usernames(blocked_usernames)
        added = len(blocked_usernames) - before
        context.user_data.pop("waiting_blocked_add", None)

        await message.reply_text(
            f"✅ {b('Блокировка установлена.')}\n\n"
            + "\n".join(f"🚫 @{esc(name)}" for name in names)
            + f"\n\nНовых добавлено: {b(added)}\n"
              f"Всего заблокировано: {b(len(blocked_usernames))}",
            parse_mode=ParseMode.HTML,
        )
        raise ApplicationHandlerStop

    # ---------- СНЯТИЕ БАНА ----------
    if context.user_data.get("waiting_blocked_remove"):
        names = _parse_usernames(message.text or "")
        if not names:
            await message.reply_text("❌ Не найдено ни одного корректного username.")
            raise ApplicationHandlerStop

        removed = []
        not_found = []
        for name in names:
            if name in blocked_usernames:
                blocked_usernames.remove(name)
                removed.append(name)
            else:
                not_found.append(name)

        await db.save_blocked_usernames(blocked_usernames)
        context.user_data.pop("waiting_blocked_remove", None)

        lines = [f"🟢 {b('ГОТОВО')}"]
        if removed:
            lines += ["", "Сняли бан:"] + [f"🟢 @{esc(name)}" for name in removed]
        if not_found:
            lines += ["", "Не были заблокированы:"] + [f"ℹ️ @{esc(name)}" for name in not_found]
        lines += ["", f"Осталось заблокировано: {b(len(blocked_usernames))}"]

        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    # ---------- ДОБАВЛЕНИЕ ЧАТА ----------
    if context.user_data.get("waiting_access_chat"):
        await _handle_access_input(message, context)
        raise ApplicationHandlerStop

    # ---------- СВОЁ ЗНАЧЕНИЕ ШАНСА ----------
    if context.user_data.get("waiting_chance"):
        raw = (message.text or "").strip().replace(",", ".")
        try:
            value = float(raw)
            if not MIN_CHANCE <= value <= MAX_CHANCE:
                raise ValueError
        except ValueError:
            await message.reply_text(
                f"❌ Укажи число от {MIN_CHANCE} до {fmt_num(MAX_CHANCE)}."
            )
            raise ApplicationHandlerStop

        S["chance"] = value
        await _db_set("chance", value)
        context.user_data.pop("waiting_chance", None)
        await message.reply_text(
            f"✅ Шанс установлен: {b(fmt_num(value) + '%')}", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    # ---------- ЦЕНЫ И ПАРАМЕТРЫ ----------
    if context.user_data.get("waiting_price_close_chat"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 1_000_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число Stars от 1 до 1 000 000.")
            raise ApplicationHandlerStop
        S["price_close_chat"] = value
        await _db_set("price_close_chat", value)
        context.user_data.pop("waiting_price_close_chat", None)
        await message.reply_text(
            f"✅ Цена закрытия чата: {b(value)} ⭐", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_price_boost"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 1_000_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число Stars от 1 до 1 000 000.")
            raise ApplicationHandlerStop
        S["price_boost"] = value
        await _db_set("price_boost", value)
        context.user_data.pop("waiting_price_boost", None)
        await message.reply_text(
            f"✅ Цена буста: {b(value)} ⭐", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_close_minutes"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 100_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число минут от 1 до 100000.")
            raise ApplicationHandlerStop
        S["close_minutes"] = value
        await _db_set("close_minutes", value)
        context.user_data.pop("waiting_close_minutes", None)
        await message.reply_text(
            f"✅ Длительность закрытия чата: {b(value)} мин", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_boost_multiplier"):
        try:
            value = float((message.text or "").strip().replace(",", "."))
            if not 1 <= value <= 1000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи число от 1 до 1000.")
            raise ApplicationHandlerStop
        S["boost_multiplier"] = value
        await _db_set("boost_multiplier", value)
        context.user_data.pop("waiting_boost_multiplier", None)
        await message.reply_text(
            f"✅ Множитель буста: ×{b(fmt_num(value))}", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_boost_hours"):
        try:
            value = float((message.text or "").strip().replace(",", "."))
            if not 0.1 <= value <= 100_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи число часов от 0.1 до 100000.")
            raise ApplicationHandlerStop
        S["boost_hours"] = value
        await _db_set("boost_hours", value)
        context.user_data.pop("waiting_boost_hours", None)
        await message.reply_text(
            f"✅ Длительность буста: {b(fmt_num(value))} ч", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    # ---------- ТЕКСТ КОММЕНТАРИЯ К ПОСТАМ ----------
    if context.user_data.get("waiting_comment_text"):
        await _save_message_setting(
            message, context, "waiting_comment_text",
            text_key="comment_text", photo_key="comment_photo",
            ent_key="comment_entities",
            title="Текст комментария",
        )
        raise ApplicationHandlerStop

    # ---------- АНТИСПАМ: ЛИМИТ / МУТ / НАПОМИНАНИЕ ----------
    if context.user_data.get("waiting_antispam_limit"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 10_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число от 1 до 10000.")
            raise ApplicationHandlerStop
        S["antispam_limit"] = value
        await _db_set("antispam_limit", value)
        context.user_data.pop("waiting_antispam_limit", None)
        await message.reply_text(f"✅ Лимит сообщений: {b(value)}/мин", parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_mute"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 100_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число минут от 1 до 100000.")
            raise ApplicationHandlerStop
        S["antispam_mute_minutes"] = value
        await _db_set("antispam_mute_minutes", value)
        context.user_data.pop("waiting_antispam_mute", None)
        await message.reply_text(f"✅ Длительность мута: {b(value)} мин", parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_reminder"):
        try:
            value = int((message.text or "").strip())
            if not 0 <= value <= 100_000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число от 0 до 100000 (0 — выключить).")
            raise ApplicationHandlerStop
        S["antispam_reminder_every"] = value
        await _db_set("antispam_reminder_every", value)
        context.user_data.pop("waiting_antispam_reminder", None)
        await message.reply_text(
            f"✅ Напоминание раз в {b(value)} сообщений" if value else "✅ Напоминания выключены",
            parse_mode=ParseMode.HTML,
        )
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_window"):
        try:
            value = int((message.text or "").strip())
            if not 5 <= value <= 3600: raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число секунд от 5 до 3600.")
            raise ApplicationHandlerStop
        S["antispam_window"] = value
        await _db_set("antispam_window", value)
        context.user_data.pop("waiting_antispam_window", None)
        await message.reply_text(f"✅ Окно антиспама: {b(value)} сек", parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_reason"):
        text = (message.text or "").strip()
        if not text or len(text) > 500:
            await message.reply_text("❌ Причина должна быть от 1 до 500 символов.")
            raise ApplicationHandlerStop
        S["antispam_reason"] = text
        await _db_set("antispam_reason", text)
        context.user_data.pop("waiting_antispam_reason", None)
        await message.reply_text(f"✅ Причина мута: {b(text)}", parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_channel_add"):
        await _handle_channel_add(message, context)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_post_content"):
        await _post_capture_content(message, context)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_post_btn_text"):
        await _post_handle_button_text(message, context)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_post_btn_url"):
        await _post_handle_button_url(message, context)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_sumrak_close_msg"):
        await _save_message_setting(message, context, "waiting_sumrak_close_msg", "sumrak_close_text", "sumrak_close_photo", "sumrak_close_entities", "Текст после закрытия")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_sumrak_open_msg"):
        await _save_message_setting(message, context, "waiting_sumrak_open_msg", "sumrak_open_text", "sumrak_open_photo", "sumrak_open_entities", "Текст после открытия")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_nonmember_msg"):
        await _save_message_setting(message, context, "waiting_nonmember_msg", "nonmember_text", "nonmember_photo", "nonmember_entities", "Текст для не вступивших")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_botadmin_add") or context.user_data.get("waiting_botadmin_del"):
        remove = bool(context.user_data.get("waiting_botadmin_del"))
        text = await _botadmins_apply(message.text or "", context.bot, remove=remove)
        if not text.startswith("❌ Отправь"):
            context.user_data.pop("waiting_botadmin_del" if remove else "waiting_botadmin_add", None)
        await message.reply_text(text, parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_userchance_set"):
        ok, text = await _apply_userchance((message.text or "").strip())
        if ok:
            context.user_data.pop("waiting_userchance_set", None)
        await message.reply_text(text, parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_userchance_del"):
        names = [n for n in (_normalize_username(_clean_recipient_input(x)) for x in (message.text or "").split()) if n]
        if not names:
            await message.reply_text("❌ Отправь @username.")
            raise ApplicationHandlerStop
        removed = [n for n in names if user_chances.pop(n, None) is not None]
        context.user_data.pop("waiting_userchance_del", None)
        if removed:
            await _save_user_chances()
            await message.reply_text(
                "✅ Личный шанс убран: " + ", ".join(f"@{esc(n)}" for n in removed),
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.reply_text("ℹ️ Ни у кого из указанных нет личного шанса.")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_reminder_msg"):
        await _save_message_setting(message, context, "waiting_antispam_reminder_msg", "antispam_reminder_text", "antispam_reminder_photo", "antispam_reminder_entities", "Напоминание о правиле")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_antispam_message"):
        await _save_message_setting(message, context, "waiting_antispam_message", "antispam_text", "antispam_photo", "antispam_entities", "Сообщение антиспама")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_manual_gift_message"):
        await _save_message_setting(message, context, "waiting_manual_gift_message", "manual_gift_text", "manual_gift_photo", "manual_gift_entities", "Сообщение ручной выдачи")
        raise ApplicationHandlerStop

    if context.user_data.get("waiting_manual_gift_recipient"):
        recipient = (message.text or "").strip()
        if not recipient or len(recipient) > 100:
            await message.reply_text("❌ Отправь @username или числовой ID.")
            raise ApplicationHandlerStop
        context.user_data.pop("waiting_manual_gift_recipient", None)
        context.user_data["manual_gift_recipient"] = recipient
        ok, error = await _send_manual_gift(recipient, context)
        context.user_data.pop("manual_gift_recipient", None)
        if ok:
            await message.reply_text("✅ Подарок успешно отправлен.")
        else:
            await message.reply_text(f"❌ Ручная выдача не удалась для {recipient}:\n{error}")
        raise ApplicationHandlerStop

    # ---------- ЦЕНА ЛУДКИ ----------
    if context.user_data.get("waiting_ludka_price"):
        try:
            value = int((message.text or "").strip())
            if not 1 <= value <= 100000:
                raise ValueError
        except ValueError:
            await message.reply_text("❌ Укажи целое число от 1 до 100000.")
            raise ApplicationHandlerStop

        S["ludka_price"] = value
        await _db_set("ludka_price", value)
        context.user_data.pop("waiting_ludka_price", None)
        await message.reply_text(
            f"✅ Цена лудки: {b(value)} соо", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    # ---------- ПРИЗ ЛУДКИ ----------
    if context.user_data.get("waiting_ludka_prize"):
        if not message.text:
            await message.reply_text("❌ Отправь текст приза.")
            raise ApplicationHandlerStop
        if len(message.text) > 4096:
            await message.reply_text("❌ Максимум 4096 символов.")
            raise ApplicationHandlerStop

        text, entities = collect_entities(message)
        S["ludka_prize"] = text
        S["ludka_prize_entities"] = entities
        await _db_set("ludka_prize", text)
        await _db_set_entities("ludka_prize_entities", entities)
        context.user_data.pop("waiting_ludka_prize", None)

        await message.reply_text("✅ Приз сохранён:")
        await message.reply_text(text, entities=entities or None)
        raise ApplicationHandlerStop

    # ---------- ЗАГОЛОВОК ТОПА ХАЛЯВЩИКОВ ----------
    if context.user_data.get("waiting_top_header"):
        if not message.text:
            await message.reply_text("❌ Отправь текст.")
            raise ApplicationHandlerStop
        if len(message.text) > 2000:
            await message.reply_text("❌ Максимум 2000 символов.")
            raise ApplicationHandlerStop

        text, entities = collect_entities(message)
        S["top_header_text"] = text
        S["top_header_entities"] = entities
        await _db_set("top_header_text", text)
        await _db_set_entities("top_header_entities", entities)
        context.user_data.pop("waiting_top_header", None)

        await message.reply_text("✅ Заголовок топа сохранён:")
        await message.reply_text(text, entities=entities or None)
        raise ApplicationHandlerStop

    # ---------- ПОДПИСЬ МЕСТА В ТОПЕ ----------
    if context.user_data.get("waiting_top_place"):
        place_n = context.user_data["waiting_top_place"]
        if not message.text:
            await message.reply_text("❌ Отправь текст.")
            raise ApplicationHandlerStop
        if len(message.text) > 200:
            await message.reply_text("❌ Максимум 200 символов.")
            raise ApplicationHandlerStop

        text, entities = collect_entities(message)
        S["top_place_labels"][place_n - 1] = {"text": text, "entities": entities}
        await _db_set("top_place_labels", _place_labels_to_json(S["top_place_labels"]))
        context.user_data.pop("waiting_top_place", None)

        await message.reply_text(f"✅ Место {place_n} сохранено:")
        await message.reply_text(text, entities=entities or None)
        raise ApplicationHandlerStop

    # ---------- ТЕКСТ ПРИ РУЧНОЙ ВЫДАЧЕ НЕВЫДАННОГО ПОДАРКА ----------
    if context.user_data.get("waiting_pending_text"):
        if not message.text:
            await message.reply_text("❌ Отправь текст.")
            raise ApplicationHandlerStop
        if len(message.text) > 4096:
            await message.reply_text("❌ Максимум 4096 символов.")
            raise ApplicationHandlerStop

        text, entities = collect_entities(message)
        S["pending_issue_text"] = text
        S["pending_issue_entities"] = entities
        await _db_set("pending_issue_text", text)
        await _db_set_entities("pending_issue_entities", entities)
        context.user_data.pop("waiting_pending_text", None)

        await message.reply_text("✅ Текст при ручной выдаче сохранён:")
        await message.reply_text(text, entities=entities or None)
        raise ApplicationHandlerStop

    # ---------- СООБЩЕНИЕ ЛУДКИ ----------
    if context.user_data.get("waiting_ludka_message"):
        await _save_message_setting(
            message, context, "waiting_ludka_message",
            text_key="ludka_text", photo_key="ludka_photo", ent_key="ludka_entities",
            title="Сообщение лудки",
        )
        raise ApplicationHandlerStop

    # ---------- ТЕКСТ УДАЧНОЙ ВЫДАЧИ ----------
    if context.user_data.get("waiting_win_success_message"):
        await _save_message_setting(
            message, context, "waiting_win_success_message",
            text_key="win_success_text", photo_key="win_success_photo",
            ent_key="win_success_entities",
            title="Текст удачной выдачи",
        )
        raise ApplicationHandlerStop

    # ---------- ЗАПАСНОЙ ТЕКСТ (ВЫДАЧА НЕ УДАЛАСЬ) ----------
    if context.user_data.get("waiting_win_message"):
        await _save_message_setting(
            message, context, "waiting_win_message",
            text_key="win_text", photo_key="win_photo", ent_key="win_entities",
            title="Запасной текст",
        )
        raise ApplicationHandlerStop

    # ---------- СООБЩЕНИЕ О БЛОКИРОВКЕ ----------
    if context.user_data.get("waiting_blocked_message"):
        await _save_message_setting(
            message, context, "waiting_blocked_message",
            text_key="blocked_text", photo_key="blocked_photo",
            ent_key="blocked_entities",
            title="Сообщение о блокировке",
        )
        raise ApplicationHandlerStop

    # ---------- НАЗВАНИЕ ИВЕНТА «УГАДАЙ ЧИСЛО» ----------
    if context.user_data.get("waiting_guess_name"):
        await _save_message_setting(
            message, context, "waiting_guess_name",
            text_key="guess_name", photo_key="guess_name_photo",
            ent_key="guess_name_entities",
            title="Название ивента",
        )
        raise ApplicationHandlerStop

    # ---------- МИН/МАКС ДИАПАЗОНА «УГАДАЙ ЧИСЛО» ----------
    if context.user_data.get("waiting_guess_min") or context.user_data.get("waiting_guess_max"):
        flag = "waiting_guess_min" if context.user_data.get("waiting_guess_min") else "waiting_guess_max"
        key = "guess_min" if flag == "waiting_guess_min" else "guess_max"
        try:
            value = int((message.text or "").strip())
        except ValueError:
            await message.reply_text("❌ Укажи целое число.")
            raise ApplicationHandlerStop

        other_key = "guess_max" if key == "guess_min" else "guess_min"
        if key == "guess_min" and value >= int(S[other_key]):
            await message.reply_text("❌ «От» должно быть меньше текущего «До».")
            raise ApplicationHandlerStop
        if key == "guess_max" and value <= int(S[other_key]):
            await message.reply_text("❌ «До» должно быть больше текущего «От».")
            raise ApplicationHandlerStop

        S[key] = value
        await _db_set(key, value)
        context.user_data.pop(flag, None)
        await message.reply_text(
            f"✅ Диапазон: {b(S['guess_min'])} – {b(S['guess_max'])}", parse_mode=ParseMode.HTML
        )
        raise ApplicationHandlerStop

    # ---------- ПРИЗ ПОБЕДИТЕЛЮ «УГАДАЙ ЧИСЛО» ----------
    if context.user_data.get("waiting_guess_prize"):
        await _save_message_setting(
            message, context, "waiting_guess_prize",
            text_key="guess_prize_text", photo_key="guess_prize_photo",
            ent_key="guess_prize_entities",
            title="Приз победителю",
        )
        raise ApplicationHandlerStop

    # ---------- СВОЁ ЧИСЛО «УГАДАЙ ЧИСЛО» (запускает ивент сразу) ----------
    if context.user_data.get("waiting_guess_secret"):
        try:
            secret = int((message.text or "").strip())
        except ValueError:
            await message.reply_text("❌ Отправь целое число, например 777.")
            raise ApplicationHandlerStop

        if message.chat.type in ("group", "supergroup"):
            chat_id = message.chat_id
        else:
            chat_id = S["guess_chat_id"]

        if not chat_id:
            await message.reply_text(
                "❌ Чат не выбран. Отправь эту команду прямо в нужной группе, "
                "либо сначала привяжи и выбери чат в «🔢 Угадай число»."
            )
            raise ApplicationHandlerStop

        context.user_data.pop("waiting_guess_secret", None)
        sent, err = await _do_launch_guess(context, chat_id, secret)
        if err is not None:
            await message.reply_text(f"❌ Не удалось запустить ивент.\n{err}")
        else:
            await message.reply_text(
                f"✅ Ивент запущен с числом {b(secret)}! Диапазон: "
                f"{S['guess_min']} – {S['guess_max']}",
                parse_mode=ParseMode.HTML,
            )
        raise ApplicationHandlerStop

    # ---------- ID ПОДАРКА ДЛЯ ПОБЕДИТЕЛЯ «УГАДАЙ ЧИСЛО» ----------
    if context.user_data.get("waiting_guess_gift_id"):
        raw = (message.text or "").strip()
        if raw.lower() in ("off", "нет", "выкл", "-"):
            S["guess_gift_id"] = None
            await _db_set("guess_gift_id", "")
            context.user_data.pop("waiting_guess_gift_id", None)
            await message.reply_text("✅ Отдельный подарок отключён — используется общий выбор.")
            raise ApplicationHandlerStop

        if not raw.lstrip("-").isdigit():
            await message.reply_text("❌ Отправь числовой ID подарка или «off».")
            raise ApplicationHandlerStop

        S["guess_gift_id"] = raw
        await _db_set("guess_gift_id", raw)
        context.user_data.pop("waiting_guess_gift_id", None)
        await message.reply_text(f"✅ Подарок для победителя: {b(raw)}", parse_mode=ParseMode.HTML)
        raise ApplicationHandlerStop

    # ---------- ТЕКСТ /START ----------
    if context.user_data.get("waiting_start_text"):
        await _save_message_setting(
            message, context, "waiting_start_text",
            text_key="start_text", photo_key="start_photo",
            ent_key="start_entities",
            title="Текст /start",
        )
        raise ApplicationHandlerStop

    # ---------- ПОДПИСИ КНОПОК /START ----------
    if any(context.user_data.get(f"waiting_btn_{k}") for k in ("close", "boost", "my", "help")):
        which = next(k for k in ("close", "boost", "my", "help") if context.user_data.get(f"waiting_btn_{k}"))
        flag = f"waiting_btn_{which}"
        key = f"btn_{which}_label"
        raw = (message.text or "").strip()
        if not raw:
            await message.reply_text("❌ Отправь текст кнопки.")
            raise ApplicationHandlerStop
        if len(raw) > 60:
            await message.reply_text("❌ Максимум 60 символов.")
            raise ApplicationHandlerStop

        S[key] = raw
        await _db_set(key, raw)
        context.user_data.pop(flag, None)
        await message.reply_text(f"✅ Подпись кнопки сохранена: {esc(raw)}")
        raise ApplicationHandlerStop

    # Админ ничего не настраивает — пропускаем дальше в обычный обработчик.


async def _save_message_setting(
    message, context, flag: str, text_key: str, photo_key: str, ent_key: str, title: str
) -> None:
    if message.photo:
        caption, entities = collect_entities(message, is_caption=True)
        if len(caption) > 1024:
            await message.reply_text("❌ Для фото максимум 1024 символа.")
            return

        S[photo_key] = message.photo[-1].file_id
        S[text_key] = caption
        S[ent_key] = entities
        await _db_set(photo_key, S[photo_key])
        await _db_set(text_key, caption)
        await _db_set_entities(ent_key, entities)
        context.user_data.pop(flag, None)

        await message.reply_text(f"✅ {title} сохранено (фото + текст). Предпросмотр:")
        await message.reply_photo(
            photo=S[photo_key], caption=caption, caption_entities=entities or None
        )
        return

    if message.text:
        text, entities = collect_entities(message)
        if len(text) > 4096:
            await message.reply_text("❌ Максимум 4096 символов.")
            return

        S[text_key] = text
        S[photo_key] = None
        S[ent_key] = entities
        await _db_set(photo_key, "")
        await _db_set(text_key, text)
        await _db_set_entities(ent_key, entities)
        context.user_data.pop(flag, None)

        await message.reply_text(f"✅ {title} сохранено. Предпросмотр:")
        await message.reply_text(text, entities=entities or None)
        return

    await message.reply_text("❌ Отправь текст или фото с подписью.")


async def _handle_account_step(step: str, message, context) -> None:
    text = (message.text or "").strip()

    try:
        if step == "api_id":
            api_id = int(text)
            if api_id <= 0:
                raise ValueError("API ID должен быть положительным числом")
            context.user_data["account_api_id"] = api_id
            context.user_data["account_step"] = "api_hash"
            await message.reply_text(
                f"🔐 Шаг 2/5 — отправь {b('API HASH')} (32 символа).",
                parse_mode=ParseMode.HTML,
            )
            return

        if step == "api_hash":
            if len(text) < 20:
                await message.reply_text("❌ API HASH выглядит некорректно. Отправь ещё раз.")
                return
            context.user_data["account_api_hash"] = text
            context.user_data["account_step"] = "phone"
            await message.reply_text(
                "📱 Шаг 3/5 — отправь номер аккаунта в международном формате, "
                "например <code>+37120000000</code>.",
                parse_mode=ParseMode.HTML,
            )
            return

        if step == "phone":
            phone = text.replace(" ", "")
            if not phone.startswith("+"):
                await message.reply_text("❌ Номер должен начинаться с +.")
                return

            phone_code_hash = await gift_account.start_login(
                context.user_data["account_api_id"],
                context.user_data["account_api_hash"],
                phone,
            )
            context.user_data["account_phone"] = phone
            context.user_data["account_phone_code_hash"] = phone_code_hash
            context.user_data["account_step"] = "code"
            await message.reply_text(
                "📨 Шаг 4/5 — отправь код входа из Telegram.\n\n"
                "Совет: вставь код с пробелами (<code>1 2 3 4 5</code>), "
                "чтобы Telegram его не аннулировал.",
                parse_mode=ParseMode.HTML,
            )
            return

        if step == "code":
            result = await gift_account.finish_login(
                context.user_data["account_phone"],
                text.replace(" ", ""),
                context.user_data["account_phone_code_hash"],
            )
            if result.get("need_password"):
                context.user_data["account_step"] = "password"
                await message.reply_text("🔑 Шаг 5/5 — отправь пароль двухэтапной аутентификации.")
                return

            await _account_linked(message, context)
            return

        if step == "password":
            await gift_account.finish_login_password(text)
            await _account_linked(message, context)
            return

    except Exception as e:
        log.exception("Ошибка привязки MTProto-аккаунта")
        await gift_account.abort_login()
        _clear_waiting(context)
        await message.reply_text(
            f"❌ Не удалось привязать аккаунт.\n\n<code>{esc(e)}</code>\n\n"
            "Открой /admin и попробуй заново.",
            parse_mode=ParseMode.HTML,
        )


async def _account_linked(message, context) -> None:
    _clear_waiting(context)
    try:
        status = await gift_account.account_status()
    except Exception:
        status = {}
    await message.reply_text(
        f"✅ {b('АККАУНТ ПРИВЯЗАН!')}\n\n"
        f"👤 {esc(status.get('name', 'Аккаунт'))}\n"
        f"⭐ Баланс: {b(status.get('balance', '—'))} Stars\n\n"
        "Теперь подарки победителям покупаются с этого аккаунта автоматически.",
        parse_mode=ParseMode.HTML,
    )


async def _handle_access_input(message, context) -> None:
    value = (message.text or "").strip()
    if not value:
        await message.reply_text("❌ Отправь @username группы или числовой chat ID.")
        return

    if value.startswith("@"):
        try:
            chat = await context.bot.get_chat(value)
        except TelegramError as e:
            await message.reply_text(
                f"❌ Не удалось найти чат.\n<code>{esc(e)}</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if chat.type not in ("group", "supergroup"):
            await message.reply_text("❌ Нужна группа или супергруппа.")
            return
        chat_id = chat.id
    else:
        try:
            chat_id = int(value)
        except ValueError:
            await message.reply_text("❌ Нужен @username или числовой chat ID.")
            return

    if chat_id not in allowed_chat_ids and len(allowed_chat_ids) >= MAX_ALLOWED_CHATS:
        await message.reply_text("❌ Уже добавлены 2 чата. Сначала удали один в /admin.")
        return

    allowed_chat_ids.add(chat_id)
    await db.add_allowed_chat(chat_id)          # раньше это забывали сохранить в БД
    context.user_data.pop("waiting_access_chat", None)
    await message.reply_text(
        f"✅ Чат <code>{chat_id}</code> добавлен.", parse_mode=ParseMode.HTML
    )


# =========================================================
# /CANCEL
# =========================================================

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    await gift_account.abort_login()
    _clear_waiting(context)
    await update.message.reply_text("❌ Настройка отменена.")


# =========================================================
# /CHANCE
# =========================================================

async def chance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("❌ Только администратор может менять шанс.")
        return

    if not context.args:
        await update.message.reply_text(
            f"🎯 Сейчас шанс: {b(fmt_num(S['chance']) + '%')}\n\n"
            "Примеры:\n<code>/chance 1</code>\n<code>/chance 0.5</code>\n<code>/chance 0.01</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        value = float(context.args[0].replace(",", "."))
        if not MIN_CHANCE <= value <= MAX_CHANCE:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            f"❌ Укажи число от {MIN_CHANCE} до {fmt_num(MAX_CHANCE)}."
        )
        return

    S["chance"] = value
    await _db_set("chance", value)
    await update.message.reply_text(
        f"✅ Шанс установлен: {b(fmt_num(value) + '%')}", parse_mode=ParseMode.HTML
    )


# =========================================================
# /LUDKA, /LUDKAOFF
# =========================================================

async def ludka_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("❌ Только администратор может управлять лудкой.")
        return

    S["ludka_enabled"] = True
    ludka_progress.clear()
    S["ludka_chat_id"] = update.effective_chat.id
    await _db_set("ludka_enabled", True)
    await _db_set("ludka_chat_id", S["ludka_chat_id"])

    try:
        if S["ludka_photo"]:
            await update.message.reply_photo(
                photo=S["ludka_photo"],
                caption=S["ludka_text"],
                caption_entities=S["ludka_entities"] or None,
            )
        else:
            await update.message.reply_text(
                S["ludka_text"], entities=S["ludka_entities"] or None
            )
    except Exception:
        log.exception("Ошибка публикации лудки")
        await update.message.reply_text("❌ Не удалось опубликовать сообщение лудки.")
        return

    await update.message.reply_text(
        f"🎰 Лудка {b('запущена')}!\n"
        f"💰 Цена 1 вращения: {b(S['ludka_price'])} соо",
        parse_mode=ParseMode.HTML,
    )


async def ludkaoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    S["ludka_enabled"] = False
    ludka_progress.clear()
    await _db_set("ludka_enabled", False)
    await update.message.reply_text("⛔ Лудка 777 остановлена.")


# =========================================================
# /GUESS, /GUESSOFF — ивент «Угадай число»
# =========================================================

async def guess_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        await update.message.reply_text("❌ Только администратор может запускать ивент.")
        return

    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Команду нужно вызвать прямо в группе.")
        return

    # /guess 777 — задать число самому. Без аргумента — случайное из диапазона.
    manual_secret = None
    if context.args:
        try:
            manual_secret = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ После /guess укажи целое число, например /guess 777.")
            return

    if manual_secret is None:
        if S["guess_min"] >= S["guess_max"]:
            await update.message.reply_text("❌ Проверь диапазон в /admin — «От» должно быть меньше «До».")
            return
        secret = random.randint(int(S["guess_min"]), int(S["guess_max"]))
    else:
        secret = manual_secret

    sent, err = await _do_launch_guess(context, update.effective_chat.id, secret)
    if err is not None:
        await update.message.reply_text(f"❌ Не удалось опубликовать сообщение ивента.\n{err}")
        return

    await update.message.reply_text(
        f"✅ Ивент запущен и закреплён! Диапазон: {S['guess_min']} – {S['guess_max']}"
    )


async def guessoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return
    await _unpin_guess_message(context)
    S["guess_enabled"] = False
    S["guess_secret"] = None
    await _db_set("guess_enabled", False)
    await _db_set("guess_secret", "")
    await update.message.reply_text("⛔ Ивент «Угадай число» остановлен.")


# =========================================================
# /REFUND — возврат звёзд
# =========================================================

async def refund_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update):
        return

    if not context.args:
        payments = await db.last_payments(10)
        if not payments:
            await update.message.reply_text("Платежей пока нет.")
            return
        lines = [f"💸 {b('ПОСЛЕДНИЕ ПЛАТЕЖИ')}", ""]
        for p in payments:
            lines.append(
                f"⭐ {p['amount']} — {esc(p['payload'])}\n"
                f"👤 <code>{p['user_id']}</code>\n"
                f"<code>/refund {esc(p['charge_id'])}</code>\n"
            )
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return

    charge_id = context.args[0]
    payment = await db.get_payment(charge_id)
    if not payment:
        await update.message.reply_text("❌ Платёж не найден.")
        return

    try:
        await context.bot.refund_star_payment(int(payment["user_id"]), charge_id)
        await update.message.reply_text("✅ Звёзды возвращены.")
    except TelegramError as e:
        await update.message.reply_text(
            f"❌ Возврат не удался:\n<code>{esc(e)}</code>", parse_mode=ParseMode.HTML
        )


# =========================================================
# ПРОВЕРКА ДОСТУПА
# =========================================================

async def access_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return

    # Личка открыта всем: там магазин /start и оплата.
    if chat.type == "private":
        return

    if is_admin(update):
        return

    if chat.id in allowed_chat_ids:
        _remember_last_message(update)
        return

    # Не спамим: одно предупреждение в час на чат.
    now = time.time()
    last = _denied_notice.get(chat.id, 0)
    if now - last > 3600 and update.message:
        _denied_notice[chat.id] = now
        _prune(_denied_notice, 500)
        try:
            await update.message.reply_text(ACCESS_DENIED_TEXT)
        except TelegramError:
            pass

    raise ApplicationHandlerStop


# =========================================================
# АНТИСПАМ
# =========================================================

async def _send_antispam_reminder(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, limit: int, window: float, mute_minutes: int
) -> None:
    """Периодическое напоминание о правиле. Текст, фото, Premium Emoji и
    форматирование целиком редактируются в админке (🛡 Антиспам → 📝 Напоминание)."""
    text, ents = render_template(
        str(S.get("antispam_reminder_text") or DEFAULT_ANTISPAM_REMINDER_TEXT),
        S.get("antispam_reminder_entities"),
        {
            "limit": str(limit),
            "window": str(int(window)),
            "minutes": str(mute_minutes),
            "reason": str(S.get("antispam_reason") or ANTISPAM_REASON),
        },
    )
    try:
        if S.get("antispam_reminder_photo"):
            await context.bot.send_photo(
                chat_id=chat_id, photo=S["antispam_reminder_photo"],
                caption=text, caption_entities=ents or None,
            )
        else:
            await context.bot.send_message(chat_id=chat_id, text=text, entities=ents or None)
    except TelegramError:
        log.exception("Антиспам: не удалось отправить напоминание")


async def check_antispam(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Считает сообщения участника за скользящее окно. Если лимит превышен —
    мутит участника на antispam_mute_minutes и возвращает True (сообщение
    обработано, дальше пропускать не нужно). Раз в antispam_reminder_every
    сообщений чата присылает напоминание о правиле."""
    if not S.get("antispam_enabled", True):
        return False

    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or not chat or user.is_bot:
        return False
    if is_bot_admin_user(user):
        return False

    limit = int(S.get("antispam_limit", 10))
    window = float(S.get("antispam_window", 60))
    mute_minutes = int(S.get("antispam_mute_minutes", 20))

    # ---- периодическое напоминание о правиле (раз в N сообщений чата) ----
    every = int(S.get("antispam_reminder_every", 0))
    if every > 0:
        count = _spam_reminder_counter.get(chat.id, 0) + 1
        _spam_reminder_counter[chat.id] = count
        _prune(_spam_reminder_counter, 2000)
        if count % every == 0:
            await _send_antispam_reminder(context, chat.id, limit, window, mute_minutes)

    # ---- скользящее окно сообщений участника ----
    key = (chat.id, user.id)
    now = time.time()
    times = _msg_activity.setdefault(key, [])
    times.append(now)
    cutoff = now - window
    while times and times[0] < cutoff:
        times.pop(0)
    _prune(_msg_activity, 5000)

    if len(times) <= limit:
        return False

    # ---- лимит превышен — мутим ----
    times.clear()
    until = int(now) + mute_minutes * 60
    try:
        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=user.id,
            permissions=CLOSED_PERMS,
            until_date=until,
        )
    except TelegramError:
        log.warning("Антиспам: не удалось замьютить %s в чате %s (нет прав?)", user.id, chat.id)
        return False

    try:
        # Плейсхолдеры подставляются с пересчётом entities, поэтому Premium Emoji
        # и форматирование не съезжают; {user} — кликабельное упоминание.
        text, ents = render_template(
            str(S.get("antispam_text") or DEFAULT_ANTISPAM_TEXT),
            S.get("antispam_entities"),
            {
                "user": user.full_name or (f"@{user.username}" if user.username else str(user.id)),
                "minutes": str(mute_minutes),
                "reason": str(S.get("antispam_reason") or ANTISPAM_REASON),
                "limit": str(limit),
                "window": str(int(window)),
            },
            mention_user=user,
        )
        if S.get("antispam_photo"):
            await message.reply_photo(photo=S["antispam_photo"], caption=text, caption_entities=ents or None)
        else:
            await message.reply_text(text, entities=ents or None)
    except TelegramError:
        log.exception("Антиспам: не удалось отправить сообщение о муте")

    return True


async def _handle_unspam_command(message, context: ContextTypes.DEFAULT_TYPE, text_raw: str) -> None:
    """Обрабатывает команду админа «снять спам»: снимает антиспам-мут с
    участника (по ответу на его сообщение, по @username или по ID),
    отправленную прямо в нужной группе."""
    chat = message.chat
    if chat.type not in ("group", "supergroup"):
        await message.reply_text(
            "❌ Отправь «снять спам» прямо в нужной группе — в ответ на "
            "сообщение участника, либо с его @username или ID."
        )
        return

    target_id = None
    target_name = None

    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name
    else:
        arg = text_raw[len("снять спам"):].strip().lstrip("@")
        if not arg:
            await message.reply_text(
                "❌ Укажи, кого размутить: ответь на его сообщение командой "
                "«снять спам», либо напиши «снять спам @username» или "
                "«снять спам 123456789»."
            )
            return
        if arg.isdigit():
            target_id = int(arg)
        else:
            username = _normalize_username(arg)
            if not username:
                await message.reply_text("❌ Некорректный username или ID.")
                return
            try:
                chat_obj = await context.bot.get_chat(f"@{username}")
                target_id = chat_obj.id
                target_name = getattr(chat_obj, "full_name", None) or username
            except TelegramError:
                await message.reply_text(
                    "❌ Не удалось найти пользователя по username. Попробуй "
                    "ответить на его сообщение командой «снять спам» или "
                    "укажи числовой ID."
                )
                return

    try:
        await context.bot.restrict_chat_member(
            chat_id=chat.id, user_id=target_id, permissions=OPEN_PERMS,
        )
    except TelegramError as e:
        log.exception("Не удалось снять антиспам-мут")
        await message.reply_text(
            f"❌ Не удалось снять мут.\n<code>{esc(e)}</code>", parse_mode=ParseMode.HTML,
        )
        return

    # Сбрасываем счётчик сообщений, иначе может тут же замьютить повторно.
    _msg_activity.pop((chat.id, target_id), None)

    who = f"{esc(target_name)} (<code>{target_id}</code>)" if target_name else f"<code>{target_id}</code>"
    await message.reply_text(f"✅ Мут снят: {who}", parse_mode=ParseMode.HTML)


# =========================================================
# ПЕРВЫЙ КОММЕНТАРИЙ ПОД ПОСТАМИ КАНАЛА
# =========================================================

async def channel_comment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ловит авто-пересланные из привязанного канала посты в группе
    обсуждения и один раз отвечает на них первым комментарием (жирным
    текстом и/или фото), заданным в админ-панели."""
    message = update.message
    chat = update.effective_chat
    if not message or not chat or not S.get("comment_enabled"):
        return

    # Реагируем только на авто-пересланные из канала посты, а не на
    # обычные сообщения участников группы обсуждения.
    if not getattr(message, "is_automatic_forward", False):
        return

    target_chat_id = S.get("comment_chat_id")
    if target_chat_id and chat.id != int(target_chat_id):
        return

    text = S.get("comment_text") or ""
    photo = S.get("comment_photo")
    if not text and not photo:
        return

    # Жирный шрифт гарантируем принудительно — независимо от того, как
    # админ ввёл текст в админ-панели.
    bold_entities = [MessageEntity(type=MessageEntity.BOLD, offset=0, length=_u16(text))] if text else []

    try:
        if photo:
            await context.bot.send_photo(
                chat_id=chat.id,
                photo=photo,
                caption=text or None,
                caption_entities=bold_entities or None,
                reply_to_message_id=message.message_id,
            )
        else:
            await context.bot.send_message(
                chat_id=chat.id,
                text=text,
                entities=bold_entities or None,
                reply_to_message_id=message.message_id,
            )
    except TelegramError:
        log.exception("Не удалось отправить первый комментарий к посту канала")
        stats["errors"] += 1
        await _db_inc_stat("errors")


# =========================================================
# ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ
# =========================================================

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    if not message or not user or user.is_bot:
        return

    # Розыгрыш и Лудка работают только в группах — не в личке с ботом
    # (в личке живёт только /start и магазин Stars).
    if not chat or chat.type not in ("group", "supergroup"):
        return

    stats["messages"] += 1
    await _db_inc_stat("messages")

    if await check_antispam(update, context):
        return

    if S["ludka_enabled"]:
        await process_ludka_message(update, context)

    if S["guess_enabled"]:
        await process_guess_message(update, context)

    if not S["giveaway_enabled"]:
        return

    chance = get_chance(user.id, user.username)
    if random.random() * 100 >= chance:
        return

    if user.username and _normalize_username(user.username) in blocked_usernames:
        await _send_blocked_message(message)
        return

    # Подарки — только участникам чата: остальным уходит заданный админом текст.
    if S.get("require_member") and not await _is_chat_member(context.bot, chat.id, user.id):
        await _reply_nonmember(message, user)
        return

    stats["wins"] += 1
    await _db_inc_stat("wins")

    if await give_gift(update, context):
        await _bump_winner(user)
        try:
            if S["win_success_photo"]:
                await message.reply_photo(
                    photo=S["win_success_photo"],
                    caption=S["win_success_text"],
                    caption_entities=S["win_success_entities"] or None,
                )
            else:
                await message.reply_text(
                    S["win_success_text"], entities=S["win_success_entities"] or None
                )
        except Exception:
            log.exception("Ошибка отправки текста удачной выдачи")
            stats["errors"] += 1
            await _db_inc_stat("errors")
        return

    # Подарок не ушёл — отправляем запасное сообщение и кладём победителя
    # в список невыданных подарков, чтобы админ мог выдать/отклонить вручную.
    await _add_pending(user, chat.id, message.message_id, S["selected_gift_id"])
    try:
        if S["win_photo"]:
            await message.reply_photo(
                photo=S["win_photo"],
                caption=S["win_text"],
                caption_entities=S["win_entities"] or None,
            )
        else:
            await message.reply_text(
                S["win_text"], entities=S["win_entities"] or None
            )
    except Exception:
        log.exception("Ошибка отправки запасного сообщения")
        stats["errors"] += 1
        await _db_inc_stat("errors")


# =========================================================
# ЛУДКА 777 — ИГРОВОЙ ПРОЦЕСС
# =========================================================

async def process_ludka_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if not user or not message:
        return

    count = ludka_progress.get(user.id, 0) + 1
    ludka_progress[user.id] = count
    _prune(ludka_progress)

    if count < int(S["ludka_price"]):
        return

    ludka_progress[user.id] = 0

    reels = [random.randint(1, 7) for _ in range(3)]
    result = " | ".join(str(x) for x in reels)

    if reels == [7, 7, 7]:
        await message.reply_text(
            f"🎰 {b('777! ДЖЕКПОТ!')}\n\n"
            f"👤 {user.mention_html()}\n"
            f"🎰 {result}",
            parse_mode=ParseMode.HTML,
        )
        await message.reply_text(
            S["ludka_prize"], entities=S["ludka_prize_entities"] or None
        )
    else:
        await message.reply_text(
            f"🎰 {result}\n"
            f"😔 Не повезло. Нужны три семёрки!\n"
            f"💰 Цена вращения: {S['ludka_price']} соо"
        )


# =========================================================
# ОБРАБОТКА ОШИБОК
# =========================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("Необработанная ошибка", exc_info=context.error)
    stats["errors"] += 1
    try:
        await _db_inc_stat("errors")
    except Exception:
        pass


# =========================================================
# ЖИЗНЕННЫЙ ЦИКЛ
# =========================================================

async def post_init(application) -> None:
    await db.init_db()
    await load_persistent_state()
    await restore_closed_chats(application)
    _spawn(keepalive_loop(application))
    _spawn(leaderboard_loop(application))
    log.info("Бот инициализирован")


async def post_shutdown(application) -> None:
    for task in list(_tasks):
        task.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)

    await gift_account.close()
    await _save_known_users()
    await db.close_db()

    if _http_server is not None:
        try:
            _http_server.shutdown()
            _http_server.server_close()
        except Exception:
            pass
    log.info("Бот остановлен корректно")


def main() -> None:
    threading.Thread(target=run_web_server, daemon=True).start()

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # --- !сумрак / !сумракофф (раньше всех, проверяет права и чат сам) ---
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, sumrak_handler), group=-2
    )

    # --- ограничение по чатам (выполняется раньше всего) ---
    app.add_handler(MessageHandler(filters.ALL, access_guard), group=-1)

    # --- команды ---
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CommandHandler("chance", chance_command))
    app.add_handler(CommandHandler("userchance", userchance_command))
    app.add_handler(CommandHandler("botadmin", botadmin_command))
    app.add_handler(CommandHandler("botadminoff", botadminoff_command))
    app.add_handler(CommandHandler("botadmins", botadmins_command))
    app.add_handler(CommandHandler("bold", bold_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("ludka", ludka_command))
    app.add_handler(CommandHandler("ludkaoff", ludkaoff_command))
    app.add_handler(CommandHandler("guess", guess_command))
    app.add_handler(CommandHandler("guessoff", guessoff_command))
    app.add_handler(CommandHandler("refund", refund_command))

    # --- платежи ---
    app.add_handler(PreCheckoutQueryHandler(precheckout_handler))
    app.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler)
    )

    # --- callback-кнопки ---
    app.add_handler(CallbackQueryHandler(buy_callback, pattern=r"^buy:"))
    app.add_handler(CallbackQueryHandler(select_gift, pattern=r"^gift:"))
    app.add_handler(CallbackQueryHandler(admin_callback))

    # --- ввод админа ---
    app.add_handler(
        MessageHandler(
            filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL
            | (filters.TEXT & ~filters.COMMAND),
            admin_content_handler,
        ),
        group=0,
    )

    # --- обычные сообщения (учитываются в розыгрыше и Лудке любые типы
    #     сообщений — текст, фото, стикеры и т.д., кроме команд и служебных) ---
    app.add_handler(
        MessageHandler(
            filters.ALL
            & ~filters.COMMAND
            & ~filters.StatusUpdate.ALL
            & ~filters.SUCCESSFUL_PAYMENT,
            message_handler,
        ),
        group=1,
    )

    # --- первый комментарий под авто-пересланными постами канала ---
    # Отдельная группа (2), т.к. в группе 1 уже стоит message_handler с
    # широким фильтром — в PTB в пределах одной группы отрабатывает только
    # первый подошедший обработчик. Фильтрация на "это именно пересланный
    # пост канала" делается внутри самого channel_comment_handler
    # (message.is_automatic_forward), чтобы не зависеть от конкретного
    # имени готового фильтра в разных версиях библиотеки.
    app.add_handler(
        MessageHandler(filters.ChatType.SUPERGROUP & ~filters.COMMAND, channel_comment_handler),
        group=2,
    )

    app.add_error_handler(error_handler)

    log.info("🎁 Telegram Gift Bot запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
