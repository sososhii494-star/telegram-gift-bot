"""
Привязка пользовательского Telegram-аккаунта (api_id / api_hash) через Telethon.

Зачем это нужно:
  Bot API НЕ умеет покупать обычные Telegram-подарки со Stars-баланса бота
  для произвольного пользователя. Поэтому подарок покупает и отправляет
  привязанный пользовательский аккаунт через MTProto.

Что делает модуль:
  * пошаговый вход (api_id -> api_hash -> телефон -> код -> 2FA-пароль)
  * хранит StringSession в БД в зашифрованном виде (Fernet)
  * отдаёт баланс Stars, список подарков и отправку подарка победителю
  * корректно закрывает клиентов — незавершённый вход не оставляет
    висящее TCP-соединение (это была одна из утечек)
"""

import asyncio
import base64
import hashlib
import logging
import os
from typing import Any, Dict, List, Optional

import db

log = logging.getLogger(__name__)

SESSION_KEY = "mtproto_session"
API_ID_KEY = "mtproto_api_id"
API_HASH_KEY = "mtproto_api_hash"

_client = None                      # TelegramClient активного аккаунта
_login: Dict[str, Any] = {}         # временное состояние входа
_lock = asyncio.Lock()


# =========================================================
# ШИФРОВАНИЕ СЕССИИ
# =========================================================

def _fernet():
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        return None
    secret = os.environ.get("SESSION_SECRET") or os.environ.get("BOT_TOKEN", "fallback")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
    return Fernet(key)


def _encrypt(raw: str) -> str:
    f = _fernet()
    if f is None:
        log.warning("cryptography не установлена — сессия хранится в открытом виде")
        return "plain:" + raw
    return "enc:" + f.encrypt(raw.encode()).decode()


def _decrypt(stored: str) -> Optional[str]:
    if not stored:
        return None
    if stored.startswith("plain:"):
        return stored[6:]
    if stored.startswith("enc:"):
        f = _fernet()
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

async def get_client():
    """Возвращает подключённый TelegramClient или None, если аккаунт не привязан."""
    global _client

    async with _lock:
        if _client is not None:
            try:
                if not _client.is_connected():
                    await _client.connect()
                if await _client.is_user_authorized():
                    return _client
            except Exception:
                log.exception("Активный MTProto-клиент сломался, пересоздаю")
            await _safe_disconnect(_client)
            _client = None

        raw = await db.get_setting(SESSION_KEY, None)
        session_str = _decrypt(raw) if raw else None
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
            await _safe_disconnect(client)
            return None

        _client = client
        return _client


async def _safe_disconnect(client) -> None:
    if client is None:
        return
    try:
        result = client.disconnect()
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        pass


async def close() -> None:
    """Вызывается при остановке бота."""
    global _client
    await abort_login()
    await _safe_disconnect(_client)
    _client = None


# =========================================================
# ВХОД
# =========================================================

async def start_login(api_id: int, api_hash: str, phone: str) -> str:
    """Шаг 1: отправляет код подтверждения. Возвращает phone_code_hash."""
    await abort_login()

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.connect()
    sent = await client.send_code_request(phone)

    _login.update(
        client=client,
        api_id=int(api_id),
        api_hash=api_hash,
        phone=phone,
        phone_code_hash=sent.phone_code_hash,
    )
    return sent.phone_code_hash


async def finish_login(phone: str, code: str, phone_code_hash: str) -> Dict[str, Any]:
    """Шаг 2: вход по коду. Может вернуть {'need_password': True}."""
    client = _login.get("client")
    if client is None:
        raise RuntimeError("Сессия входа истекла. Начни привязку заново.")

    from telethon.errors import SessionPasswordNeededError

    try:
        me = await client.sign_in(
            phone=phone, code=code, phone_code_hash=phone_code_hash
        )
    except SessionPasswordNeededError:
        return {"need_password": True}

    await _persist_login()
    return {"need_password": False, "me": me}


async def finish_login_password(password: str):
    """Шаг 3 (если включена двухэтапная аутентификация)."""
    client = _login.get("client")
    if client is None:
        raise RuntimeError("Сессия входа истекла. Начни привязку заново.")

    me = await client.sign_in(password=password)
    await _persist_login()
    return me


async def _persist_login() -> None:
    """Сохраняет сессию в БД и делает клиента активным."""
    global _client

    from telethon.sessions import StringSession

    client = _login.pop("client", None)
    if client is None:
        return

    session_str = StringSession.save(client.session)
    await db.set_setting(SESSION_KEY, _encrypt(session_str))
    await db.set_setting(API_ID_KEY, str(_login.get("api_id", 0)))
    await db.set_setting(API_HASH_KEY, _login.get("api_hash", ""))
    _login.clear()

    await _safe_disconnect(_client)
    _client = client


async def abort_login() -> None:
    """Отмена привязки: закрывает временного клиента (иначе утечка соединения)."""
    client = _login.pop("client", None)
    await _safe_disconnect(client)
    _login.clear()


async def clear_account() -> None:
    """Отвязка аккаунта."""
    global _client
    await abort_login()
    if _client is not None:
        try:
            await _client.log_out()
        except Exception:
            log.exception("Не удалось выполнить log_out")
        await _safe_disconnect(_client)
        _client = None
    await db.set_setting(SESSION_KEY, "")
    await db.set_setting(API_ID_KEY, "")
    await db.set_setting(API_HASH_KEY, "")


# =========================================================
# СТАТУС И БАЛАНС
# =========================================================

async def get_balance() -> Optional[int]:
    client = await get_client()
    if client is None:
        return None

    from telethon.tl import functions, types

    status = await client(
        functions.payments.GetStarsStatusRequest(peer=types.InputPeerSelf())
    )
    balance = getattr(status, "balance", None)
    # В новых слоях balance — это StarsAmount с полем amount
    return int(getattr(balance, "amount", balance) or 0)


async def account_status() -> Dict[str, Any]:
    client = await get_client()
    if client is None:
        return {"connected": False}

    me = await client.get_me()
    try:
        balance = await get_balance()
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

async def list_gifts() -> List[Dict[str, Any]]:
    """Список доступных обычных подарков глазами привязанного аккаунта."""
    client = await get_client()
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


async def _resolve_peer(client, recipient):
    """
    recipient может быть '@username' или числовым user_id.
    Для user_id аккаунт должен «видеть» пользователя — иначе Telegram
    вернёт PEER_ID_INVALID, и мы честно сообщим об этом.
    """
    try:
        return await client.get_input_entity(recipient)
    except Exception:
        if isinstance(recipient, str) and recipient.lstrip("-").isdigit():
            return await client.get_input_entity(int(recipient))
        raise


def _build_invoice(peer, gift_id: int, message: Optional[str]):
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


async def send_gift(recipient, gift_id: int, message: Optional[str] = None):
    """Покупает и отправляет подарок победителю со Stars привязанного аккаунта."""
    client = await get_client()
    if client is None:
        raise RuntimeError("Аккаунт выдачи не привязан")

    from telethon.tl import functions

    peer = await _resolve_peer(client, recipient)
    invoice = _build_invoice(peer, int(gift_id), message)

    form = await client(functions.payments.GetPaymentFormRequest(invoice=invoice))
    return await client(
        functions.payments.SendStarsFormRequest(form_id=form.form_id, invoice=invoice)
    )
