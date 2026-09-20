
import os
import json
import asyncio
import logging

from cryptography.fernet import Fernet, InvalidToken
from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    PasswordHashInvalidError,
)

import database as db

log = logging.getLogger(__name__)

SETTING_KEY = "mtproto_account"
_client = None
_client_lock = asyncio.Lock()
_pending_client = None


def _fernet():
    key = os.environ.get("SESSION_ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "SESSION_ENCRYPTION_KEY is not set. Generate a Fernet key and add it to Render."
        )
    return Fernet(key.encode())


def _encrypt(data: dict) -> str:
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return _fernet().encrypt(raw).decode("utf-8")


def _decrypt(token: str) -> dict:
    try:
        raw = _fernet().decrypt(token.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except (InvalidToken, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("Не удалось расшифровать MTProto-сессию. Проверь SESSION_ENCRYPTION_KEY.") from exc


async def save_account(api_id: int, api_hash: str, session_string: str, me) -> None:
    payload = {
        "api_id": int(api_id),
        "api_hash": api_hash,
        "session": session_string,
        "user_id": int(me.id),
        "username": me.username or "",
        "first_name": me.first_name or "",
        "last_name": me.last_name or "",
        "phone": getattr(me, "phone", "") or "",
    }
    await db.set_setting(SETTING_KEY, _encrypt(payload))


async def load_account():
    token = await db.get_setting(SETTING_KEY, None)
    if not token:
        return None
    return _decrypt(token)


async def clear_account():
    global _client
    if _client is not None:
        try:
            await _client.disconnect()
        except Exception:
            pass
        _client = None
    await db.delete_setting(SETTING_KEY)


async def get_client():
    global _client

    async with _client_lock:
        if _client is not None and _client.is_connected():
            return _client

        account = await load_account()
        if not account:
            return None

        client = TelegramClient(
            StringSession(account["session"]),
            int(account["api_id"]),
            account["api_hash"],
        )
        await client.connect()

        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("MTProto-сессия больше не авторизована. Привяжи аккаунт заново.")

        _client = client
        return _client


async def account_status():
    account = await load_account()
    if not account:
        return {"connected": False}

    try:
        client = await get_client()
        me = await client.get_me()
        status = await client(functions.payments.GetStarsStatusRequest(
            peer=types.InputPeerSelf()
        ))
        return {
            "connected": True,
            "user_id": int(me.id),
            "username": me.username or "",
            "name": " ".join(x for x in [me.first_name, me.last_name] if x) or "Без имени",
            "balance": int(status.balance.amount),
            "phone": getattr(me, "phone", "") or account.get("phone", ""),
        }
    except Exception as exc:
        log.exception("MTProto account status error")
        return {
            "connected": True,
            "user_id": account.get("user_id"),
            "username": account.get("username", ""),
            "name": " ".join(x for x in [account.get("first_name"), account.get("last_name")] if x) or "Аккаунт",
            "balance": None,
            "phone": account.get("phone", ""),
            "error": str(exc),
        }


async def start_login(api_id: int, api_hash: str, phone: str):
    global _pending_client

    if _pending_client is not None:
        try:
            await _pending_client.disconnect()
        except Exception:
            pass
        _pending_client = None

    client = TelegramClient(StringSession(), int(api_id), api_hash)
    await client.connect()

    attach_pending_meta(client, api_id, api_hash)
    sent = await client.send_code_request(phone)
    _pending_client = client
    return sent.phone_code_hash


async def finish_login(phone: str, code: str, phone_code_hash: str):
    global _pending_client

    if _pending_client is None:
        raise RuntimeError("Сессия авторизации истекла. Нажми «Привязать аккаунт» заново.")

    try:
        me = await _pending_client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=phone_code_hash,
        )
    except SessionPasswordNeededError:
        return {"need_password": True}
    except PhoneCodeInvalidError:
        raise RuntimeError("Неверный код Telegram.")
    except Exception:
        raise

    await _save_pending(me)
    return {"need_password": False, "me": me}


async def finish_login_password(password: str):
    global _pending_client

    if _pending_client is None:
        raise RuntimeError("Сессия авторизации истекла. Нажми «Привязать аккаунт» заново.")

    try:
        me = await _pending_client.sign_in(password=password)
    except PasswordHashInvalidError:
        raise RuntimeError("Неверный пароль двухэтапной аутентификации.")
    await _save_pending(me)
    return me


async def _save_pending(me):
    global _pending_client

    account = await load_account()
    # Эти значения передаются отдельно из pending_login_state через configure_pending().
    pending = getattr(_pending_client, "_richlife_pending_meta", None)
    if not pending:
        raise RuntimeError("Не найдены данные авторизации.")

    await save_account(
        pending["api_id"],
        pending["api_hash"],
        _pending_client.session.save(),
        me,
    )

    try:
        await _pending_client.disconnect()
    finally:
        _pending_client = None


def attach_pending_meta(client, api_id, api_hash):
    client._richlife_pending_meta = {
        "api_id": int(api_id),
        "api_hash": api_hash,
    }


async def abort_login():
    global _pending_client
    if _pending_client is not None:
        try:
            await _pending_client.disconnect()
        except Exception:
            pass
        _pending_client = None


async def get_balance():
    client = await get_client()
    if client is None:
        return None
    status = await client(functions.payments.GetStarsStatusRequest(
        peer=types.InputPeerSelf()
    ))
    return int(status.balance.amount)


async def send_gift(recipient, gift_id: int, message: str = ""):
    """
    Buy a regular Telegram Star Gift from the linked user account and send it
    directly to recipient. The payment is performed by the MTProto user account.
    """
    client = await get_client()
    if client is None:
        raise RuntimeError("Аккаунт выдачи не привязан.")

    peer = await client.get_input_entity(recipient)

    invoice = types.InputInvoiceStarGift(
        peer=peer,
        gift_id=int(gift_id),
        message=types.TextWithEntities(
            text=message or "",
            entities=[],
        ),
    )

    form = await client(functions.payments.GetPaymentFormRequest(
        invoice=invoice
    ))

    result = await client(functions.payments.SendStarsFormRequest(
        form_id=form.form_id,
        invoice=invoice,
    ))
    return result
