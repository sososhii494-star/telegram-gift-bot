import os
import random
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    ApplicationHandlerStop,
    filters,
)


# =========================================================
# НАСТРОЙКИ
# =========================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

TOKEN = os.environ["BOT_TOKEN"]

PORT = int(os.environ.get("PORT", 10000))

DEFAULT_CHANCE = float(
    os.environ.get("GIFT_CHANCE", "1")
)

# ТВОЙ TELEGRAM ID
ADMIN_ID = 7491572487


# =========================================================
# НАСТРОЙКИ СООБЩЕНИЯ ПОБЕДИТЕЛЯ
# =========================================================

DEFAULT_WIN_TEXT = (
    "🎉 Ты выиграл!\n\n"
    "Но сейчас Telegram не позволил отправить подарок. "
    "Администратор проверит ситуацию."
)

# Текст
win_text = DEFAULT_WIN_TEXT

# Фото file_id
win_photo = None

# Premium / Custom Emoji и другое форматирование Telegram
win_entities = None


# =========================================================
# СОСТОЯНИЕ БОТА
# =========================================================

selected_gift_id = None

giveaway_enabled = True

stats = {
    "messages": 0,
    "wins": 0,
    "gifts_sent": 0,
    "errors": 0,
}

# =========================================================
# ДОСТУП БОТА — МАКСИМУМ 2 ЧАТА
# =========================================================

allowed_chat_ids = set()
ACCESS_DENIED_TEXT = (
    "🚫 БОТ НЕ РАБОТАЕТ ТУТ БРАТ\n\n"
    "ДОСТУП ПРИОБРЕТИ ТУТ @POLYSYMRAK"
)

# =========================================================
# НАСТРОЙКИ ЛУДКИ 777
# =========================================================

ludka_enabled = False

# Сколько сообщений пользователя нужно для одного вращения
ludka_price = 1

# Текст приза — можно менять через админку
ludka_prize = "подарок какой то"
ludka_prize_entities = []

# Сообщение, которое бот публикует при запуске лудки
ludka_text = (
    "🎰 Лудка 777 запущена!\n"
    "🎁 Приз: подарок какой то\n"
    "💰 Цена 1 соо: 1"
)

# Фото + Telegram entities для сообщения запуска
ludka_photo = None
ludka_entities = None

# Счётчик сообщений каждого участника в текущем раунде
ludka_progress = {}
ludka_chat_id = None


# =========================================================
# HTTP SERVER ДЛЯ RENDER
# =========================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(
            b"Telegram Gift Bot is running!"
        )

    def log_message(self, format, *args):
        pass


def run_web_server():
    server = HTTPServer(
        ("0.0.0.0", PORT),
        HealthHandler
    )

    server.serve_forever()


# =========================================================
# ПРОВЕРКА АДМИНА
# =========================================================

async def is_admin(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_user:
        return False

    return update.effective_user.id == ADMIN_ID


# =========================================================
# ШАНС
# =========================================================


def _entities_to_json(entities):
    if not entities:
        return "[]"
    return json.dumps([e.to_dict() for e in entities], ensure_ascii=False)


def _entities_from_json(raw):
    if not raw:
        return []
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        result = []
        for item in data or []:
            # MessageEntity fields used by Telegram/PTB.
            kwargs = {
                "type": item.get("type"),
                "offset": item.get("offset", 0),
                "length": item.get("length", 0),
            }
            for key in ("url", "language", "custom_emoji_id"):
                if item.get(key) is not None:
                    kwargs[key] = item[key]
            result.append(MessageEntity(**kwargs))
        return result
    except Exception:
        logging.exception("Failed to restore message entities")
        return []


async def load_persistent_state():
    """Load all admin settings from PostgreSQL into the bot's runtime state."""
    global selected_gift_id, giveaway_enabled, allowed_chat_ids
    global ludka_enabled, ludka_price, ludka_prize, ludka_prize_entities
    global ludka_text, ludka_photo, ludka_entities, ludka_chat_id
    global win_text, win_photo, win_entities, stats, DEFAULT_CHANCE

    DEFAULT_CHANCE = await db.get_float_setting("chance", DEFAULT_CHANCE)
    selected_gift_id = await db.get_setting("selected_gift_id", None)
    giveaway_enabled = await db.get_bool_setting("giveaway_enabled", giveaway_enabled)

    allowed_chat_ids.clear()
    allowed_chat_ids.update(await db.load_allowed_chats())

    ludka_enabled = await db.get_bool_setting("ludka_enabled", ludka_enabled)
    ludka_price = await db.get_int_setting("ludka_price", ludka_price)
    ludka_prize = await db.get_setting("ludka_prize", ludka_prize)
    ludka_prize_entities = _entities_from_json(
        await db.get_setting("ludka_prize_entities", "[]")
    )
    ludka_text = await db.get_setting("ludka_text", ludka_text)
    ludka_photo = await db.get_setting("ludka_photo", ludka_photo)
    ludka_entities = _entities_from_json(
        await db.get_setting("ludka_entities", "[]")
    )
    ludka_chat_id = await db.get_int_setting("ludka_chat_id", 0) or None

    win_text = await db.get_setting("win_text", win_text)
    win_photo = await db.get_setting("win_photo", win_photo)
    win_entities = _entities_from_json(
        await db.get_setting("win_entities", "[]")
    )

    loaded_stats = await db.load_stats()
    stats.update(loaded_stats)

    logging.info(
        "Persistent state loaded: chance=%s, giveaway=%s, gift=%s, chats=%s",
        DEFAULT_CHANCE, giveaway_enabled, selected_gift_id, len(allowed_chat_ids)
    )


async def _db_set(key, value):
    try:
        await db.set_setting(key, str(value) if value is not None else "")
    except Exception:
        logging.exception("Failed to save setting: %s", key)


async def _db_set_entities(key, entities):
    try:
        await db.set_setting(key, _entities_to_json(entities))
    except Exception:
        logging.exception("Failed to save entities: %s", key)


async def _db_inc_stat(name, amount=1):
    try:
        await db.increment_stat(name, amount)
    except Exception:
        logging.exception("Failed to save statistic: %s", name)


def get_chance(context):

    value = context.chat_data.get(
        "chance",
        DEFAULT_CHANCE
    )

    return float(value)


# =========================================================
# КЛАВИАТУРА АДМИНКИ
# =========================================================

def admin_keyboard():

    status = (
        "🟢 ВКЛЮЧЕН"
        if giveaway_enabled
        else
        "🔴 ВЫКЛЮЧЕН"
    )

    return InlineKeyboardMarkup([

        [
            InlineKeyboardButton(
                "🎯 Шанс",
                callback_data="chance"
            ),

            InlineKeyboardButton(
                "🎁 Подарки",
                callback_data="gifts"
            ),
        ],

        [
            InlineKeyboardButton(
                "💰 Stars",
                callback_data="balance"
            ),

            InlineKeyboardButton(
                "📊 Статистика",
                callback_data="stats"
            ),
        ],

        [
            InlineKeyboardButton(
                f"🎲 Розыгрыш: {status}",
                callback_data="toggle"
            ),
        ],

        [
            InlineKeyboardButton(
                "🎰 Лудка 777",
                callback_data="ludka"
            ),
        ],

        [
            InlineKeyboardButton(
                "✏️ Сообщение победителя",
                callback_data="winmessage"
            ),
        ],

        [
            InlineKeyboardButton(
                "🔐 Доступные чаты (2)",
                callback_data="access"
            ),
        ],

        [
            InlineKeyboardButton(
                "🔄 Обновить подарки",
                callback_data="refresh"
            ),
        ],
    ])


# =========================================================
# /ADMIN
# =========================================================

async def admin_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not await is_admin(update, context):

        await update.message.reply_text(
            "❌ Админ-панель доступна только владельцу бота."
        )

        return

    await update.message.reply_text(

        "🛠 **АДМИН-ПАНЕЛЬ**\n\n"

        "Здесь можно управлять:\n"
        "🎯 шансом\n"
        "🎁 подарками\n"
        "💰 Stars\n"
        "📊 статистикой\n"
        "✏️ сообщением победителя\n\n"

        "Выбери действие ниже.",

        reply_markup=admin_keyboard(),

        parse_mode="Markdown"
    )


# =========================================================
# CALLBACK АДМИНКИ
# =========================================================

async def admin_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global giveaway_enabled
    global selected_gift_id

    query = update.callback_query

    if not await is_admin(update, context):

        await query.answer(
            "❌ Только для администратора.",
            show_alert=True
        )

        return

    await query.answer()

    data = query.data


    # -----------------------------------------------------
    # ГЛАВНОЕ МЕНЮ
    # -----------------------------------------------------

    if data == "main":

        await query.edit_message_text(

            "🛠 **АДМИН-ПАНЕЛЬ**\n\n"
            "Выбери действие:",

            reply_markup=admin_keyboard(),

            parse_mode="Markdown"
        )

        return


    # -----------------------------------------------------
    # НАСТРОЙКА ШАНСА
    # -----------------------------------------------------

    if data == "chance":

        current = get_chance(context)

        keyboard = InlineKeyboardMarkup([

            [
                InlineKeyboardButton(
                    "0.1%",
                    callback_data="setchance:0.1"
                ),

                InlineKeyboardButton(
                    "0.5%",
                    callback_data="setchance:0.5"
                ),
            ],

            [
                InlineKeyboardButton(
                    "1%",
                    callback_data="setchance:1"
                ),

                InlineKeyboardButton(
                    "2%",
                    callback_data="setchance:2"
                ),
            ],

            [
                InlineKeyboardButton(
                    "5%",
                    callback_data="setchance:5"
                ),

                InlineKeyboardButton(
                    "10%",
                    callback_data="setchance:10"
                ),
            ],

            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data="main"
                ),
            ],
        ])

        await query.edit_message_text(

            f"🎯 **НАСТРОЙКА ШАНСА**\n\n"
            f"Сейчас: **{current}%**\n\n"
            "Можно выбрать готовый вариант ниже.\n\n"
            "Или использовать команду:\n"
            "`/chance 0.5`\n"
            "`/chance 1`\n"
            "`/chance 10`",

            reply_markup=keyboard,

            parse_mode="Markdown"
        )

        return


    # -----------------------------------------------------
    # УСТАНОВКА ШАНСА
    # -----------------------------------------------------

    if data.startswith("setchance:"):

        value = float(
            data.split(":", 1)[1]
        )

        global DEFAULT_CHANCE
        DEFAULT_CHANCE = value
        await _db_set("chance", value)

        await query.edit_message_text(

            f"✅ **Шанс установлен: {value}%**",

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ В админ-панель",
                        callback_data="main"
                    )
                ]

            ]),

            parse_mode="Markdown"
        )

        return


    # -----------------------------------------------------
    # ПОДАРКИ
    # -----------------------------------------------------

    if data == "gifts":

        await show_gifts(
            query,
            context
        )

        return


    # -----------------------------------------------------
    # STARS
    # -----------------------------------------------------

    if data == "balance":

        try:

            balance = await context.bot.get_my_star_balance()

            amount = balance.amount

            await query.edit_message_text(

                "💰 **БАЛАНС БОТА**\n\n"
                f"⭐ Stars: **{amount}**",

                reply_markup=InlineKeyboardMarkup([

                    [
                        InlineKeyboardButton(
                            "⬅️ Назад",
                            callback_data="main"
                        )
                    ]

                ]),

                parse_mode="Markdown"
            )

        except Exception as e:

            logging.exception(
                "Ошибка получения баланса"
            )

            await query.edit_message_text(

                "❌ Не удалось получить баланс Stars.\n\n"
                f"Ошибка: `{e}`",

                reply_markup=InlineKeyboardMarkup([

                    [
                        InlineKeyboardButton(
                            "⬅️ Назад",
                            callback_data="main"
                        )
                    ]

                ]),

                parse_mode="Markdown"
            )

        return


    # -----------------------------------------------------
    # СТАТИСТИКА
    # -----------------------------------------------------

    if data == "stats":

        current = get_chance(context)

        text = (

            "📊 **СТАТИСТИКА БОТА**\n\n"

            f"💬 Сообщений: **{stats['messages']}**\n"
            f"🎯 Срабатываний: **{stats['wins']}**\n"
            f"🎁 Подарков отправлено: **{stats['gifts_sent']}**\n"
            f"❌ Ошибок: **{stats['errors']}**\n\n"

            f"🎯 Текущий шанс: **{current}%**\n"

            f"🎁 Выбранный подарок: "
            f"**{selected_gift_id or 'Авто'}**"
        )

        await query.edit_message_text(

            text,

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="main"
                    )
                ]

            ]),

            parse_mode="Markdown"
        )

        return


    # -----------------------------------------------------
    # ВКЛ / ВЫКЛ РОЗЫГРЫШ
    # -----------------------------------------------------

    if data == "toggle":

        giveaway_enabled = not giveaway_enabled
        await _db_set("giveaway_enabled", giveaway_enabled)

        status = (
            "🟢 ВКЛЮЧЕН"
            if giveaway_enabled
            else
            "🔴 ВЫКЛЮЧЕН"
        )

        await query.edit_message_text(

            f"🎲 **Розыгрыш {status}**",

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ В админ-панель",
                        callback_data="main"
                    )
                ]

            ]),

            parse_mode="Markdown"
        )

        return


    # -----------------------------------------------------
    # ДОСТУПНЫЕ ЧАТЫ
    # -----------------------------------------------------

    if data == "access":
        await show_access_menu(query)
        return

    if data == "access_add_current":
        chat = query.message.chat if query.message else None
        if not chat or chat.type not in ("group", "supergroup"):
            await query.answer(
                "Открой /admin прямо в нужной группе, чтобы добавить её.",
                show_alert=True
            )
            return
        if chat.id not in allowed_chat_ids and len(allowed_chat_ids) >= 2:
            await query.answer("❌ Уже добавлены 2 чата. Сначала удали один.", show_alert=True)
            return
        allowed_chat_ids.add(chat.id)
        await db.add_allowed_chat(chat.id)
        await query.answer("✅ Чат добавлен")
        await show_access_menu(query)
        return

    if data == "access_add_username":
        context.user_data["waiting_access_chat"] = True
        await query.edit_message_text(
            "➕ **ДОБАВЛЕНИЕ ЧАТА**\n\n"
            "Отправь @username группы или её chat ID.\n\n"
            "Примеры:\n`@mygroup`\n`-1001234567890`\n\n"
            "❌ /cancel — отменить.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="access")]
            ]),
            parse_mode="Markdown"
        )
        return

    if data.startswith("access_remove:"):
        try:
            chat_id = int(data.split(":", 1)[1])
            allowed_chat_ids.discard(chat_id)
            await db.remove_allowed_chat(chat_id)
            await query.answer("🗑 Чат удалён")
        except ValueError:
            await query.answer("❌ Неверный chat ID", show_alert=True)
        await show_access_menu(query)
        return

    if data == "access_clear":
        allowed_chat_ids.clear()
        await db.clear_allowed_chats()
        await query.answer("🧹 Список очищен")
        await show_access_menu(query)
        return

    # -----------------------------------------------------
    # ОБНОВИТЬ ПОДАРКИ
    # -----------------------------------------------------

    if data == "refresh":

        await show_gifts(
            query,
            context
        )

        return


    # -----------------------------------------------------
    # ЛУДКА 777
    # -----------------------------------------------------

    if data == "ludka":
        await show_ludka_menu(query)
        return

    if data == "ludka_price":
        context.user_data["waiting_ludka_price"] = True
        await query.edit_message_text(
            "💰 **ЦЕНА ЛУДКИ 777**\n\n"
            "Напиши количество сообщений, которое нужно отправить "
            "для одного вращения.\n\n"
            "Например: `1`, `5`, `10`\n\n"
            "❌ `/cancel` — отменить.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="ludka")]
            ]),
            parse_mode="Markdown"
        )
        return

    if data == "ludka_prize":
        context.user_data["waiting_ludka_prize"] = True
        await query.edit_message_text(
            "🎁 **ПРИЗ ЛУДКИ 777**\n\n"
            "Отправь текст приза. Можно использовать Premium/Custom Emoji — "
            "Telegram-форматирование сохранится.\n\n"
            "Например:\n"
            "`🎁 Подарок какой то`\n\n"
            "❌ `/cancel` — отменить.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="ludka")]
            ]),
            parse_mode="Markdown"
        )
        return

    if data == "ludka_message":
        context.user_data["waiting_ludka_message"] = True
        await query.edit_message_text(
            "📝 **СООБЩЕНИЕ ЛУДКИ 777**\n\n"
            "Отправь текст или фотографию с подписью.\n"
            "Premium/Custom Emoji сохраняются.\n\n"
            "Это сообщение будет публиковаться при запуске лудки.\n\n"
            "❌ `/cancel` — отменить.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="ludka")]
            ]),
            parse_mode="Markdown"
        )
        return

    if data == "ludka_launch":
        await launch_ludka(query, context)
        return

    if data == "ludka_stop":
        await stop_ludka(query, context)
        return

    # -----------------------------------------------------
    # НАСТРОЙКА СООБЩЕНИЯ ПОБЕДИТЕЛЯ
    # -----------------------------------------------------

    if data == "winmessage":

        context.user_data[
            "waiting_win_message"
        ] = True

        await query.edit_message_text(

            "✏️ **НАСТРОЙКА СООБЩЕНИЯ**\n\n"

            "Теперь отправь мне сообщение одним из способов:\n\n"

            "📝 **Только текст**\n"
            "→ изменится текст\n\n"

            "📷 **Фотография + подпись**\n"
            "→ бот будет отправлять фото и текст\n\n"

            "✨ **Premium Emoji**\n"
            "→ просто вставь Premium/Custom Emoji "
            "прямо в текст — Telegram-сущность сохранится.\n\n"

            "❌ `/cancel` — отменить настройку.",

            parse_mode="Markdown"
        )

        return


# =========================================================
# ДОСТУПНЫЕ ЧАТЫ — АДМИНКА
# =========================================================

def access_menu_keyboard(current_chat=None):
    buttons = []
    if current_chat and current_chat.type in ("group", "supergroup"):
        if current_chat.id not in allowed_chat_ids or len(allowed_chat_ids) < 2:
            buttons.append([InlineKeyboardButton("➕ Привязать этот чат", callback_data="access_add_current")])
    buttons.append([InlineKeyboardButton("➕ По @username / ID", callback_data="access_add_username")])
    for chat_id in list(allowed_chat_ids)[:2]:
        buttons.append([InlineKeyboardButton(f"🗑 Удалить {chat_id}", callback_data=f"access_remove:{chat_id}")])
    if allowed_chat_ids:
        buttons.append([InlineKeyboardButton("🧹 Очистить всё", callback_data="access_clear")])
    buttons.append([InlineKeyboardButton("⬅️ Админ-панель", callback_data="main")])
    return InlineKeyboardMarkup(buttons)

async def show_access_menu(query):
    current_chat = query.message.chat if query.message else None
    lines = ["🔐 **ДОСТУПНЫЕ ЧАТЫ**", "", "Бот работает только в этих чатах:"]
    if not allowed_chat_ids:
        lines.append("❌ Пока ни одного чата нет.")
    else:
        for i, chat_id in enumerate(list(allowed_chat_ids)[:2], 1):
            lines.append(f"{i}. `{chat_id}`")
    lines += [
        "",
        f"📊 Занято: **{len(allowed_chat_ids)}/2**",
        "",
        "Для публичной группы можно указать @username или chat ID.",
        "Для приватной группы открой /admin прямо в ней и нажми «Привязать этот чат»."
    ]
    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=access_menu_keyboard(current_chat),
        parse_mode="Markdown"
    )

# =========================================================
# ЛУДКА 777 — АДМИНКА
# =========================================================

def ludka_status():
    return "🟢 ВКЛЮЧЕНА" if ludka_enabled else "🔴 ВЫКЛЮЧЕНА"


def ludka_menu_keyboard():
    toggle_text = "⛔ Остановить" if ludka_enabled else "🎰 Запустить"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{toggle_text}",
                callback_data="ludka_stop" if ludka_enabled else "ludka_launch"
            )
        ],
        [
            InlineKeyboardButton("💰 Цена", callback_data="ludka_price"),
            InlineKeyboardButton("🎁 Приз", callback_data="ludka_prize"),
        ],
        [
            InlineKeyboardButton("📝 Сообщение", callback_data="ludka_message"),
        ],
        [
            InlineKeyboardButton("⬅️ Админ-панель", callback_data="main"),
        ],
    ])


async def show_ludka_menu(query):
    text = (
        "🎰 НАСТРОЙКИ ЛУДКИ 777\n\n"
        f"Статус: {ludka_status()}\n"
        f"🎁 Приз: {ludka_prize}\n"
        f"💰 Цена 1 соо: {ludka_price}\n\n"
        "📝 Сообщение можно менять текстом или "
        "фото + подписью. Premium/Custom Emoji сохраняются."
    )
    await query.edit_message_text(
        text,
        reply_markup=ludka_menu_keyboard(),
        parse_mode=None
    )


async def launch_ludka(query, context):
    global ludka_enabled, ludka_progress, ludka_chat_id

    if not query.message:
        return

    ludka_enabled = True
    ludka_progress = {}
    await _db_set("ludka_enabled", True)

    # Если лудку запускают из админки в группе — запоминаем эту группу.
    # Если админка открыта в личке, используем последнюю группу.
    if query.message.chat.type in ("group", "supergroup"):
        ludka_chat_id = query.message.chat_id
        await _db_set("ludka_chat_id", ludka_chat_id)
    chat_id = ludka_chat_id or query.message.chat_id

    try:
        if ludka_photo:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=ludka_photo,
                caption=ludka_text,
                caption_entities=ludka_entities or []
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=ludka_text,
                entities=ludka_entities or []
            )

        await query.edit_message_text(
            "✅ **Лудка 777 запущена!**\n\n"
            "Пользователи могут отправлять сообщения. "
            f"Каждые **{ludka_price}** сообщений участника — одно вращение.",
            reply_markup=ludka_menu_keyboard(),
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.exception("Ошибка запуска лудки")
        await query.edit_message_text(
            f"❌ Не удалось запустить лудку.\n\nОшибка: `{e}`",
            reply_markup=ludka_menu_keyboard(),
            parse_mode="Markdown"
        )


async def stop_ludka(query, context):
    global ludka_enabled, ludka_progress

    ludka_enabled = False
    ludka_progress = {}
    await _db_set("ludka_enabled", False)

    await query.edit_message_text(
        "⛔ **Лудка 777 остановлена.**",
        reply_markup=ludka_menu_keyboard(),
        parse_mode="Markdown"
    )


# =========================================================
# ПОКАЗАТЬ ПОДАРКИ
# =========================================================

async def show_gifts(
    query,
    context
):

    global selected_gift_id

    try:

        gifts = await context.bot.get_available_gifts()

    except Exception as e:

        logging.exception(
            "Ошибка получения подарков"
        )

        await query.edit_message_text(

            "❌ Не удалось получить список подарков.\n\n"
            f"Ошибка: `{e}`",

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="main"
                    )
                ]

            ]),

            parse_mode="Markdown"
        )

        return


    if not gifts.gifts:

        await query.edit_message_text(

            "❌ Сейчас Telegram не вернул доступные подарки.",

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="main"
                    )
                ]

            ])
        )

        return


    text = "🎁 **ДОСТУПНЫЕ ПОДАРКИ**\n\n"

    buttons = []


    for gift in gifts.gifts:

        selected = (
            " ✅"
            if gift.id == selected_gift_id
            else
            ""
        )

        text += (
            f"🎁 ID: `{gift.id}`\n"
            f"⭐ Цена: **{gift.star_count} Stars**"
            f"{selected}\n\n"
        )

        buttons.append([

            InlineKeyboardButton(

                f"Выбрать 🎁 {gift.star_count}⭐",

                callback_data=f"gift:{gift.id}"
            )

        ])


    buttons.append([

        InlineKeyboardButton(
            "🤖 Автоматический выбор",
            callback_data="gift:auto"
        )

    ])


    buttons.append([

        InlineKeyboardButton(
            "⬅️ Назад",
            callback_data="main"
        )

    ])


    await query.edit_message_text(

        text,

        reply_markup=InlineKeyboardMarkup(
            buttons
        ),

        parse_mode="Markdown"
    )


# =========================================================
# ВЫБОР ПОДАРКА
# =========================================================

async def select_gift(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global selected_gift_id

    query = update.callback_query

    if not await is_admin(update, context):

        await query.answer(
            "❌ Нет доступа.",
            show_alert=True
        )

        return

    await query.answer()


    if query.data == "gift:auto":

        selected_gift_id = None
        await _db_set("selected_gift_id", "")

        await query.edit_message_text(

            "🤖 **Автоматический выбор включен.**\n\n"
            "Бот будет выбирать самый дешевый "
            "доступный подарок.",

            reply_markup=InlineKeyboardMarkup([

                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="gifts"
                    )
                ]

            ]),

            parse_mode="Markdown"
        )

        return


    selected_gift_id = query.data.split(
        ":",
        1
    )[1]
    await _db_set("selected_gift_id", selected_gift_id)


    await query.edit_message_text(

        "✅ **Подарок выбран!**\n\n"
        f"🎁 ID: `{selected_gift_id}`\n\n"
        "Теперь этот подарок будет использоваться "
        "при выигрыше.",

        reply_markup=InlineKeyboardMarkup([

            [
                InlineKeyboardButton(
                    "🎁 Другие подарки",
                    callback_data="gifts"
                )
            ],

            [
                InlineKeyboardButton(
                    "⬅️ Админ-панель",
                    callback_data="main"
                )
            ]

        ]),

        parse_mode="Markdown"
    )


# =========================================================
# ОТПРАВКА ПОДАРКА
# =========================================================

async def give_gift(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global selected_gift_id

    if not update.effective_user:
        return False


    user_id = update.effective_user.id


    try:

        gifts = await context.bot.get_available_gifts()


        if not gifts.gifts:

            stats["errors"] += 1

            await _db_inc_stat("errors")
            return False


        gift = None


        # -------------------------------------------------
        # ЕСЛИ ВЫБРАН КОНКРЕТНЫЙ ПОДАРОК
        # -------------------------------------------------

        if selected_gift_id:

            for g in gifts.gifts:

                if g.id == selected_gift_id:

                    gift = g

                    break


        # -------------------------------------------------
        # ЕСЛИ НЕ НАШЛИ — АВТО
        # -------------------------------------------------

        if gift is None:

            gift = min(
                gifts.gifts,
                key=lambda g: g.star_count
            )


        # -------------------------------------------------
        # ОТПРАВЛЯЕМ ПОДАРОК
        # -------------------------------------------------

        await context.bot.send_gift(

            user_id=user_id,

            gift_id=gift.id,

            text="🎁 Поздравляем! Ты выиграл подарок!"

        )


        stats["gifts_sent"] += 1


        await _db_inc_stat("gifts_sent")
        return True


    except Exception as e:

        logging.exception(
            "Ошибка отправки подарка"
        )

        stats["errors"] += 1

        await _db_inc_stat("errors")
        return False


# =========================================================
# НАСТРОЙКА СООБЩЕНИЯ АДМИНОМ
# =========================================================

async def admin_content_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global win_text, win_photo, win_entities
    global ludka_text, ludka_photo, ludka_entities
    global ludka_prize, ludka_prize_entities, ludka_price

    if not update.effective_user:
        return

    if update.effective_user.id != ADMIN_ID:
        return

    message = update.message
    if not message:
        return

    # =====================================================
    # ДОБАВЛЕНИЕ ЧАТА В ДОПУЩЕННЫЕ
    # =====================================================
    if context.user_data.get("waiting_access_chat"):
        if not message.text:
            await message.reply_text("❌ Отправь @username группы или числовой chat ID.")
            raise ApplicationHandlerStop

        value = message.text.strip()
        if value.startswith("@"): 
            try:
                chat = await context.bot.get_chat(value)
                if chat.type not in ("group", "supergroup"):
                    await message.reply_text("❌ Нужна группа или супергруппа, а не личный чат/канал.")
                    raise ApplicationHandlerStop
                if len(allowed_chat_ids) >= 2 and chat.id not in allowed_chat_ids:
                    await message.reply_text("❌ Уже добавлены 2 чата. Сначала удали один в админке.")
                    raise ApplicationHandlerStop
                allowed_chat_ids.add(chat.id)
                context.user_data["waiting_access_chat"] = False
                await message.reply_text(f"✅ Группа {value} добавлена.\n\nChat ID: `{chat.id}`", parse_mode="Markdown")
            except Exception as e:
                logging.exception("Ошибка добавления чата")
                await message.reply_text(
                    "❌ Не удалось найти этот чат. Проверь @username.\n\n"
                    f"Ошибка: `{e}`", parse_mode="Markdown"
                )
            raise ApplicationHandlerStop

        try:
            chat_id = int(value)
        except ValueError:
            await message.reply_text("❌ Нужен @username или числовой chat ID.")
            raise ApplicationHandlerStop

        if len(allowed_chat_ids) >= 2 and chat_id not in allowed_chat_ids:
            await message.reply_text("❌ Уже добавлены 2 чата. Сначала удали один в админке.")
            raise ApplicationHandlerStop
        allowed_chat_ids.add(chat_id)
        context.user_data["waiting_access_chat"] = False
        await message.reply_text(f"✅ Чат `{chat_id}` добавлен в разрешённые.", parse_mode="Markdown")
        raise ApplicationHandlerStop

    # =====================================================
    # ЦЕНА ЛУДКИ
    # =====================================================
    if context.user_data.get("waiting_ludka_price"):
        if not message.text:
            await message.reply_text("❌ Отправь число, например: 1 или 5.")
            raise ApplicationHandlerStop

        try:
            value = int(message.text.strip())
            if value < 1 or value > 100000:
                raise ValueError

            ludka_price = value
            await _db_set("ludka_price", ludka_price)
            context.user_data["waiting_ludka_price"] = False

            await message.reply_text(
                f"✅ Цена лудки установлена: **{ludka_price} соо**",
                parse_mode="Markdown"
            )
        except ValueError:
            await message.reply_text(
                "❌ Укажи целое число от 1 до 100000.\n"
                "Например: `1` или `10`.",
                parse_mode="Markdown"
            )

        raise ApplicationHandlerStop

    # =====================================================
    # ПРИЗ ЛУДКИ
    # =====================================================
    if context.user_data.get("waiting_ludka_prize"):
        if not message.text:
            await message.reply_text("❌ Отправь текст приза.")
            raise ApplicationHandlerStop

        if len(message.text) > 4096:
            await message.reply_text(
                "❌ Приз слишком длинный. Максимум 4096 символов."
            )
            raise ApplicationHandlerStop

        ludka_prize = message.text
        ludka_prize_entities = message.entities or []
        await _db_set("ludka_prize", ludka_prize)
        await _db_set_entities("ludka_prize_entities", ludka_prize_entities)
        context.user_data["waiting_ludka_prize"] = False

        await message.reply_text(
            "✅ **Приз сохранён!**\n\n"
            f"{ludka_prize}\n\n"
            f"✨ Premium Emoji: "
            f"{'сохранены' if ludka_prize_entities else 'нет'}",
            entities=ludka_prize_entities,
            parse_mode=None
        )
        raise ApplicationHandlerStop

    # =====================================================
    # СООБЩЕНИЕ ЛУДКИ
    # =====================================================
    if context.user_data.get("waiting_ludka_message"):
        if message.photo:
            caption = message.caption or ""

            if len(caption) > 1024:
                await message.reply_text(
                    "❌ Подпись слишком длинная. Для фотографии максимум 1024 символа."
                )
                raise ApplicationHandlerStop

            ludka_photo = message.photo[-1].file_id
            ludka_text = caption
            ludka_entities = message.caption_entities or []
            await _db_set("ludka_photo", ludka_photo)
            await _db_set("ludka_text", ludka_text)
            await _db_set_entities("ludka_entities", ludka_entities)
            context.user_data["waiting_ludka_message"] = False

            await message.reply_text(
                "✅ **Сообщение лудки сохранено!**\n\n"
                "📷 Фото: установлено\n"
                f"📝 Текст: {ludka_text or '(без текста)'}\n"
                f"✨ Premium Emoji: "
                f"{'сохранены' if ludka_entities else 'нет'}",
                parse_mode="Markdown"
            )
        elif message.text:
            if len(message.text) > 4096:
                await message.reply_text(
                    "❌ Текст слишком длинный. Максимум 4096 символов."
                )
                raise ApplicationHandlerStop

            ludka_text = message.text
            ludka_photo = None
            ludka_entities = message.entities or []
            await _db_set("ludka_photo", "")
            await _db_set("ludka_text", ludka_text)
            await _db_set_entities("ludka_entities", ludka_entities)
            context.user_data["waiting_ludka_message"] = False

            await message.reply_text(
                "✅ **Сообщение лудки сохранено!**\n\n"
                f"{ludka_text}\n\n"
                f"✨ Premium Emoji: "
                f"{'сохранены' if ludka_entities else 'нет'}",
                parse_mode="Markdown"
            )
        else:
            await message.reply_text(
                "❌ Отправь текст или фотографию с подписью."
            )

        raise ApplicationHandlerStop

    # Админ сейчас ничего не настраивает.
    if not context.user_data.get("waiting_win_message"):
        return

    # =====================================================
    # СТАРОЕ: СООБЩЕНИЕ ПОБЕДИТЕЛЯ
    # =====================================================
    if message.photo:
        caption = message.caption or ""

        if len(caption) > 1024:
            await message.reply_text(
                "❌ Подпись слишком длинная.\n\n"
                "Для фотографии максимум 1024 символа."
            )
            raise ApplicationHandlerStop

        photo = message.photo[-1]
        win_photo = photo.file_id
        win_text = caption
        win_entities = message.caption_entities or []
        await _db_set("win_photo", win_photo)
        await _db_set("win_text", win_text)
        await _db_set_entities("win_entities", win_entities)

        context.user_data["waiting_win_message"] = False

        await message.reply_text(
            "✅ **Сообщение сохранено!**\n\n"
            "📷 Фото: установлено\n"
            f"📝 Текст: {win_text or '(без текста)'}\n\n"
            "✨ Premium Emoji: "
            f"{'сохранены' if win_entities else 'нет'}",
            parse_mode="Markdown"
        )
        raise ApplicationHandlerStop

    if message.text:
        text = message.text

        if len(text) > 4096:
            await message.reply_text(
                "❌ Текст слишком длинный.\n\n"
                "Максимум 4096 символов."
            )
            raise ApplicationHandlerStop

        win_text = text
        win_photo = None
        win_entities = message.entities or []
        await _db_set("win_photo", "")
        await _db_set("win_text", win_text)
        await _db_set_entities("win_entities", win_entities)

        context.user_data["waiting_win_message"] = False

        await message.reply_text(
            "✅ **Текст сохранён!**\n\n"
            f"{win_text}\n\n"
            "✨ Premium Emoji: "
            f"{'сохранены' if win_entities else 'нет'}",
            parse_mode="Markdown"
        )

        raise ApplicationHandlerStop

# =========================================================
# /CANCEL
# =========================================================

async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if (
        update.effective_user
        and update.effective_user.id == ADMIN_ID
    ):

        context.user_data["waiting_win_message"] = False
        context.user_data["waiting_ludka_price"] = False
        context.user_data["waiting_ludka_prize"] = False
        context.user_data["waiting_ludka_message"] = False
        context.user_data["waiting_access_chat"] = False

        await update.message.reply_text(
            "❌ Настройка отменена."
        )


# =========================================================
# ПРОВЕРКА ДОСТУПА
# =========================================================

async def access_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat:
        return

    # Админ может управлять ботом в любом чате.
    if update.effective_user and update.effective_user.id == ADMIN_ID:
        return

    chat_id = update.effective_chat.id
    if update.effective_chat.type == "private" or chat_id not in allowed_chat_ids:
        if update.message:
            await update.message.reply_text(ACCESS_DENIED_TEXT)
        raise ApplicationHandlerStop

# =========================================================
# ОСНОВНОЙ ОБРАБОТЧИК СООБЩЕНИЙ
# =========================================================

async def message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not update.message:
        return


    stats["messages"] += 1


    await _db_inc_stat("messages")
    # Лудка 777 работает независимо от обычного розыгрыша
    if (
        ludka_enabled
        and update.effective_user
        and not update.effective_user.is_bot
    ):
        await process_ludka_message(update, context)


    # Розыгрыш выключен
    if not giveaway_enabled:
        return


    # Ботов не учитываем
    if (
        update.effective_user
        and update.effective_user.is_bot
    ):

        return


    # -----------------------------------------------------
    # ПОЛУЧАЕМ ШАНС
    # -----------------------------------------------------

    chance = get_chance(context)


    # -----------------------------------------------------
    # РАНДОМ
    # -----------------------------------------------------

    roll = random.random() * 100


    if roll >= chance:

        return


    stats["wins"] += 1


    await _db_inc_stat("wins")
    # -----------------------------------------------------
    # ПЫТАЕМСЯ ОТПРАВИТЬ ПОДАРОК
    # -----------------------------------------------------

    success = await give_gift(
        update,
        context
    )


    # =====================================================
    # ПОДАРОК ОТПРАВЛЕН
    # =====================================================

    if success:

        await update.message.reply_text(

            "🎉 **ПОЗДРАВЛЯЕМ!**\n\n"
            "Ты выиграл настоящий "
            "Telegram-подарок! 🎁",

            parse_mode="Markdown"
        )

        return


    # =====================================================
    # ПОДАРОК НЕ УДАЛОСЬ ОТПРАВИТЬ
    # =====================================================

    try:

        if win_photo:

            await update.message.reply_photo(

                photo=win_photo,

                caption=win_text,

                caption_entities=win_entities

            )

        else:

            await update.message.reply_text(

                text=win_text,

                entities=win_entities

            )


    except Exception as e:

        logging.exception(
            "Ошибка отправки сообщения победителя"
        )

        stats["errors"] += 1

        await _db_inc_stat("errors")
# =========================================================
# ЛУДКА 777 — ИГРОВОЙ ПРОЦЕСС
# =========================================================

async def process_ludka_message(update, context):
    global ludka_progress

    user = update.effective_user
    if not user or not update.message:
        return

    user_id = user.id
    count = ludka_progress.get(user_id, 0) + 1
    ludka_progress[user_id] = count

    if count < ludka_price:
        return

    # Сбрасываем накопленные сообщения перед вращением.
    ludka_progress[user_id] = 0

    # Три барабана от 1 до 7.
    reels = [random.randint(1, 7) for _ in range(3)]
    result = " | ".join(str(x) for x in reels)

    if reels == [7, 7, 7]:
        await update.message.reply_text(
            "🎰 **777! ДЖЕКПОТ!**\n\n"
            f"👤 {user.mention_html()}\n"
            f"🎰 {result}",
            parse_mode="Markdown"
        )
        # Отдельным сообщением сохраняем исходные Telegram entities приза.
        await update.message.reply_text(
            ludka_prize,
            entities=ludka_prize_entities or None
        )
    else:
        await update.message.reply_text(
            f"🎰 {result}\n"
            f"😔 Не повезло. Нужны три семёрки!\n"
            f"💰 Цена вращения: {ludka_price} соо"
        )


# =========================================================
# /CHANCE
# =========================================================

async def chance_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not await is_admin(update, context):

        await update.message.reply_text(
            "❌ Только администратор может менять шанс."
        )

        return


    if not context.args:

        await update.message.reply_text(

            f"🎯 Сейчас шанс: "
            f"{get_chance(context)}%\n\n"

            "Примеры:\n"
            "/chance 1\n"
            "/chance 5\n"
            "/chance 0.5"
        )

        return


    try:

        value = float(
            context.args[0]
        )


        if value < 0 or value > 100:

            raise ValueError


        context.chat_data[
            "chance"
        ] = value


        await update.message.reply_text(

            f"✅ Шанс установлен: **{value}%**",

            parse_mode="Markdown"
        )


    except ValueError:

        await update.message.reply_text(

            "❌ Укажи число от 0 до 100.\n\n"
            "Например:\n"
            "/chance 1\n"
            "/chance 0.5"
        )


# =========================================================
# /ЛУДКА
# =========================================================

async def ludka_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ludka_enabled, ludka_progress, ludka_chat_id

    if not await is_admin(update, context):
        await update.message.reply_text("❌ Только администратор может управлять лудкой.")
        return

    ludka_enabled = True
    ludka_progress = {}
    await _db_set("ludka_enabled", True)
    ludka_chat_id = update.effective_chat.id

    try:
        if ludka_photo:
            await update.message.reply_photo(
                photo=ludka_photo,
                caption=ludka_text,
                caption_entities=ludka_entities or []
            )
        else:
            await update.message.reply_text(
                text=ludka_text,
                entities=ludka_entities or []
            )
    except Exception:
        logging.exception("Ошибка публикации лудки")
        await update.message.reply_text("❌ Не удалось опубликовать лудку.")
        return

    await update.message.reply_text(
        "🎰 Лудка **запущена**!\n\n"
        f"🎁 Приз: {ludka_prize}\n"
        f"💰 Цена 1 соо: {ludka_price}",
        parse_mode="Markdown"
    )


async def ludkaoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global ludka_enabled, ludka_progress

    if not await is_admin(update, context):
        return

    ludka_enabled = False
    ludka_progress = {}
    await _db_set("ludka_enabled", False)
    await update.message.reply_text("⛔ Лудка 777 остановлена.")


# =========================================================
# /START
# =========================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(

        "🎁 **Telegram Gift Bot**\n\n"

        "Бот случайно разыгрывает "
        "настоящие Telegram-подарки.\n\n"

        "🎯 Шанс зависит от настроек администратора.",

        parse_mode="Markdown"
    )


# =========================================================
# MAIN
# =========================================================


async def post_init(application):
    await db.init_db()
    await load_persistent_state()


async def post_shutdown(application):
    await db.close_db()


def main():

    # HTTP для Render
    threading.Thread(
        target=run_web_server,
        daemon=True
    ).start()


    # Создаём приложение
    app = (
        Application
        .builder()
        .token(TOKEN)
        .build()
    )


    # -----------------------------------------------------
    # COMMANDS
    # -----------------------------------------------------

    app.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )


    app.add_handler(
        CommandHandler(
            "admin",
            admin_command
        )
    )


    app.add_handler(
        CommandHandler(
            "chance",
            chance_command
        )
    )


    app.add_handler(
        CommandHandler(
            "cancel",
            cancel_command
        )
    )

    app.add_handler(
        CommandHandler(
            "ludka",
            ludka_command
        )
    )

    app.add_handler(
        CommandHandler(
            "ludkaoff",
            ludkaoff_command
        )
    )


    # -----------------------------------------------------
    # CALLBACKS
    # -----------------------------------------------------

    app.add_handler(

        CallbackQueryHandler(

            admin_callback,

            pattern=(
                r"^(main|chance|gifts|balance|stats|"
                r"toggle|refresh|setchance:.*|winmessage|"
                r"access|access_add_current|access_add_username|access_clear|access_remove:.*|"
                r"ludka|ludka_price|ludka_prize|"
                r"ludka_message|ludka_launch|ludka_stop)$"
            )
        )
    )


    app.add_handler(

        CallbackQueryHandler(

            select_gift,

            pattern=r"^gift:"
        )
    )


    # -----------------------------------------------------
    # ОГРАНИЧЕНИЕ ПО ЧАТАМ
    # -----------------------------------------------------
    app.add_handler(
        MessageHandler(filters.ALL, access_guard),
        group=-1
    )


    # -----------------------------------------------------
    # НАСТРОЙКА СООБЩЕНИЯ АДМИНОМ
    #
    # Сначала ловим фото и обычный текст.
    # Если админ сейчас находится в режиме настройки,
    # сообщение будет обработано здесь.
    # -----------------------------------------------------

    admin_input_filter = (
        filters.PHOTO
        |
        (filters.TEXT & ~filters.COMMAND)
    )


    app.add_handler(

        MessageHandler(
            admin_input_filter,
            admin_content_handler
        ),

        group=0
    )


    # -----------------------------------------------------
    # ОБЫЧНЫЕ СООБЩЕНИЯ
    # -----------------------------------------------------

    app.add_handler(

        MessageHandler(

            filters.TEXT & ~filters.COMMAND,

            message_handler

        ),

        group=1
    )


    print(
        "🎁 Telegram Gift Bot запущен!"
    )


    # Запуск
    app.run_polling()


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()

