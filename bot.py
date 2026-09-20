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
                "✏️ Сообщение победителя",
                callback_data="winmessage"
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

        context.chat_data["chance"] = value

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
    # ОБНОВИТЬ ПОДАРКИ
    # -----------------------------------------------------

    if data == "refresh":

        await show_gifts(
            query,
            context
        )

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

        return True


    except Exception as e:

        logging.exception(
            "Ошибка отправки подарка"
        )

        stats["errors"] += 1

        return False


# =========================================================
# НАСТРОЙКА СООБЩЕНИЯ АДМИНОМ
# =========================================================

async def admin_content_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global win_text
    global win_photo
    global win_entities


    if not update.effective_user:

        return


    if update.effective_user.id != ADMIN_ID:

        return


    # Админ сейчас ничего не настраивает
    if not context.user_data.get(
        "waiting_win_message"
    ):

        return


    message = update.message

    if not message:

        return


    # =====================================================
    # ФОТО + ТЕКСТ
    # =====================================================

    if message.photo:

        caption = message.caption or ""

        # Telegram caption максимум 1024 символа
        if len(caption) > 1024:

            await message.reply_text(

                "❌ Подпись слишком длинная.\n\n"
                "Для фотографии максимум 1024 символа."
            )

            return


        photo = message.photo[-1]

        win_photo = photo.file_id

        win_text = caption

        # Сохраняем ВСЕ Telegram-сущности,
        # включая Premium/Custom Emoji
        win_entities = message.caption_entities or []


        context.user_data[
            "waiting_win_message"
        ] = False


        await message.reply_text(

            "✅ **Сообщение сохранено!**\n\n"

            "📷 Фото: установлено\n"

            f"📝 Текст: "
            f"{win_text or '(без текста)'}\n\n"

            "✨ Premium Emoji: "
            f"{'сохранены' if win_entities else 'нет'}",

            parse_mode="Markdown"
        )


        # Останавливаем дальнейшую обработку
        raise ApplicationHandlerStop


    # =====================================================
    # ТОЛЬКО ТЕКСТ
    # =====================================================

    if message.text:

        text = message.text


        # Telegram text максимум 4096 символов
        if len(text) > 4096:

            await message.reply_text(

                "❌ Текст слишком длинный.\n\n"
                "Максимум 4096 символов."
            )

            return


        win_text = text

        win_photo = None

        # Сохраняем форматирование,
        # включая Premium/Custom Emoji
        win_entities = message.entities or []


        context.user_data[
            "waiting_win_message"
        ] = False


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

        context.user_data[
            "waiting_win_message"
        ] = False

        await update.message.reply_text(
            "❌ Настройка сообщения отменена."
        )


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


    # -----------------------------------------------------
    # CALLBACKS
    # -----------------------------------------------------

    app.add_handler(

        CallbackQueryHandler(

            admin_callback,

            pattern=(
                r"^(main|chance|gifts|balance|stats|"
                r"toggle|refresh|setchance:.*|winmessage)$"
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
