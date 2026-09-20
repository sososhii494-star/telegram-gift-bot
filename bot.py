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
    filters,
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

DEFAULT_CHANCE = float(os.environ.get("GIFT_CHANCE", "1"))

# Статистика
stats = {
    "messages": 0,
    "wins": 0,
    "gifts_sent": 0,
    "errors": 0,
}

# Выбранный подарок.
# Если None — бот сам выберет самый дешёвый доступный.
selected_gift_id = None

# Розыгрыш включён
giveaway_enabled = True


# =========================================================
# RENDER WEB SERVER
# =========================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Telegram Gift Bot is running!")

    def log_message(self, format, *args):
        pass


def run_web_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


# =========================================================
# ADMIN
# =========================================================

# =========================================================
# АДМИН БОТА
# =========================================================

ADMIN_ID = 7491572487


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.effective_user:
        return False

    return update.effective_user.id == ADMIN_ID


# =========================================================
# ADMIN PANEL
# =========================================================

def admin_keyboard():

    status = "🟢 ВКЛЮЧЕН" if giveaway_enabled else "🔴 ВЫКЛЮЧЕН"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "🎯 Шанс",
                callback_data="chance"
            ),
            InlineKeyboardButton(
                "🎁 Подарки",
                callback_data="gifts"
            )
        ],
        [
            InlineKeyboardButton(
                "💰 Stars",
                callback_data="balance"
            ),
            InlineKeyboardButton(
                "📊 Статистика",
                callback_data="stats"
            )
        ],
        [
            InlineKeyboardButton(
                f"🎲 Розыгрыш: {status}",
                callback_data="toggle"
            )
        ],
        [
            InlineKeyboardButton(
                "🔄 Обновить подарки",
                callback_data="refresh"
            )
        ],
    ])


async def admin_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not await is_admin(update, context):
        await update.message.reply_text(
            "❌ Админ-панель доступна только администраторам."
        )
        return

    await update.message.reply_text(
        "🛠 **АДМИН-ПАНЕЛЬ**\n\n"
        "Здесь можно управлять розыгрышем, "
        "подарками и статистикой.",
        reply_markup=admin_keyboard(),
        parse_mode="Markdown"
    )


# =========================================================
# CALLBACKS
# =========================================================

async def admin_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    global giveaway_enabled
    global selected_gift_id

    query = update.callback_query

    await query.answer()

    if not await is_admin(update, context):
        await query.answer(
            "❌ Только для администраторов.",
            show_alert=True
        )
        return

    data = query.data

    # -----------------------------------------------------
    # ГЛАВНАЯ
    # -----------------------------------------------------

    if data == "main":

        status = (
            "🟢 ВКЛЮЧЕН"
            if giveaway_enabled
            else "🔴 ВЫКЛЮЧЕН"
        )

        await query.edit_message_text(
            "🛠 **АДМИН-ПАНЕЛЬ**\n\n"
            f"Розыгрыш: {status}\n"
            f"Шанс: {get_chance(context)}%\n\n"
            "Выбери действие:",
            reply_markup=admin_keyboard(),
            parse_mode="Markdown"
        )

    # -----------------------------------------------------
    # ШАНС
    # -----------------------------------------------------

    elif data == "chance":

        chance = get_chance(context)

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
                InlineKeyboardButton(
                    "1%",
                    callback_data="setchance:1"
                )
            ],
            [
                InlineKeyboardButton(
                    "2%",
                    callback_data="setchance:2"
                ),
                InlineKeyboardButton(
                    "5%",
                    callback_data="setchance:5"
                ),
                InlineKeyboardButton(
                    "10%",
                    callback_data="setchance:10"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data="main"
                )
            ]
        ])

        await query.edit_message_text(
            f"🎯 **ШАНС РОЗЫГРЫША**\n\n"
            f"Сейчас: **{chance}%**\n\n"
            "Выбери новый шанс:",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

    # -----------------------------------------------------
    # УСТАНОВКА ШАНСА
    # -----------------------------------------------------

    elif data.startswith("setchance:"):

        value = float(data.split(":")[1])

        context.chat_data["chance"] = value

        await query.edit_message_text(
            f"✅ Шанс установлен: **{value}%**",
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

    # -----------------------------------------------------
    # ПОДАРКИ
    # -----------------------------------------------------

    elif data == "gifts":

        await show_gifts(query, context)

    # -----------------------------------------------------
    # БАЛАНС
    # -----------------------------------------------------

    elif data == "balance":

        try:

            balance = await context.bot.get_my_star_balance()

            await query.edit_message_text(
                "💰 **БАЛАНС БОТА**\n\n"
                f"⭐ Stars: **{balance.amount}**",
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

            logging.exception(e)

            await query.edit_message_text(
                "❌ Не удалось получить баланс Stars.",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад",
                            callback_data="main"
                        )
                    ]
                ])
            )

    # -----------------------------------------------------
    # СТАТИСТИКА
    # -----------------------------------------------------

    elif data == "stats":

        chance = get_chance(context)

        await query.edit_message_text(
            "📊 **СТАТИСТИКА**\n\n"
            f"💬 Сообщений: **{stats['messages']}**\n"
            f"🎉 Выигрышей: **{stats['wins']}**\n"
            f"🎁 Подарков отправлено: **{stats['gifts_sent']}**\n"
            f"❌ Ошибок: **{stats['errors']}**\n\n"
            f"🎯 Текущий шанс: **{chance}%**",
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

    # -----------------------------------------------------
    # ВКЛ / ВЫКЛ
    # -----------------------------------------------------

    elif data == "toggle":

        giveaway_enabled = not giveaway_enabled

        status = (
            "🟢 включён"
            if giveaway_enabled
            else "🔴 выключен"
        )

        await query.edit_message_text(
            f"🎲 Розыгрыш теперь **{status}**.",
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

    # -----------------------------------------------------
    # ОБНОВИТЬ
    # -----------------------------------------------------

    elif data == "refresh":

        await query.edit_message_text(
            "🔄 Получаю актуальный список подарков..."
        )

        await show_gifts(query, context)


# =========================================================
# GIFTS
# =========================================================

async def show_gifts(query, context):

    global selected_gift_id

    try:

        gifts = await context.bot.get_available_gifts()

        if not gifts.gifts:

            await query.edit_message_text(
                "🎁 Сейчас Telegram не вернул доступные подарки.",
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

        buttons = []

        text = "🎁 **ДОСТУПНЫЕ ПОДАРКИ**\n\n"

        for gift in gifts.gifts:

            selected = " ✅" if gift.id == selected_gift_id else ""

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
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="Markdown"
        )

    except Exception as e:

        logging.exception(e)

        await query.edit_message_text(
            "❌ Ошибка при получении подарков.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="main"
                    )
                ]
            ])
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
            "🤖 Включён автоматический выбор подарка.\n\n"
            "Бот будет выбирать самый дешёвый доступный подарок.",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data="gifts"
                    )
                ]
            ])
        )

        return

    selected_gift_id = query.data.split(":", 1)[1]

    await query.edit_message_text(
        "✅ Подарок выбран!\n\n"
        f"ID: `{selected_gift_id}`",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "⬅️ К подаркам",
                    callback_data="gifts"
                )
            ],
            [
                InlineKeyboardButton(
                    "🏠 Админ-панель",
                    callback_data="main"
                )
            ]
        ]),
        parse_mode="Markdown"
    )


# =========================================================
# CHANCE
# =========================================================

def get_chance(context):

    return context.chat_data.get(
        "chance",
        DEFAULT_CHANCE
    )


# =========================================================
# SEND GIFT
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

            logging.error(
                "Нет доступных подарков."
            )

            stats["errors"] += 1

            return False

        gift = None

        # Если админ выбрал конкретный подарок
        if selected_gift_id:

            for g in gifts.gifts:

                if g.id == selected_gift_id:
                    gift = g
                    break

        # Если выбранный подарок больше недоступен
        if gift is None:

            # Автоматически берём самый дешёвый
            gift = min(
                gifts.gifts,
                key=lambda g: g.star_count
            )

        logging.info(
            f"🎁 Отправляем {gift.id} "
            f"за {gift.star_count} Stars "
            f"user={user_id}"
        )

        await context.bot.send_gift(
            user_id=user_id,
            gift_id=gift.id,
            text="🎁 Поздравляем! Ты выиграл подарок!"
        )

        stats["gifts_sent"] += 1

        return True

    except Exception as e:

        logging.exception(
            f"Ошибка отправки подарка: {e}"
        )

        stats["errors"] += 1

        return False


# =========================================================
# MESSAGE HANDLER
# =========================================================

async def message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not update.message:
        return

    stats["messages"] += 1

    if not giveaway_enabled:
        return

    # Не разыгрываем подарки за сообщения бота
    if update.effective_user and update.effective_user.is_bot:
        return

    chance = get_chance(context)

    roll = random.random() * 100

    if roll >= chance:
        return

    stats["wins"] += 1

    logging.info(
        f"🎉 ВЫИГРЫШ! "
        f"roll={roll:.4f}, "
        f"chance={chance}%"
    )

    success = await give_gift(
        update,
        context
    )

    if success:

        await update.message.reply_text(
            "🎉🎁 **ПОЗДРАВЛЯЕМ!**\n\n"
            "Ты выиграл настоящий Telegram-подарок! 🎁",
            parse_mode="Markdown"
        )

    else:

        await update.message.reply_text(
            "🎉 Ты выиграл!\n\n"
            "Но сейчас Telegram не позволил отправить подарок. "
            "Администратор проверит ситуацию."
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
            f"🎯 Сейчас шанс: {get_chance(context)}%\n\n"
            "Например:\n"
            "/chance 1\n"
            "/chance 5\n"
            "/chance 0.5"
        )

        return

    try:

        value = float(context.args[0])

        if value < 0 or value > 100:
            raise ValueError

        context.chat_data["chance"] = value

        await update.message.reply_text(
            f"✅ Шанс установлен: {value}%"
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Укажи число от 0 до 100."
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
        "Я разыгрываю настоящие Telegram-подарки "
        "с заданным шансом.\n\n"
        "Администратор может открыть /admin",
        parse_mode="Markdown"
    )


# =========================================================
# MAIN
# =========================================================

def main():

    threading.Thread(
        target=run_web_server,
        daemon=True
    ).start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(
        CommandHandler("start", start_command)
    )

    app.add_handler(
        CommandHandler("admin", admin_command)
    )

    app.add_handler(
        CommandHandler("chance", chance_command)
    )

    app.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern="^(main|chance|gifts|balance|stats|toggle|refresh|setchance:)"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            select_gift,
            pattern="^gift:"
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            message_handler
        )
    )

    print("🎁 Telegram Gift Bot запущен!")

    app.run_polling()


if __name__ == "__main__":
    main()
