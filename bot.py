import os
import random
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)

TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 10000))

# Шанс по умолчанию — 1%
DEFAULT_CHANCE = float(os.environ.get("GIFT_CHANCE", "1"))


# -----------------------------
# Web-сервер для Render
# -----------------------------

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is running!")

    def log_message(self, format, *args):
        pass


def run_web_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()


# -----------------------------
# Проверка администратора
# -----------------------------

async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user:
        return False

    member = await context.bot.get_chat_member(
        update.effective_chat.id,
        update.effective_user.id
    )

    return member.status in ["administrator", "creator"]


# -----------------------------
# Команда /chance
# -----------------------------

async def chance_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_chat:
        return

    if not await is_admin(update, context):
        await update.message.reply_text(
            "❌ Только администратор группы может менять шанс."
        )
        return

    chat_data = context.chat_data

    # /chance
    if not context.args:
        chance = chat_data.get("chance", DEFAULT_CHANCE)

        await update.message.reply_text(
            f"🎁 Текущий шанс: {chance}%\n\n"
            f"Изменить: /chance 5"
        )
        return

    try:
        new_chance = float(context.args[0])

        if new_chance < 0 or new_chance > 100:
            raise ValueError

        chat_data["chance"] = new_chance

        await update.message.reply_text(
            f"✅ Шанс изменён на {new_chance}%"
        )

    except ValueError:
        await update.message.reply_text(
            "❌ Укажи число от 0 до 100.\n"
            "Например: /chance 1"
        )


# -----------------------------
# Выдача подарка
# -----------------------------

async def give_gift(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not update.effective_user:
        return

    user_id = update.effective_user.id

    try:
        gifts = await context.bot.get_available_gifts()

        if not gifts.gifts:
            logging.error("Telegram не вернул доступные подарки.")
            return False

        # Берём самый дешёвый доступный подарок
        gift = min(
            gifts.gifts,
            key=lambda g: g.star_count
        )

        logging.info(
            f"Выдаём подарок {gift.id}, "
            f"стоимость {gift.star_count} Stars, "
            f"user_id={user_id}"
        )

        await context.bot.send_gift(
            user_id=user_id,
            gift_id=gift.id,
            text="🎁 Поздравляем! Ты выиграл подарок!"
        )

        return True

    except Exception as e:
        logging.exception(f"Ошибка выдачи подарка: {e}")
        return False


# -----------------------------
# Сообщения в чате
# -----------------------------

async def message_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not update.message or not update.effective_user:
        return

    chance = context.chat_data.get(
        "chance",
        DEFAULT_CHANCE
    )

    # Проверяем шанс
    roll = random.random() * 100

    if roll >= chance:
        return

    logging.info(
        f"🎉 ВЫИГРЫШ! "
        f"user={update.effective_user.id}, "
        f"roll={roll:.4f}, chance={chance}"
    )

    success = await give_gift(update, context)

    if success:
        await update.message.reply_text(
            "🎉🎁 ПОЗДРАВЛЯЕМ!\n\n"
            "Тебе выпал настоящий Telegram-подарок!"
        )
    else:
        await update.message.reply_text(
            "🎉 Выпал подарок, но сейчас его не удалось отправить."
        )


# -----------------------------
# Запуск
# -----------------------------

def main():

    threading.Thread(
        target=run_web_server,
        daemon=True
    ).start()

    app = Application.builder().token(TOKEN).build()

    # Команда изменения шанса
    app.add_handler(
        CommandHandler("chance", chance_command)
    )

    # Обычные сообщения
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
