import os
import sqlite3
import logging
from datetime import datetime, timedelta, date
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, ContextTypes, filters
)

# Настройка логов
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("BeautyBot")

# =========================================================
# БАЗА ДАННЫХ (SQLite)
# =========================================================
class Storage:
    def __init__(self, path: str = "data.sqlite3"):
        self.path = path
        self._init_db()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._conn() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT, 
                phone TEXT, created_at TEXT)""")
            # Добавлены поля для контроля напоминаний
            c.execute("""CREATE TABLE IF NOT EXISTS bookings(
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, 
                service_title TEXT, price INTEGER, 
                book_date TEXT, book_time TEXT, comment TEXT, 
                status TEXT, remind_24h INTEGER DEFAULT 0, 
                remind_1h INTEGER DEFAULT 0, created_at TEXT)""")

    def upsert_user(self, tg_id, username, name, phone):
        with self._conn() as c:
            c.execute("INSERT INTO users VALUES(?,?,?,?,?) ON CONFLICT(tg_id) DO UPDATE SET full_name=excluded.full_name, phone=excluded.phone",
                      (tg_id, username, name, phone, datetime.now().isoformat()))

    def get_user(self, tg_id):
        with self._conn() as c:
            return c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()

    def create_booking(self, uid, stitle, price, bdate, btime, comment):
        with self._conn() as c:
            cur = c.execute("INSERT INTO bookings(user_id, service_title, price, book_date, book_time, comment, status, created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                      (uid, stitle, price, bdate, btime, comment, 'confirmed', datetime.now().isoformat()))
            return cur.lastrowid

    def cancel_booking(self, bid):
        with self._conn() as c:
            c.execute("UPDATE bookings SET status='cancelled' WHERE id=?", (bid,))

    def get_user_bookings(self, uid):
        with self._conn() as c:
            return c.execute("SELECT * FROM bookings WHERE user_id=? AND status='confirmed' AND book_date >= ? ORDER BY book_date, book_time", 
                             (uid, date.today().isoformat())).fetchall()

    def get_upcoming_reminders(self):
        """Ищет записи для напоминаний (статус confirmed и время близко)"""
        with self._conn() as c:
            return c.execute("SELECT * FROM bookings WHERE status='confirmed'").fetchall()

    def mark_reminded(self, bid, type_remind):
        with self._conn() as c:
            c.execute(f"UPDATE bookings SET {type_remind}=1 WHERE id=?", (bid,))

# =========================================================
# КОНФИГУРАЦИЯ И СТАДИИ
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "ТВОЙ_ТОКЕН")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
SALON_NAME = "💎 Luxury Beauty Studio"

store = Storage()
STAGE_REG_NAME, STAGE_REG_PHONE, STAGE_COMMENT = range(3)

SERVICES = {
    "svc1": ("Маникюр + Гель-лак", 2500),
    "svc2": ("Наращивание ресниц", 3500),
    "svc3": ("Педикюр", 3000),
}

# =========================================================
# КЛАВИАТУРЫ И УТИЛИТЫ
# =========================================================
def main_menu(uid):
    btns = [["💅 Записаться", "📅 Мои записи"], ["💰 Цены", "📍 Контакты"]]
    if uid == ADMIN_ID: btns.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)



# =========================================================
# ФУНКЦИИ НАПОМИНАНИЙ (JOB QUEUE)
# =========================================================
async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """Фоновая задача: проверяет кому пора отправить напоминание"""
    now = datetime.now()
    bookings = store.get_upcoming_reminders()

    for b in bookings:
        try:
            # Парсим дату и время записи
            appt_dt = datetime.strptime(f"{b['book_date']} {b['book_time']}", "%Y-%m-%d %H:%M")
            diff = appt_dt - now
            
            # Напоминание за 24 часа
            if timedelta(hours=23) < diff <= timedelta(hours=24) and not b['remind_24h']:
                await context.bot.send_message(
                    b['user_id'], 
                    f"🔔 Напоминание! Завтра в {b['book_time']} ждем вас на: {b['service_title']} ✨"
                )
                store.mark_reminded(b['id'], 'remind_24h')

            # Напоминание за 1 час
            elif timedelta(minutes=50) < diff <= timedelta(hours=1, minutes=5) and not b['remind_1h']:
                await context.bot.send_message(
                    b['user_id'], 
                    f"⚡️ До вашей записи остался всего 1 час! Ждем вас в {b['book_time']}! 🤗"
                )
                store.mark_reminded(b['id'], 'remind_1h')
        except Exception as e:
            log.error(f"Ошибка в рассылке напоминания: {e}")

# =========================================================
# ОБРАБОТЧИКИ
# =========================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = store.get_user(uid)
    if not user:
        context.user_data["stage"] = STAGE_REG_NAME
        await update.message.reply_text(f"Добро пожаловать в {SALON_NAME}! ✨\n\nДавайте познакомимся. Как вас зовут?", reply_markup=ReplyKeyboardRemove())
    else:
        context.user_data.clear()
        await update.message.reply_text(f"Здравствуйте, {user['full_name']}! Рады видеть вас снова.", reply_markup=main_menu(uid))

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text
    stage = context.user_data.get("stage")

    if stage == STAGE_REG_NAME:
        context.user_data["reg_name"] = text
        context.user_data["stage"] = STAGE_REG_PHONE
        kb = ReplyKeyboardMarkup([[KeyboardButton("📱 Поделиться контактом", request_contact=True)]], resize_keyboard=True)
        await update.message.reply_text(f"Приятно познакомиться, {text}! Пожалуйста, нажмите кнопку ниже, чтобы отправить номер телефона.", reply_markup=kb)
        return

    if stage == STAGE_COMMENT:
        context.user_data["comment"] = text
        context.user_data["stage"] = None
        s_title, s_price = SERVICES[context.user_data["s_key"]]
        msg = (f"🧐 *Проверьте детали записи:*\n\n"
               f"💅 Услуга: {s_title}\n"
               f"📅 Дата: {context.user_data['date']}\n"
               f"⏰ Время: {context.user_data['time']}\n"
               f"💰 Стоимость: {s_price}₽\n"
               f"💬 Комментарий: {text}")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Все верно, записываюсь!", callback_data="confirm_final")],
                                   [InlineKeyboardButton("❌ Отмена", callback_data="cancel_flow")]])
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return

    if text == "💅 Записаться":
        kb = [[InlineKeyboardButton(f"{v[0]} ({v[1]}₽)", callback_data=f"svc_{k}")] for k, v in SERVICES.items()]
        await update.message.reply_text("Что будем делать сегодня?", reply_markup=InlineKeyboardMarkup(kb))

    elif text == "📅 Мои записи":
        bookings = store.get_user_bookings(uid)
        if not bookings:
            await update.message.reply_text("У вас нет активных записей.")
            return
        for b in bookings:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить запись", callback_data=f"cancel_bid_{b['id']}")]])
            await update.message.reply_text(f"📍 {b['book_date']} в {b['book_time']}\n💅 {b['service_title']}", reply_markup=kb)

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if context.user_data.get("stage") == STAGE_REG_PHONE:
        phone = update.message.contact.phone_number
        name = context.user_data["reg_name"]
        store.upsert_user(uid, update.effective_user.username, name, phone)
        context.user_data.clear()
        await update.message.reply_text(f"Регистрация завершена! 🎉 Поздравляем, {name}!", reply_markup=main_menu(uid))

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = update.effective_user.id
    await query.answer()

    if query.data.startswith("svc_"):
        context.user_data["s_key"] = query.data.split("_")[1]
        days = [[InlineKeyboardButton((date.today() + timedelta(days=i)).strftime("%d.%m"), 
                callback_data=f"date_{(date.today() + timedelta(days=i)).isoformat()}")] for i in range(1, 10)]
        await query.edit_message_text("Выберите удобный день:", reply_markup=InlineKeyboardMarkup(days))

    elif query.data.startswith("date_"):
        context.user_data["date"] = query.data.split("_")[1]
        times = ["10:00", "12:00", "14:00", "16:00", "18:00"]
        kb = [[InlineKeyboardButton(t, callback_data=f"time_{t}")] for t in times]
        await query.edit_message_text("Выберите время:", reply_markup=InlineKeyboardMarkup(kb))

    elif query.data.startswith("time_"):
        context.user_data["time"] = query.data.split("_")[1]
        context.user_data["stage"] = STAGE_COMMENT
        await query.edit_message_text("Добавьте комментарий к записи (или напишите '-')")

    elif query.data == "confirm_final":
        s_title, s_price = SERVICES[context.user_data["s_key"]]
        bid = store.create_booking(uid, s_title, s_price, context.user_data["date"], 
                                   context.user_data["time"], context.user_data["comment"])
        await query.edit_message_text(f"🎉 Вы успешно записаны! Номер записи: #{bid}")
        
        # ОПОВЕЩЕНИЕ АДМИНУ
        user = store.get_user(uid)
        admin_msg = (f"🔔 *НОВАЯ ЗАПИСЬ #{bid}*\n\n"
                     f"👤 Клиент: {user['full_name']}\n"
                     f"📞 Телефон: `{user['phone']}`\n"
                     f"🔗 Ник: @{user['username'] if user['username'] else 'нет'}\n"
                     f"💅 Услуга: {s_title}\n"
                     f"📅 Дата: {context.user_data['date']}\n"
                     f"⏰ Время: {context.user_data['time']}\n"
                     f"💬 Комм: {context.user_data['comment']}")
        await context.bot.send_message(ADMIN_ID, admin_msg, parse_mode="Markdown")

    elif query.data.startswith("cancel_bid_"):
        bid = query.data.split("_")[2]
        store.cancel_booking(bid)
        await query.edit_message_text("❌ Запись отменена.")
        await context.bot.send_message(ADMIN_ID, f"⚠️ *ОТМЕНА ЗАПИСИ #{bid}* клиент отозвал запись.")

# =========================================================
# ЗАПУСК
# =========================================================
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Запуск фонового планировщика (каждую минуту)
    app.job_queue.run_repeating(reminder_job, interval=60, first=10)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_callback))

    print("--- Бот запущен и готов к работе! ---")
    app.run_polling()

if __name__ == "__main__":
    main()
