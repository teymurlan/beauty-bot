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

# 1. НАСТРОЙКА ЛОГИРОВАНИЯ
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("BeautyBot")

# 2. БАЗА ДАННЫХ (SQLite)
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
            # Таблица пользователей
            c.execute("""CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT, 
                phone TEXT, created_at TEXT)""")
            # Таблица записей с флагами для напоминаний
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

    def get_active_reminders(self):
        with self._conn() as c:
            return c.execute("SELECT * FROM bookings WHERE status='confirmed' AND (remind_24h=0 OR remind_1h=0)").fetchall()

    def mark_reminded(self, bid, col):
        with self._conn() as c:
            c.execute(f"UPDATE bookings SET {col}=1 WHERE id=?", (bid,))

store = Storage()

# 3. КОНФИГУРАЦИЯ
# ВНИМАНИЕ: Замените эти данные на свои!
BOT_TOKEN = "ВАШ_ТОКЕН_ИЗ_BOTFATHER"
ADMIN_ID = 123456789  # Ваш числовой ID (узнать можно в @userinfobot)

# Состояния (Stages)
REG_NAME, REG_PHONE, ADD_COMMENT = "RN", "RP", "AC"

SERVICES = {
    "s1": ("Маникюр + Покрытие", 2500),
    "s2": ("Педикюр + Покрытие", 3000),
    "s3": ("Наращивание", 4500)
}

# 4. КЛАВИАТУРЫ
def get_main_menu(uid):
    btns = [["💅 Записаться", "📅 Мои записи"], ["💰 Цены", "📍 Контакты"]]
    if uid == ADMIN_ID:
        btns.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

# 5. ФОНОВЫЕ НАПОМИНАНИЯ
async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    bookings = store.get_active_reminders()
    
    for b in bookings:
        try:
            appt_dt = datetime.strptime(f"{b['book_date']} {b['book_time']}", "%Y-%m-%d %H:%M")
            diff = appt_dt - now
            
            # За 24 часа
            if timedelta(hours=23) < diff <= timedelta(hours=24) and not b['remind_24h']:
                await context.bot.send_message(b['user_id'], f"🔔 Напоминание: Завтра в {b['book_time']} ждем вас на {b['service_title']}!")
                store.mark_reminded(b['id'], "remind_24h")
            
            # За 1 час
            elif timedelta(minutes=50) < diff <= timedelta(hours=1, minutes=5) and not b['remind_1h']:
                await context.bot.send_message(b['user_id'], f"⚡️ Ждем вас через час ({b['book_time']}) на процедуру: {b['service_title']}!")
                store.mark_reminded(b['id'], "remind_1h")
        except Exception as e:
            log.error(f"Ошибка напоминания: {e}")

# 6. ОБРАБОТЧИКИ
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = store.get_user(uid)
    context.user_data.clear() # Сброс всех временных данных

    if not user:
        context.user_data["stage"] = REG_NAME
        await update.message.reply_text("Добро пожаловать! ✨ Как мне к вам обращаться?", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text(f"Рады видеть вас снова, {user['full_name']}!", reply_markup=get_main_menu(uid))

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text
    user = store.get_user(uid)
    stage = context.user_data.get("stage")

    # Если юзер в базе, блокируем стадии регистрации
    if user and stage in [REG_NAME, REG_PHONE]:
        context.user_data["stage"] = None
        stage = None

    if stage == REG_NAME:
        context.user_data["tmp_name"] = text
        context.user_data["stage"] = REG_PHONE
        kb = ReplyKeyboardMarkup([[KeyboardButton("📱 Отправить контакт", request_contact=True)]], resize_keyboard=True)
        await update.message.reply_text("Теперь нажмите кнопку ниже, чтобы отправить номер телефона:", reply_markup=kb)
        return

    if stage == ADD_COMMENT:
        context.user_data["comment"] = text
        context.user_data["stage"] = None
        s_title, s_price = SERVICES[context.user_data["s_key"]]
        msg = (f"🧐 *Проверьте запись:*\n\n💅 {s_title}\n📅 {context.user_data['date']}\n⏰ {context.user_data['time']}\n💬 {text}")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ ПОДТВЕРДИТЬ", callback_data="final_ok")],
                                   [InlineKeyboardButton("❌ ОТМЕНА", callback_data="cancel")]])
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return

    # Главное меню
    if text == "💅 Записаться":
        kb = [[InlineKeyboardButton(f"{v[0]} ({v[1]}₽)", callback_data=f"svc_{k}")] for k, v in SERVICES.items()]
        await update.message.reply_text("Выберите услугу:", reply_markup=InlineKeyboardMarkup(kb))

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if context.user_data.get("stage") == REG_PHONE:
        phone = update.message.contact.phone_number
        store.upsert_user(uid, update.effective_user.username, context.user_data["tmp_name"], phone)
        context.user_data.clear()
        await update.message.reply_text("Регистрация завершена! 🎉", reply_markup=get_main_menu(uid))

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = update.effective_user.id
    await query.answer()

    if query.data.startswith("svc_"):
        context.user_data["s_key"] = query.data.split("_")[1]
        days = [[InlineKeyboardButton((date.today() + timedelta(days=i)).strftime("%d.%m"), 
                callback_data=f"date_{(date.today() + timedelta(days=i)).isoformat()}")] for i in range(1, 6)]
        await query.edit_message_text("Выберите дату:", reply_markup=InlineKeyboardMarkup(days))

    elif query.data.startswith("date_"):
        context.user_data["date"] = query.data.split("_")[1]
        kb = [[InlineKeyboardButton(t, callback_data=f"time_{t}")] for t in ["10:00", "13:00", "16:00", "19:00"]]
        await query.edit_message_text("Выберите время:", reply_markup=InlineKeyboardMarkup(kb))

    elif query.data.startswith("time_"):
        context.user_data["time"] = query.data.split("_")[1]
        context.user_data["stage"] = ADD_COMMENT
        await query.edit_message_text("Напишите комментарий (дизайн, снятие и т.д.) или отправьте '-'")

    elif query.data == "final_ok":
        s_title, s_price = SERVICES[context.user_data["s_key"]]
        bid = store.create_booking(uid, s_title, s_price, context.user_data["date"], 
                                   context.user_data["time"], context.user_data.get("comment", "-"))
        
        # УВЕДОМЛЕНИЕ АДМИНУ
        user = store.get_user(uid)
        admin_msg = (f"🔥 *НОВАЯ ЗАПИСЬ #{bid}*\n\n"
                     f"👤 Клиент: {user['full_name']}\n"
                     f"📞 Тел: `{user['phone']}`\n"
                     f"🔗 Ник: @{user['username'] if user['username'] else 'нет'}\n"
                     f"💅 Услуга: {s_title}\n"
                     f"📅 Дата: {context.user_data['date']}\n"
                     f"⏰ Время: {context.user_data['time']}\n"
                     f"💬 Комм: {context.user_data['comment']}")
        
        try: await context.bot.send_message(ADMIN_ID, admin_msg, parse_mode="Markdown")
        except: pass

        context.user_data.clear() # Очистка
        await query.edit_message_text(f"✅ Запись #{bid} подтверждена! Ждем вас.")
        await context.bot.send_message(uid, "Вы вернулись в главное меню:", reply_markup=get_main_menu(uid))

# 7. ЗАПУСК
def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Запуск напоминаний раз в минуту
    app.job_queue.run_repeating(reminder_job, interval=60, first=10)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    
    print("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
