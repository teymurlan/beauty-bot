import os
import sqlite3
import logging
import asyncio
from datetime import datetime, timedelta, date
from typing import Optional

from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, ContextTypes, filters, ApplicationBuilder
)
from telegram.error import Forbidden, BadRequest

# =========================================================
# 1. КОНФИГУРАЦИЯ (ОБЯЗАТЕЛЬНО ЗАПОЛНИТЬ)
# =========================================================
TOKEN = "ВАШ_ТОКЕН_ИЗ_BOTFATHER"
ADMIN_ID = 123456789  # Ваш числовой ID (узнать в @userinfobot)
SALON_NAME = "💎 LUXURY BEAUTY STUDIO"

# Настройка логирования для отладки
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# =========================================================
# 2. СИСТЕМА ХРАНЕНИЯ ДАННЫХ (SQLITE)
# =========================================================
class Database:
    def __init__(self, db_name="salon_beauty.db"):
        self.db_name = db_name
        self._create_tables()

    def _execute(self, query, params=(), fetchone=False, fetchall=False, commit=False):
        with sqlite3.connect(self.db_name) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            if commit: conn.commit()
            if fetchone: return cursor.fetchone()
            if fetchall: return cursor.fetchall()
            return cursor

    def _create_tables(self):
        # Таблица пользователей
        self._execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                phone TEXT,
                reg_date DATETIME
            )""", commit=True)
        # Таблица записей
        self._execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service_name TEXT,
                b_date TEXT,
                b_time TEXT,
                comment TEXT,
                remind_24h INTEGER DEFAULT 0,
                remind_1h INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active'
            )""", commit=True)

    def add_user(self, uid, username, name, phone):
        self._execute("INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?, ?)", 
                      (uid, username, name, phone, datetime.now()), commit=True)

    def get_user(self, uid):
        return self._execute("SELECT * FROM users WHERE user_id = ?", (uid,), fetchone=True)

    def add_booking(self, uid, service, b_date, b_time, comment):
        self._execute("INSERT INTO bookings (user_id, service_name, b_date, b_time, comment) VALUES (?, ?, ?, ?, ?)",
                      (uid, service, b_date, b_time, comment), commit=True)

    def get_user_bookings(self, uid):
        return self._execute("SELECT * FROM bookings WHERE user_id = ? AND status = 'active' ORDER BY b_date, b_time", (uid,), fetchall=True)

    def get_all_active_bookings(self):
        return self._execute("SELECT b.*, u.full_name, u.phone FROM bookings b JOIN users u ON b.user_id = u.user_id WHERE b.status = 'active'", fetchall=True)

db = Database()

# =========================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ КЛАВИАТУРЫ
# =========================================================
def main_kb(uid):
    buttons = [
        [KeyboardButton("💅 Записаться на процедуру")],
        [KeyboardButton("📅 Мои записи"), KeyboardButton("💰 Прайс-лист")],
        [KeyboardButton("📍 Контакты и адрес")]
    ]
    if uid == ADMIN_ID:
        buttons.append([KeyboardButton("🛠 Админ-панель")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

SERVICES = {
    "manic": "Маникюр + Покрытие (2500₽)",
    "pedic": "Педикюр (3000₽)",
    "complex": "Комплекс в 4 руки (5000₽)"
}

# =========================================================
# 4. ФОНОВЫЕ ЗАДАЧИ (НАПОМИНАНИЯ)
# =========================================================
async def reminder_callback(context: ContextTypes.DEFAULT_TYPE):
    """Проверка записей и отправка уведомлений каждые 60 секунд"""
    now = datetime.now()
    bookings = db.get_all_active_bookings()
    
    for b in bookings:
        try:
            appt_time = datetime.strptime(f"{b['b_date']} {b['b_time']}", "%Y-%m-%d %H:%M")
            time_diff = appt_time - now
            
            # Напоминание за 24 часа
            if timedelta(hours=23) < time_diff <= timedelta(hours=24) and not b['remind_24h']:
                await context.bot.send_message(b['user_id'], f"🔔 Напоминание! Завтра в {b['b_time']} ждем вас на {b['service_name']}! ✨")
                db._execute("UPDATE bookings SET remind_24h = 1 WHERE id = ?", (b['id'],), commit=True)
            
            # Напоминание за 1 час
            elif timedelta(minutes=55) < time_diff <= timedelta(hours=1, minutes=5) and not b['remind_1h']:
                await context.bot.send_message(b['user_id'], f"⚡️ Ждем вас через час ({b['b_time']})! Не опаздывайте 🤗")
                db._execute("UPDATE bookings SET remind_1h = 1 WHERE id = ?", (b['id'],), commit=True)
                
        except Exception as e:
            logger.error(f"Ошибка в системе напоминаний: {e}")

# =========================================================
# 5. ОСНОВНАЯ ЛОГИКА БОТА
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = db.get_user(uid)
    context.user_data.clear()

    if not user:
        context.user_data["step"] = "WAITING_NAME"
        await update.message.reply_text(
            f"Добро пожаловать в {SALON_NAME}! ✨\n\nВы у нас впервые. Пожалуйста, введите ваше **Имя** для регистрации:",
            parse_mode="Markdown", reply_markup=ReplyKeyboardRemove()
        )
    else:
        await update.message.reply_text(
            f"Рады видеть вас снова, {user['full_name']}! 👋\nЧем могу помочь сегодня?",
            reply_markup=main_kb(uid)
        )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text
    step = context.user_data.get("step")
    user = db.get_user(uid)

    # --- ЛОГИКА РЕГИСТРАЦИИ ---
    if step == "WAITING_NAME":
        context.user_data["reg_name"] = text
        context.user_data["step"] = "WAITING_PHONE"
        btn = [[KeyboardButton("📱 Отправить номер", request_contact=True)]]
        await update.message.reply_text(f"Приятно познакомиться, {text}! Теперь отправьте ваш номер телефона для связи:", 
                                         reply_markup=ReplyKeyboardMarkup(btn, resize_keyboard=True))
        return

    # --- ЛОГИКА ЗАПИСИ (КОММЕНТАРИЙ) ---
    if step == "WAITING_COMMENT":
        context.user_data["comment"] = text
        context.user_data["step"] = None
        s_name = SERVICES[context.user_data["svc"]]
        d, t = context.user_data["date"], context.user_data["time"]
        
        summary = (f"🧐 *Проверьте вашу запись:*\n\n"
                   f"💅 Услуга: {s_name}\n"
                   f"📅 Дата: {d}\n"
                   f"⏰ Время: {t}\n"
                   f"💬 Комментарий: {text}\n\n"
                   f"Все верно?")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, подтверждаю!", callback_data="confirm_final")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_booking")]
        ])
        await update.message.reply_text(summary, parse_mode="Markdown", reply_markup=kb)
        return

    # --- ГЛАВНОЕ МЕНЮ ---
    if text == "💅 Записаться на процедуру":
        btns = [[InlineKeyboardButton(name, callback_data=f"svc_{key}")] for key, name in SERVICES.items()]
        await update.message.reply_text("Выберите желаемую услугу:", reply_markup=InlineKeyboardMarkup(btns))

    elif text == "📅 Мои записи":
        books = db.get_user_bookings(uid)
        if not books:
            await update.message.reply_text("У вас пока нет активных записей. Хотите записаться?")
        else:
            res = "🗓 *Ваши ближайшие визиты:*\n\n"
            for b in books:
                res += f"📍 {b['b_date']} в {b['b_time']} — {b['service_name']}\n"
            await update.message.reply_text(res, parse_mode="Markdown")

    elif text == "🛠 Админ-панель" and uid == ADMIN_ID:
        all_b = db.get_all_active_bookings()
        report = f"📊 *Всего активных записей:* {len(all_b)}\n\n"
        for ab in all_b[:10]: # Показываем последние 10
            report += f"👤 {ab['full_name']} | {ab['b_date']} {ab['b_time']}\n📞 {ab['phone']}\n---\n"
        await update.message.reply_text(report, parse_mode="Markdown")

    elif text == "💰 Прайс-лист":
        p = "💳 *Наши услуги:*\n\n" + "\n".join([f"• {v}" for v in SERVICES.values()])
        await update.message.reply_text(p, parse_mode="Markdown")

async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if context.user_data.get("step") == "WAITING_PHONE":
        phone = update.message.contact.phone_number
        name = context.user_data.get("reg_name")
        db.add_user(uid, update.effective_user.username, name, phone)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Регистрация завершена! Теперь вы можете записываться на услуги.", 
                                         reply_markup=main_kb(uid))

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = update.effective_user.id
    data = query.data
    await query.answer()

    if data.startswith("svc_"):
        context.user_data["svc"] = data.split("_")[1]
        # Выбор даты (на 7 дней вперед)
        dates_kb = []
        for i in range(1, 8):
            d = (date.today() + timedelta(days=i)).isoformat()
            dates_kb.append([InlineKeyboardButton(d, callback_data=f"date_{d}")])
        await query.edit_message_text("На какой день планируем визит?", reply_markup=InlineKeyboardMarkup(dates_kb))

    elif data.startswith("date_"):
        context.user_data["date"] = data.split("_")[1]
        times = ["10:00", "12:30", "15:00", "17:30", "20:00"]
        times_kb = [[InlineKeyboardButton(t, callback_data=f"time_{t}")] for t in times]
        await query.edit_message_text("Выберите свободное время:", reply_markup=InlineKeyboardMarkup(times_kb))

    elif data.startswith("time_"):
        context.user_data["time"] = data.split("_")[1]
        context.user_data["step"] = "WAITING_COMMENT"
        await query.edit_message_text("Почти готово! Напишите короткое пожелание (дизайн, снятие и т.д.) или просто '-'")

    elif data == "confirm_final":
        db.add_booking(uid, SERVICES[context.user_data["svc"]], context.user_data["date"], 
                       context.user_data["time"], context.user_data["comment"])
        await query.edit_message_text("✅ Вы успешно записаны! Напоминания придут за 24 часа и за 1 час до начала.")
        
        # Уведомление админу
        u = db.get_user(uid)
        admin_text = (f"🔔 *НОВАЯ ЗАПИСЬ!*\n\nКлиент: {u['full_name']}\nТел: `{u['phone']}`\n"
                      f"Услуга: {SERVICES[context.user_data['svc']]}\nДата: {context.user_data['date']} {context.user_data['time']}")
        try:
            await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown")
        except: pass
        context.user_data.clear()

    elif data == "cancel_booking":
        context.user_data.clear()
        await query.edit_message_text("Запись отменена. Возвращайтесь, когда будете готовы!")

# =========================================================
# 6. ЗАПУСК
# =========================================================
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    # Включаем планировщик задач
    app.job_queue.run_repeating(reminder_callback, interval=60, first=10)

    # Регистрируем обработчики
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))

    print("--- БОТ ЗАПУЩЕН И ГОТОВ К РАБОТЕ ---")
    app.run_polling()

if __name__ == "__main__":
    main()
