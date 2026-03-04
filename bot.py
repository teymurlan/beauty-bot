# bot.py
# python-telegram-bot >= 21 (async)
# Deploy: GitHub + Railway (polling)

import os
import re
import html
import json
import sqlite3
import logging
import calendar as pycal
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time as dtime
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple, Dict, Any

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, BadRequest, Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("nails-booking-bot")

# -------------------------
# ENV
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")

TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"
TZ = ZoneInfo(TZ_NAME)

AUTO_CLEAN = (os.getenv("AUTO_CLEAN", "1").strip() or "1") == "1"
SALON_TITLE = os.getenv("SALON_TITLE", "Beauty Lounge").strip() or "Beauty Lounge"
MAPS_URL = os.getenv("MAPS_URL", "https://yandex.ru/maps/").strip() or "https://yandex.ru/maps/"
ADDRESS_TEXT = os.getenv("ADDRESS_TEXT", "Дальневосточный проспект 19 к 1, кв 69, этаж 10").strip() or \
               "Дальневосточный проспект 19 к 1, кв 69, этаж 10"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required (Railway Variables).")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required (Railway Variables).")

DB_PATH = os.getenv("DB_PATH", "bot.db").strip() or "bot.db"

# -------------------------
# Constants / UI
# -------------------------
TRACK_KEEP = 6  # keep last N message_ids (both user + bot) per chat

PHOTO_URLS: List[str] = [
    # Можно добавить ссылки на фото (https://...) или оставить пустым.
    # "https://example.com/photo1.jpg",
]

REPLY_BUTTONS = [
    "📅 Записаться",
    "💰 Цены",
    "👩‍🎨 Обо мне",
    "📍 Как нас найти",
    "📋 Мои записи",
    "⭐ Отзывы",
    "🛠 Админ панель",
    "🏠 Меню",
]

SERVICES = [
    ("💅 Маникюр", "manicure"),
    ("🦶 Педикюр", "pedicure"),
    ("✨ Наращивание", "extension"),
    ("🔧 Коррекция", "correction"),
]

SERVICE_LABEL_BY_KEY = {k: v for v, k in SERVICES}
SERVICE_KEY_BY_LABEL = {v: k for v, k in SERVICES}

WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

PRICE_TEXT = (
    "💰 <b>Прайс</b>\n\n"
    "✨ <b>Маникюр без покрытия</b> — 1300 ₽\n"
    "💅 <b>Маникюр с покрытием</b> — 2500 ₽\n"
    "🎨 <b>Маникюр с покрытием + дизайн</b> — 3000 ₽\n\n"
    "🦶 <b>Педикюр без покрытия</b> — 2000 ₽\n"
    "💖 <b>Педикюр + покрытие</b> — 2800 ₽\n"
    "👣 <b>Педикюр пальчики</b> — 1800 ₽\n"
    "🦶 <b>Обработка стоп</b> — 1500 ₽\n\n"
    "✨ <b>Наращивание ногтей</b> — от 3500 ₽\n"
    "🔧 <b>Коррекция ногтей</b> — от 2800 ₽\n"
    "🎨 <b>Дизайн</b> — от 50 ₽ / ноготь\n\n"
    "Подсказка: нажмите <b>📅 Записаться</b>, чтобы выбрать услугу, дату и время."
)

ABOUT_TEXT = (
    "👩‍🎨 <b>О мастере</b>\n\n"
    "Привет! Я <b>Ира</b> 💛\n"
    "Стаж: <b>7+ лет</b> в маникюре и педикюре.\n\n"
    "✅ Стерильность и безопасность: одноразовые расходники, обработка инструментов.\n"
    "✅ Качественные материалы и аккуратная работа.\n"
    "✅ Комфортная атмосфера и бережный подход.\n\n"
    "Хочу, чтобы вы уходили с идеальными ногтями и отличным настроением ✨"
)

FIND_US_TEXT = (
    "📍 <b>Как нас найти</b>\n\n"
    f"🏠 Адрес:\n<b>{html.escape(ADDRESS_TEXT)}</b>\n\n"
    f"🗺 Ссылка на карту:\n{html.escape(MAPS_URL)}\n\n"
    "Если нужно — напишите, подскажу ориентиры 😊"
)

WELCOME_TEXT = (
    f"✨ <b>{html.escape(SALON_TITLE)}</b> — запись на маникюр/педикюр в пару кликов!\n\n"
    "Как это работает:\n"
    "1) Нажмите <b>📅 Записаться</b>\n"
    "2) Выберите услугу\n"
    "3) Выберите дату и время\n"
    "4) Подтвердите запись ✅\n\n"
    "После этого мастер подтвердит запись, и вы получите уведомление.\n"
    "Для первого раза потребуется короткая регистрация (имя + телефон)."
)

# -------------------------
# DB helpers
# -------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init() -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                name        TEXT NOT NULL,
                phone       TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                service     TEXT NOT NULL,
                date        TEXT NOT NULL,   -- YYYY-MM-DD
                time        TEXT NOT NULL,   -- HH:MM
                comment     TEXT,
                status      TEXT NOT NULL,   -- pending/confirmed/cancelled
                created_at  TEXT NOT NULL,
                reminded    INTEGER NOT NULL DEFAULT 0,
                admin_note  TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_user_datetime
            ON bookings (user_id, date, time);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bookings_status_reminded
            ON bookings (status, reminded);
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                text        TEXT NOT NULL,
                created_at  TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
        """)
        # Optional key-value settings (future-proof)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key         TEXT PRIMARY KEY,
                value       TEXT NOT NULL
            );
        """)
        conn.commit()
    finally:
        conn.close()

def db_get_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()
    finally:
        conn.close()

def db_upsert_user(user_id: int, name: str, phone: str) -> None:
    now = now_tz().isoformat()
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users(user_id, name, phone, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                name=excluded.name,
                phone=excluded.phone
        """, (user_id, name, phone, now))
        conn.commit()
    finally:
        conn.close()

def db_create_booking(user_id: int, service_key: str, d_ymd: str, t_hm: str, comment: str, status: str = "pending") -> int:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bookings(user_id, service, date, time, comment, status, created_at, reminded)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (user_id, service_key, d_ymd, t_hm, comment, status, now_tz().isoformat()))
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()

def db_get_booking(booking_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.id = ?
        """, (booking_id,))
        return cur.fetchone()
    finally:
        conn.close()

def db_update_booking_status(booking_id: int, status: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET status = ? WHERE id = ?", (status, booking_id))
        conn.commit()
    finally:
        conn.close()

def db_set_booking_reminded(booking_id: int) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET reminded = 1 WHERE id = ?", (booking_id,))
        conn.commit()
    finally:
        conn.close()

def db_update_booking_comment(booking_id: int, comment: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET comment = ? WHERE id = ?", (comment, booking_id))
        conn.commit()
    finally:
        conn.close()

def db_list_user_future_bookings(user_id: int) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.user_id = ?
              AND b.status != 'cancelled'
            ORDER BY b.date ASC, b.time ASC
        """, (user_id,))
        rows = cur.fetchall()
    finally:
        conn.close()

    # Filter in Python by timezone-aware "now"
    now_ = now_tz()
    out = []
    for r in rows:
        dt = booking_dt_from_row(r)
        if dt >= now_:
            out.append(r)
    return out

def db_cancel_booking(booking_id: int) -> None:
    db_update_booking_status(booking_id, "cancelled")

def db_add_review(user_id: int, text: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO reviews(user_id, text, created_at)
            VALUES (?, ?, ?)
        """, (user_id, text, now_tz().isoformat()))
        conn.commit()
    finally:
        conn.close()

def db_list_last_reviews(limit: int = 5) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.*, u.name AS user_name
            FROM reviews r
            JOIN users u ON u.user_id = r.user_id
            ORDER BY r.id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()
    finally:
        conn.close()

def db_find_reminder_candidates(window_start: datetime, window_end: datetime) -> List[sqlite3.Row]:
    # Candidates: status pending/confirmed, reminded=0, datetime within [start, end)
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT b.*, u.name AS user_name, u.phone AS user_phone
            FROM bookings b
            JOIN users u ON u.user_id = b.user_id
            WHERE b.reminded = 0
              AND b.status IN ('pending', 'confirmed')
        """)
        rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for r in rows:
        dt = booking_dt_from_row(r)
        if window_start <= dt < window_end:
            out.append(r)
    return out

# -------------------------
# Time helpers
# -------------------------
def now_tz() -> datetime:
    return datetime.now(tz=TZ)

def booking_dt(date_ymd: str, time_hm: str) -> datetime:
    y, m, d = map(int, date_ymd.split("-"))
    hh, mm = map(int, time_hm.split(":"))
    return datetime(y, m, d, hh, mm, tzinfo=TZ)

def booking_dt_from_row(row: sqlite3.Row) -> datetime:
    return booking_dt(row["date"], row["time"])

def fmt_date_ru(d_ymd: str) -> str:
    y, m, d = map(int, d_ymd.split("-"))
    return f"{d:02d}.{m:02d}.{y}"

def fmt_datetime_ru(d_ymd: str, t_hm: str) -> str:
    return f"{fmt_date_ru(d_ymd)} {t_hm}"

# -------------------------
# Reply menu + normalize
# -------------------------
def build_reply_kb(is_admin: bool) -> ReplyKeyboardMarkup:
    # 2 columns
    buttons = [
        ["📅 Записаться", "💰 Цены"],
        ["👩‍🎨 Обо мне", "📍 Как нас найти"],
        ["📋 Мои записи", "⭐ Отзывы"],
        ["🏠 Меню", "🛠 Админ панель"],
    ]
    if not is_admin:
        buttons = [
            ["📅 Записаться", "💰 Цены"],
            ["👩‍🎨 Обо мне", "📍 Как нас найти"],
            ["📋 Мои записи", "⭐ Отзывы"],
            ["🏠 Меню"],
        ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, is_persistent=True)

def normalize_button(text: str) -> str:
    if not text:
        return ""
    s = " ".join(text.strip().lower().split())
    # remove leading emojis/spaces variants
    s = s.replace("🏠", "").replace("📅", "").replace("💰", "").replace("👩‍🎨", "").replace("📍", "").replace("📋", "").replace("⭐", "").replace("🛠", "")
    s = " ".join(s.split())
    aliases = {
        "записаться": "book",
        "запись": "book",
        "запис": "book",
        "цены": "prices",
        "прайс": "prices",
        "обо мне": "about",
        "о мастере": "about",
        "как нас найти": "find",
        "адрес": "find",
        "мои записи": "my",
        "записи": "my",
        "отзывы": "reviews",
        "отзыв": "reviews",
        "админ панель": "admin",
        "админка": "admin",
        "admin": "admin",
        "меню": "menu",
        "главное меню": "menu",
        "домой": "menu",
    }
    # exact match
    if s in aliases:
        return aliases[s]
    # allow if emoji versions left
    raw = " ".join(text.strip().lower().split())
    raw = raw.replace("  ", " ")
    for k, v in aliases.items():
        if raw.endswith(k):
            return v
    return ""

def is_admin_user(user_id: int) -> bool:
    return user_id == ADMIN_ID

# -------------------------
# Chat cleanup helpers
# -------------------------
def track_message_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int) -> None:
    if not AUTO_CLEAN:
        return
    cd = context.chat_data
    key = "tracked_message_ids"
    if key not in cd or not isinstance(cd.get(key), list):
        cd[key] = []
    cd[key].append(int(message_id))
    # avoid unbounded growth
    if len(cd[key]) > 60:
        cd[key] = cd[key][-30:]

async def cleanup_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, keep_last: int = TRACK_KEEP) -> None:
    if not AUTO_CLEAN:
        return
    ids = context.chat_data.get("tracked_message_ids", [])
    if not ids or len(ids) <= keep_last:
        return
    to_delete = ids[:-keep_last]
    still_keep = ids[-keep_last:]
    context.chat_data["tracked_message_ids"] = still_keep

    for mid in to_delete:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except BadRequest:
            # message can't be deleted (too old / already deleted / not found)
            continue
        except Forbidden:
            continue
        except TelegramError:
            continue

async def safe_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.HTML,
    disable_web_page_preview: bool = True,
) -> Optional[int]:
    chat = update.effective_chat
    if not chat:
        return None
    msg = await context.bot.send_message(
        chat_id=chat.id,
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=disable_web_page_preview,
    )
    track_message_id(context, chat.id, msg.message_id)
    # cleanup AFTER sending new message
    await cleanup_chat(context, chat.id)
    return msg.message_id

async def safe_edit_or_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    query=None,
    text: str,
    reply_markup=None,
    parse_mode: Optional[str] = ParseMode.HTML,
    disable_web_page_preview: bool = True,
) -> None:
    # Try edit message if callback; if fails, send new
    chat = update.effective_chat
    if query and query.message:
        try:
            await query.message.edit_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
            )
            # track edited message id too (still counts as visible)
            track_message_id(context, chat.id, query.message.message_id)
            await cleanup_chat(context, chat.id)
            return
        except BadRequest:
            pass
        except TelegramError:
            pass
    await safe_send(update, context, text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)

async def acknowledge_callback(query) -> None:
    try:
        await query.answer()
    except TelegramError:
        pass

# -------------------------
# State helpers
# -------------------------
def set_mode(context: ContextTypes.DEFAULT_TYPE, mode: Optional[str]) -> None:
    if mode:
        context.user_data["mode"] = mode
    else:
        context.user_data.pop("mode", None)

def get_mode(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("mode", "") or "")

def reset_booking_draft(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("draft", None)

def get_draft(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    if "draft" not in context.user_data or not isinstance(context.user_data.get("draft"), dict):
        context.user_data["draft"] = {}
    return context.user_data["draft"]

# -------------------------
# Booking UI builders
# -------------------------
def kb_services() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for label, key in SERVICES:
        row.append(InlineKeyboardButton(label, callback_data=f"svc:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")])
    return InlineKeyboardMarkup(rows)

def month_title_ru(year: int, month: int) -> str:
    return f"{MONTHS_RU[month-1]} {year}"

def clamp_month(year: int, month: int) -> Tuple[int, int]:
    if month < 1:
        return year - 1, 12
    if month > 12:
        return year + 1, 1
    return year, month

def kb_calendar(year: int, month: int, today: date) -> InlineKeyboardMarkup:
    cal = pycal.Calendar(firstweekday=0)  # Monday
    month_days = list(cal.itermonthdates(year, month))

    # header rows
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(f"📅 {month_title_ru(year, month)}", callback_data="noop")])
    rows.append([InlineKeyboardButton(d, callback_data="noop") for d in WEEKDAYS_RU])

    # 6 rows x 7 cols
    grid: List[List[InlineKeyboardButton]] = []
    week: List[InlineKeyboardButton] = []
    for d in month_days:
        if d.month != month:
            week.append(InlineKeyboardButton(" ", callback_data="noop"))
        else:
            if d < today:
                week.append(InlineKeyboardButton(f"{d.day}", callback_data="noop"))
            else:
                week.append(InlineKeyboardButton(f"{d.day}", callback_data=f"day:{d.isoformat()}"))
        if len(week) == 7:
            grid.append(week)
            week = []
    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="noop"))
        grid.append(week)

    # Ensure at least 5-6 rows for stable UI (like typical calendars)
    while len(grid) < 6:
        grid.append([InlineKeyboardButton(" ", callback_data="noop") for _ in range(7)])

    rows.extend(grid[:6])

    # nav
    prev_y, prev_m = clamp_month(year, month - 1)
    next_y, next_m = clamp_month(year, month + 1)

    rows.append([
        InlineKeyboardButton("◀️", callback_data=f"cal:{prev_y}-{prev_m:02d}"),
        InlineKeyboardButton("Сегодня", callback_data="cal:today"),
        InlineKeyboardButton("▶️", callback_data=f"cal:{next_y}-{next_m:02d}"),
    ])
    rows.append([
        InlineKeyboardButton("⬅️ Назад", callback_data="nav:services"),
    ])
    return InlineKeyboardMarkup(rows)

def time_slots_for_date(selected_date: date, now_dt: datetime) -> List[str]:
    # 08:00–23:00, step 30 minutes
    slots: List[str] = []
    start = dtime(8, 0)
    end = dtime(23, 0)
    cur = datetime.combine(selected_date, start, tzinfo=TZ)
    last = datetime.combine(selected_date, end, tzinfo=TZ)
    while cur <= last:
        if selected_date == now_dt.date():
            # hide past times (strictly <= now)
            if cur > now_dt:
                slots.append(cur.strftime("%H:%M"))
        else:
            slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=30)
    return slots

def kb_time_picker(date_iso: str, slots: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(f"🕒 Время: {fmt_date_ru(date_iso)}", callback_data="noop")])

    row: List[InlineKeyboardButton] = []
    for t in slots:
        row.append(InlineKeyboardButton(t, callback_data=f"time:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if len(slots) == 0:
        rows.append([InlineKeyboardButton("Нет доступного времени сегодня 🙈", callback_data="noop")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="nav:calendar")])
    return InlineKeyboardMarkup(rows)

def booking_summary_text(user: sqlite3.Row, draft: Dict[str, Any]) -> str:
    service_key = draft.get("service_key", "")
    d_ymd = draft.get("date", "")
    t_hm = draft.get("time", "")
    comment = (draft.get("comment") or "").strip()

    service_label = SERVICE_LABEL_BY_KEY.get(service_key, service_key or "—")

    text = (
        "🧾 <b>Подтверждение записи</b>\n\n"
        f"💎 Услуга: <b>{html.escape(service_label)}</b>\n"
        f"📅 Дата: <b>{html.escape(fmt_date_ru(d_ymd))}</b>\n"
        f"🕒 Время: <b>{html.escape(t_hm)}</b>\n\n"
        f"👤 Имя: <b>{html.escape(user['name'])}</b>\n"
        f"📞 Телефон: <b>{html.escape(user['phone'])}</b>\n"
    )
    if comment:
        text += f"\n💬 Комментарий: <i>{html.escape(comment)}</i>\n"
    text += "\nВыберите действие ниже 👇"
    return text

def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data="confirm:yes"),
            InlineKeyboardButton("✏️ Комментарий", callback_data="confirm:comment"),
        ],
        [
            InlineKeyboardButton("❌ Отменить", callback_data="confirm:cancel"),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="nav:time"),
        ],
    ])

def admin_booking_controls(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm:confirm:{booking_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"adm:cancel:{booking_id}"),
        ],
        [
            InlineKeyboardButton("💬 Написать клиенту", callback_data=f"adm:msg:{booking_id}"),
        ],
    ])

def kb_prices_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записаться", callback_data="nav:book")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

def kb_about_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📷 Фотогалерея", callback_data="about:photos")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

def kb_reviews_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Оставить отзыв", callback_data="reviews:write")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])

# -------------------------
# Menu actions
# -------------------------
async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "🏠 <b>Главное меню</b>\n\nВыберите раздел кнопками ниже 👇", reply_markup=kb)

async def ensure_registered_or_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[sqlite3.Row]:
    uid = update.effective_user.id if update.effective_user else 0
    user = db_get_user(uid)
    if user:
        return user

    kb = build_reply_kb(is_admin_user(uid))
    # Do not “lock” user; just set mode and explain
    set_mode(context, "await_name")
    await safe_send(
        update,
        context,
        "📝 Для записи нужна регистрация (один раз).\n\n"
        "Пожалуйста, напишите <b>ваше имя</b> 👇",
        reply_markup=kb,
    )
    return None

async def menu_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_registered_or_prompt(update, context)
    if not user:
        return
    reset_booking_draft(context)
    set_mode(context, None)
    draft = get_draft(context)
    draft.clear()
    await safe_send(
        update,
        context,
        "📅 <b>Запись</b>\n\nВыберите услугу:",
        reply_markup=kb_services(),
    )

async def menu_prices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    # keep reply menu visible + inline actions
    await safe_send(update, context, PRICE_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    await safe_send(update, context, "👇 Быстрые действия:", reply_markup=kb_prices_actions(), parse_mode=ParseMode.HTML)

async def menu_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    await safe_send(update, context, ABOUT_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    await safe_send(update, context, "👇", reply_markup=kb_about_actions(), parse_mode=ParseMode.HTML)

async def menu_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    await safe_send(update, context, FIND_US_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)

async def menu_my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = await ensure_registered_or_prompt(update, context)
    if not user:
        return
    rows = db_list_user_future_bookings(user["user_id"])
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    if not rows:
        await safe_send(update, context, "📋 <b>Мои записи</b>\n\nПока нет будущих записей.", reply_markup=kb)
        return

    text_lines = ["📋 <b>Мои записи</b>\n"]
    for r in rows[:10]:
        service_label = SERVICE_LABEL_BY_KEY.get(r["service"], r["service"])
        status = r["status"]
        status_emoji = "🟡" if status == "pending" else ("✅" if status == "confirmed" else "❌")
        text_lines.append(
            f"{status_emoji} <b>#{r['id']}</b> — {html.escape(service_label)}\n"
            f"📅 {html.escape(fmt_date_ru(r['date']))}  🕒 {html.escape(r['time'])}\n"
        )
    await safe_send(update, context, "\n".join(text_lines), reply_markup=kb)

    # Inline controls per booking (chunked)
    for r in rows[:6]:
        await safe_send(
            update,
            context,
            f"Действия для <b>#{r['id']}</b>:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отменить", callback_data=f"user:cancel:{r['id']}")],
                [InlineKeyboardButton("⬅️ Назад в меню", callback_data="nav:menu")],
            ]),
        )

async def menu_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    rows = db_list_last_reviews(5)
    if not rows:
        text = "⭐ <b>Отзывы</b>\n\nПока отзывов нет. Будете первым? 😊"
    else:
        parts = ["⭐ <b>Отзывы</b>\n"]
        for r in rows:
            name = r["user_name"]
            created = r["created_at"][:16].replace("T", " ")
            parts.append(f"🗣 <b>{html.escape(name)}</b> <i>({html.escape(created)})</i>\n{html.escape(r['text'])}\n")
        text = "\n".join(parts)
    await safe_send(update, context, text, reply_markup=kb)
    await safe_send(update, context, "👇", reply_markup=kb_reviews_actions())

async def menu_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin_user(uid):
        await safe_send(update, context, "⛔ Доступ только для администратора.")
        return

    # Basic dashboard
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='pending'")
        pending = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='confirmed'")
        confirmed = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE status='cancelled'")
        cancelled = cur.fetchone()["c"]
    finally:
        conn.close()

    now_ = now_tz()
    today_ymd = now_.date().isoformat()
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM bookings WHERE date=? AND status IN ('pending','confirmed')", (today_ymd,))
        today_cnt = cur.fetchone()["c"]
    finally:
        conn.close()

    text = (
        "🛠 <b>Админ панель</b>\n\n"
        f"🟡 Ожидают подтверждения: <b>{pending}</b>\n"
        f"✅ Подтверждено: <b>{confirmed}</b>\n"
        f"❌ Отменено: <b>{cancelled}</b>\n"
        f"📅 Записей на сегодня: <b>{today_cnt}</b>\n\n"
        "Выберите действие:"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записи на 7 дней", callback_data="adm:list:7")],
        [InlineKeyboardButton("🟡 Только pending", callback_data="adm:list:pending")],
        [InlineKeyboardButton("✅ Только confirmed", callback_data="adm:list:confirmed")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")],
    ])
    await safe_send(update, context, text, reply_markup=kb)

# -------------------------
# Dispatch for reply menu (MUST be first)
# -------------------------
async def dispatch_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str) -> bool:
    if not key:
        return False

    if key == "menu":
        await send_menu(update, context)
        return True
    if key == "book":
        await menu_book(update, context)
        return True
    if key == "prices":
        await menu_prices(update, context)
        return True
    if key == "about":
        await menu_about(update, context)
        return True
    if key == "find":
        await menu_find(update, context)
        return True
    if key == "my":
        await menu_my_bookings(update, context)
        return True
    if key == "reviews":
        await menu_reviews(update, context)
        return True
    if key == "admin":
        await menu_admin(update, context)
        return True

    return False

# -------------------------
# /start
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        track_message_id(context, update.effective_chat.id, update.message.message_id)

    uid = update.effective_user.id if update.effective_user else 0
    kb = build_reply_kb(is_admin_user(uid))

    await safe_send(update, context, WELCOME_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)

    user = db_get_user(uid)
    if user:
        set_mode(context, None)
        await safe_send(update, context, "✅ Вы уже зарегистрированы!\n\nНажмите <b>📅 Записаться</b> — и выберите удобное время 👇", reply_markup=kb)
        return

    set_mode(context, "await_name")
    await safe_send(update, context, "📝 Давайте зарегистрируемся (один раз).\n\nНапишите <b>ваше имя</b> 👇", reply_markup=kb)

# -------------------------
# Registration handlers
# -------------------------
def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    s = s.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    # allow leading +
    digits = re.sub(r"[^\d+]", "", s)
    # If has +, keep it for now
    only_digits = re.sub(r"\D", "", s)
    if len(only_digits) < 10 or len(only_digits) > 12:
        return None
    # normalize to +7XXXXXXXXXX when possible
    if only_digits.startswith("8") and len(only_digits) == 11:
        only_digits = "7" + only_digits[1:]
    if only_digits.startswith("7") and len(only_digits) == 11:
        return "+" + only_digits
    # if 10 digits assume РФ without country
    if len(only_digits) == 10:
        return "+7" + only_digits
    # fallback
    if digits.startswith("+") and 10 <= len(only_digits) <= 12:
        return "+" + only_digits
    return "+" + only_digits

async def ask_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    kb = build_reply_kb(is_admin_user(update.effective_user.id))
    contact_kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("📲 Отправить номер", request_contact=True)],
            ["🏠 Меню"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )
    await safe_send(
        update,
        context,
        "📞 Теперь отправьте <b>номер телефона</b>:\n\n"
        "• Нажмите кнопку <b>📲 Отправить номер</b>\n"
        "или\n"
        "• Введите номер вручную (например: <code>+79991234567</code>)",
        reply_markup=contact_kb,
    )
    # keep mode await_phone
    set_mode(context, "await_phone")

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        track_message_id(context, update.effective_chat.id, update.message.message_id)

    # Reply buttons must still work: if user sent contact, it's registration flow only
    uid = update.effective_user.id if update.effective_user else 0
    mode = get_mode(context)
    if mode != "await_phone":
        # ignore if not waiting phone
        kb = build_reply_kb(is_admin_user(uid))
        await safe_send(update, context, "📲 Контакт получен, но сейчас он не требуется. Нажмите нужный раздел в меню 👇", reply_markup=kb)
        return

    contact = update.message.contact
    phone_raw = contact.phone_number if contact else ""
    phone = normalize_phone(phone_raw)
    if not phone:
        await safe_send(update, context, "❌ Не смог распознать номер. Попробуйте ещё раз или введите вручную (+7/8...).")
        return

    name = (context.user_data.get("reg_name") or "").strip()
    if not name:
        # Ask name again, do not break
        set_mode(context, "await_name")
        await safe_send(update, context, "📝 Похоже, имя не сохранено. Напишите, пожалуйста, <b>ваше имя</b> 👇")
        return

    db_upsert_user(uid, name=name, phone=phone)
    context.user_data.pop("reg_name", None)
    set_mode(context, None)

    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "✅ Готово! Регистрация завершена.\n\nНажмите <b>📅 Записаться</b> — выберем услугу, дату и время 👇", reply_markup=kb)

# -------------------------
# Text handler (reply routing FIRST)
# -------------------------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    chat_id = update.effective_chat.id
    track_message_id(context, chat_id, update.message.message_id)

    text = (update.message.text or "").strip()
    key = normalize_button(text)

    # 1) STRICT reply routing FIRST
    if key:
        handled = await dispatch_reply(update, context, key)
        if handled:
            return

    # 2) Modes handling
    uid = update.effective_user.id if update.effective_user else 0
    mode = get_mode(context)

    # Admin message mode (admin can still use reply buttons — already handled above)
    if mode == "admin_msg":
        if not is_admin_user(uid):
            set_mode(context, None)
            await safe_send(update, context, "⛔ Режим админ-сообщения недоступен.")
            return
        payload = context.user_data.get("admin_msg_payload") or {}
        booking_id = int(payload.get("booking_id") or 0)
        target_user_id = int(payload.get("user_id") or 0)
        if not booking_id or not target_user_id:
            set_mode(context, None)
            context.user_data.pop("admin_msg_payload", None)
            await safe_send(update, context, "⚠️ Не удалось определить получателя. Откройте действие заново в админ-панели.")
            return

        msg_text = text
        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"💬 Сообщение от мастера:\n\n{html.escape(msg_text)}",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            await safe_send(update, context, "❌ Не удалось отправить сообщение клиенту (возможно, он не начинал чат с ботом).")
        else:
            await safe_send(update, context, f"✅ Отправлено клиенту (booking #{booking_id}).")

        set_mode(context, None)
        context.user_data.pop("admin_msg_payload", None)
        return

    # Registration: await_name
    if mode == "await_name":
        name = text
        if len(name) < 2:
            await safe_send(update, context, "❌ Имя слишком короткое. Напишите, пожалуйста, имя ещё раз 👇")
            return
        # store temp
        context.user_data["reg_name"] = name
        await ask_phone(update, context)
        return

    # Registration: await_phone
    if mode == "await_phone":
        phone = normalize_phone(text)
        if not phone:
            await safe_send(update, context, "❌ Номер выглядит некорректно.\nПример: <code>+79991234567</code> или <code>89991234567</code>\nПопробуйте ещё раз 👇")
            return
        name = (context.user_data.get("reg_name") or "").strip()
        if not name:
            set_mode(context, "await_name")
            await safe_send(update, context, "📝 Сначала нужно имя. Напишите, пожалуйста, <b>ваше имя</b> 👇")
            return
        db_upsert_user(uid, name=name, phone=phone)
        context.user_data.pop("reg_name", None)
        set_mode(context, None)
        kb = build_reply_kb(is_admin_user(uid))
        await safe_send(update, context, "✅ Готово! Регистрация завершена.\n\nНажмите <b>📅 Записаться</b> 👇", reply_markup=kb)
        return

    # Booking comment mode
    if mode == "await_comment":
        draft = get_draft(context)
        draft["comment"] = text
        set_mode(context, None)
        user = db_get_user(uid)
        if not user:
            await ensure_registered_or_prompt(update, context)
            return
        await safe_send(update, context, booking_summary_text(user, draft), reply_markup=kb_confirm())
        return

    # Review mode
    if mode == "await_review":
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        if len(text) < 4:
            await safe_send(update, context, "❌ Отзыв слишком короткий. Напишите чуть подробнее 🙏")
            return
        db_add_review(uid, text)
        set_mode(context, None)
        await safe_send(update, context, "Спасибо за отзыв! 💛\nОн сохранён ✅")
        await menu_reviews(update, context)
        return

    # Default fallthrough
    kb = build_reply_kb(is_admin_user(uid))
    await safe_send(update, context, "Я на связи 😊\n\nВыберите нужный раздел в меню ниже 👇", reply_markup=kb)

# -------------------------
# Callback router
# -------------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    await acknowledge_callback(query)

    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id and query.message:
        track_message_id(context, chat_id, query.message.message_id)

    data = query.data or ""
    uid = update.effective_user.id if update.effective_user else 0

    # NOOP / unavailable
    if data == "noop":
        try:
            await query.answer("Недоступно", show_alert=False)
        except TelegramError:
            pass
        return

    # Navigation
    if data.startswith("nav:"):
        dest = data.split(":", 1)[1]
        if dest == "menu":
            set_mode(context, None)
            reset_booking_draft(context)
            await safe_edit_or_send(update, context, query=query, text="🏠 <b>Главное меню</b>\n\nВыберите раздел кнопками ниже 👇", reply_markup=None)
            # show menu via reply keyboard (message)
            await send_menu(update, context)
            return

        if dest == "book":
            await menu_book(update, context)
            return

        if dest == "services":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            set_mode(context, None)
            await safe_edit_or_send(update, context, query=query, text="📅 <b>Запись</b>\n\nВыберите услугу:", reply_markup=kb_services())
            return

        if dest == "calendar":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            draft = get_draft(context)
            if not draft.get("service_key"):
                await safe_edit_or_send(update, context, query=query, text="Сначала выберите услугу 👇", reply_markup=kb_services())
                return
            now_ = now_tz()
            today = now_.date()
            # show current month
            await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(today.year, today.month, today))
            return

        if dest == "time":
            user = await ensure_registered_or_prompt(update, context)
            if not user:
                return
            draft = get_draft(context)
            if not draft.get("date"):
                # back to calendar
                now_ = now_tz()
                await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(now_.year, now_.month, now_.date()))
                return
            now_ = now_tz()
            selected = date.fromisoformat(draft["date"])
            slots = time_slots_for_date(selected, now_)
            await safe_edit_or_send(update, context, query=query, text="Выберите время 👇", reply_markup=kb_time_picker(draft["date"], slots))
            return

    # Service selection
    if data.startswith("svc:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        service_key = data.split(":", 1)[1]
        if service_key not in SERVICE_LABEL_BY_KEY:
            await safe_edit_or_send(update, context, query=query, text="❌ Неизвестная услуга. Выберите ещё раз 👇", reply_markup=kb_services())
            return
        draft = get_draft(context)
        draft["service_key"] = service_key
        draft.pop("date", None)
        draft.pop("time", None)
        set_mode(context, None)

        now_ = now_tz()
        today = now_.date()
        await safe_edit_or_send(
            update,
            context,
            query=query,
            text=f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY[service_key])}</b>\n\nВыберите дату 👇",
            reply_markup=kb_calendar(today.year, today.month, today),
        )
        return

    # Calendar navigation
    if data.startswith("cal:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        now_ = now_tz()
        today = now_.date()
        token = data.split(":", 1)[1]
        if token == "today":
            y, m = today.year, today.month
        else:
            try:
                y_s, m_s = token.split("-", 1)
                y, m = int(y_s), int(m_s)
            except Exception:
                y, m = today.year, today.month

        # Prevent navigating too far back (before current month)
        if (y, m) < (today.year, today.month):
            y, m = today.year, today.month

        await safe_edit_or_send(update, context, query=query, text="Выберите дату 👇", reply_markup=kb_calendar(y, m, today))
        return

    # Day selection
    if data.startswith("day:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        d_iso = data.split(":", 1)[1]
        try:
            selected = date.fromisoformat(d_iso)
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректная дата. Попробуйте ещё раз.", reply_markup=None)
            return

        now_ = now_tz()
        if selected < now_.date():
            try:
                await query.answer("Эта дата уже прошла", show_alert=False)
            except TelegramError:
                pass
            return

        draft = get_draft(context)
        draft["date"] = d_iso
        draft.pop("time", None)
        set_mode(context, None)

        slots = time_slots_for_date(selected, now_)
        await safe_edit_or_send(update, context, query=query, text="Выберите время 👇", reply_markup=kb_time_picker(d_iso, slots))
        return

    # Time selection
    if data.startswith("time:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        t_hm = data.split(":", 1)[1]
        if not re.fullmatch(r"\d{2}:\d{2}", t_hm):
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректное время. Выберите ещё раз.", reply_markup=None)
            return

        draft = get_draft(context)
        if not draft.get("service_key") or not draft.get("date"):
            await safe_edit_or_send(update, context, query=query, text="⚠️ Начните запись заново: выберите услугу 👇", reply_markup=kb_services())
            return

        # Validate not past (especially for today)
        now_ = now_tz()
        dt = booking_dt(draft["date"], t_hm)
        if dt <= now_:
            try:
                await query.answer("Это время уже прошло", show_alert=False)
            except TelegramError:
                pass
            return

        draft["time"] = t_hm
        set_mode(context, None)

        await safe_edit_or_send(update, context, query=query, text=booking_summary_text(user, draft), reply_markup=kb_confirm())
        return

    # Confirm flow
    if data.startswith("confirm:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        action = data.split(":", 1)[1]
        draft = get_draft(context)

        if action == "cancel":
            reset_booking_draft(context)
            set_mode(context, None)
            await safe_edit_or_send(update, context, query=query, text="❌ Запись отменена.\n\nВозвращаю в меню 👇", reply_markup=None)
            await send_menu(update, context)
            return

        if action == "comment":
            set_mode(context, "await_comment")
            await safe_edit_or_send(update, context, query=query, text="✏️ Напишите комментарий к записи (пожелания, дизайн, нюансы) 👇\n\nМожно коротко 🙂", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="nav:time")],
            ]))
            return

        if action == "yes":
            # Validate draft fully
            service_key = draft.get("service_key")
            d_ymd = draft.get("date")
            t_hm = draft.get("time")
            comment = (draft.get("comment") or "").strip()

            if not (service_key and d_ymd and t_hm):
                await safe_edit_or_send(update, context, query=query, text="⚠️ Не хватает данных записи. Начните заново: 📅 Записаться", reply_markup=None)
                reset_booking_draft(context)
                return

            # Create booking as pending
            booking_id = db_create_booking(user["user_id"], service_key, d_ymd, t_hm, comment, status="pending")

            # Client message
            await safe_edit_or_send(
                update,
                context,
                query=query,
                text=(
                    "✅ <b>Запись создана!</b>\n\n"
                    "Ожидайте подтверждения мастера 🟡\n\n"
                    f"Номер записи: <b>#{booking_id}</b>\n"
                    f"Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(service_key, service_key))}</b>\n"
                    f"Дата/время: <b>{html.escape(fmt_datetime_ru(d_ymd, t_hm))}</b>\n"
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="nav:menu")]]),
            )

            # Notify admin
            admin_text = (
                "🆕 <b>Новая запись</b>\n\n"
                f"🆔 <b>#{booking_id}</b>\n"
                f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(service_key, service_key))}</b>\n"
                f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(d_ymd, t_hm))}</b>\n\n"
                f"👤 Клиент: <b>{html.escape(user['name'])}</b>\n"
                f"📞 Телефон: <b>{html.escape(user['phone'])}</b>\n"
            )
            if comment:
                admin_text += f"\n💬 Комментарий: <i>{html.escape(comment)}</i>\n"

            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=admin_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=admin_booking_controls(booking_id),
                    disable_web_page_preview=True,
                )
            except TelegramError as e:
                log.warning("Failed to notify admin: %s", e)

            reset_booking_draft(context)
            set_mode(context, None)
            return

    # About photos
    if data == "about:photos":
        kb = build_reply_kb(is_admin_user(uid))
        if not PHOTO_URLS:
            await safe_edit_or_send(update, context, query=query, text="📷 Фото скоро добавлю ✨", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
            ]))
            return

        await safe_edit_or_send(update, context, query=query, text="📷 <b>Фотогалерея</b>\n\nЛистайте фото ниже 👇", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
        ]))
        # send photos as separate messages (won't break without them)
        for url in PHOTO_URLS[:10]:
            try:
                msg = await context.bot.send_photo(chat_id=chat_id, photo=url)
                track_message_id(context, chat_id, msg.message_id)
                await cleanup_chat(context, chat_id)
            except TelegramError:
                continue
        return

    # Reviews write
    if data == "reviews:write":
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        set_mode(context, "await_review")
        await safe_edit_or_send(update, context, query=query, text="✍️ Напишите ваш отзыв одним сообщением 👇\n\n(Reply-кнопки меню по-прежнему работают.)", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
        ]))
        return

    # User cancels booking from "My bookings"
    if data.startswith("user:cancel:"):
        user = await ensure_registered_or_prompt(update, context)
        if not user:
            return
        try:
            booking_id = int(data.split(":")[2])
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="❌ Некорректный номер записи.", reply_markup=None)
            return

        row = db_get_booking(booking_id)
        if not row or int(row["user_id"]) != int(user["user_id"]):
            await safe_edit_or_send(update, context, query=query, text="⚠️ Запись не найдена.", reply_markup=None)
            return

        db_cancel_booking(booking_id)

        await safe_edit_or_send(update, context, query=query, text=f"❌ Запись <b>#{booking_id}</b> отменена.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Мои записи", callback_data="nav:menu")],
        ]))

        # notify admin
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "❌ <b>Отмена записи клиентом</b>\n\n"
                    f"🆔 <b>#{booking_id}</b>\n"
                    f"👤 Клиент: <b>{html.escape(row['user_name'])}</b>\n"
                    f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(row['date'], row['time']))}</b>\n"
                    f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(row['service'], row['service']))}</b>\n"
                ),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass
        return

    # Admin list
    if data.startswith("adm:list:"):
        if not is_admin_user(uid):
            await safe_edit_or_send(update, context, query=query, text="⛔ Доступ только для администратора.", reply_markup=None)
            return

        token = data.split(":", 2)[2]
        now_ = now_tz()
        conn = db_connect()
        try:
            cur = conn.cursor()
            rows = []
            if token == "7":
                end_date = (now_.date() + timedelta(days=7)).isoformat()
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status IN ('pending','confirmed')
                      AND b.date >= ?
                      AND b.date <= ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 50
                """, (now_.date().isoformat(), end_date))
                rows = cur.fetchall()
                title = "📅 Записи на 7 дней"
            elif token in ("pending", "confirmed"):
                cur.execute("""
                    SELECT b.*, u.name AS user_name, u.phone AS user_phone
                    FROM bookings b
                    JOIN users u ON u.user_id = b.user_id
                    WHERE b.status = ?
                    ORDER BY b.date ASC, b.time ASC
                    LIMIT 50
                """, (token,))
                rows = cur.fetchall()
                title = f"🧾 Список: {token}"
            else:
                title = "🧾 Список"
        finally:
            conn.close()

        if not rows:
            await safe_edit_or_send(update, context, query=query, text=f"{title}\n\nПока пусто.", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
            ]))
            return

        lines = [f"<b>{html.escape(title)}</b>\n"]
        for r in rows[:20]:
            status = r["status"]
            status_emoji = "🟡" if status == "pending" else ("✅" if status == "confirmed" else "❌")
            lines.append(
                f"{status_emoji} <b>#{r['id']}</b> — {html.escape(SERVICE_LABEL_BY_KEY.get(r['service'], r['service']))}\n"
                f"📅 {html.escape(fmt_date_ru(r['date']))}  🕒 {html.escape(r['time'])}\n"
                f"👤 {html.escape(r['user_name'])}  📞 {html.escape(r['user_phone'])}\n"
            )

        await safe_edit_or_send(update, context, query=query, text="\n".join(lines), reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
        ]))
        return

    # Admin confirm/cancel/message
    if data.startswith("adm:"):
        if not is_admin_user(uid):
            await safe_edit_or_send(update, context, query=query, text="⛔ Доступ только для администратора.", reply_markup=None)
            return

        parts = data.split(":")
        if len(parts) < 3:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Некорректная команда админа.", reply_markup=None)
            return
        action = parts[1]
        try:
            booking_id = int(parts[2])
        except Exception:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Некорректный ID записи.", reply_markup=None)
            return

        row = db_get_booking(booking_id)
        if not row:
            await safe_edit_or_send(update, context, query=query, text="⚠️ Запись не найдена.", reply_markup=None)
            return

        if action == "confirm":
            db_update_booking_status(booking_id, "confirmed")
            # client notify
            try:
                await context.bot.send_message(
                    chat_id=int(row["user_id"]),
                    text=(
                        "✅ <b>Запись подтверждена!</b>\n\n"
                        f"🆔 <b>#{booking_id}</b>\n"
                        f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(row['service'], row['service']))}</b>\n"
                        f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(row['date'], row['time']))}</b>\n\n"
                        f"📍 Адрес: <b>{html.escape(ADDRESS_TEXT)}</b>\n"
                        f"🗺 Карта: {html.escape(MAPS_URL)}\n\n"
                        "До встречи! 💛"
                    ),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except TelegramError:
                pass

            await safe_edit_or_send(
                update,
                context,
                query=query,
                text=f"✅ Подтверждено: <b>#{booking_id}</b>",
                reply_markup=admin_booking_controls(booking_id),
            )
            return

        if action == "cancel":
            db_update_booking_status(booking_id, "cancelled")
            # client notify
            try:
                await context.bot.send_message(
                    chat_id=int(row["user_id"]),
                    text=(
                        "❌ <b>Запись отменена</b>\n\n"
                        f"🆔 <b>#{booking_id}</b>\n"
                        f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(row['service'], row['service']))}</b>\n"
                        f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(row['date'], row['time']))}</b>\n\n"
                        "Если хотите — выберите другое время через 📅 Записаться."
                    ),
                    parse_mode=ParseMode.HTML,
                )
            except TelegramError:
                pass

            await safe_edit_or_send(
                update,
                context,
                query=query,
                text=f"❌ Отменено: <b>#{booking_id}</b>",
                reply_markup=admin_booking_controls(booking_id),
            )
            return

        if action == "msg":
            # enter admin message mode
            set_mode(context, "admin_msg")
            context.user_data["admin_msg_payload"] = {"booking_id": booking_id, "user_id": int(row["user_id"])}
            await safe_edit_or_send(
                update,
                context,
                query=query,
                text=(
                    f"💬 <b>Сообщение клиенту</b>\n\n"
                    f"Запись <b>#{booking_id}</b>\n"
                    f"Клиент: <b>{html.escape(row['user_name'])}</b>\n\n"
                    "Напишите текст одним сообщением 👇\n"
                    "(Reply-кнопки админа продолжают работать.)"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="nav:menu")]
                ]),
            )
            return

    # fallback for unknown callback
    await safe_edit_or_send(update, context, query=query, text="⚠️ Не понял действие. Вернитесь в меню 👇", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🏠 Меню", callback_data="nav:menu")]
    ]))

# -------------------------
# Reminder job
# -------------------------
async def reminders_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now_ = now_tz()
    window_start = now_ + timedelta(hours=23)
    window_end = now_ + timedelta(hours=25)

    candidates = db_find_reminder_candidates(window_start, window_end)
    if not candidates:
        return

    for r in candidates:
        booking_id = int(r["id"])
        try:
            dt = booking_dt_from_row(r)
        except Exception:
            continue

        # Build "tomorrow/через 24 часа"
        delta = dt - now_
        label = "завтра" if 20 <= delta.total_seconds() / 3600 <= 30 else "через 24 часа"

        text = (
            f"⏰ <b>Напоминание о записи {label}</b>\n\n"
            f"💎 Услуга: <b>{html.escape(SERVICE_LABEL_BY_KEY.get(r['service'], r['service']))}</b>\n"
            f"📅 Дата/время: <b>{html.escape(fmt_datetime_ru(r['date'], r['time']))}</b>\n\n"
            f"📍 Адрес: <b>{html.escape(ADDRESS_TEXT)}</b>\n"
            f"🗺 Ссылка на карту: {html.escape(MAPS_URL)}\n\n"
            "До встречи! 💛"
        )
        try:
            await context.bot.send_message(
                chat_id=int(r["user_id"]),
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramError:
            # still mark reminded to avoid spam loops
            db_set_booking_reminded(booking_id)
            continue

        db_set_booking_reminded(booking_id)

# -------------------------
# Main
# -------------------------
def main() -> None:
    db_init()

    app = Application.builder().token(BOT_TOKEN).build()

    # Job queue reminder every 15 minutes
    app.job_queue.run_repeating(reminders_job, interval=15 * 60, first=30)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    log.info("Starting bot (polling). TZ=%s AUTO_CLEAN=%s ADMIN_ID=%s", TZ_NAME, AUTO_CLEAN, ADMIN_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
