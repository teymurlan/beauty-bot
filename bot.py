# bot.py
# python-telegram-bot >= 21
# One-file booking bot for manicure/pedicure (GitHub + Railway, polling)

import os
import re
import html
import sqlite3
import logging
import calendar as cal
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, Dict, List, Tuple

from zoneinfo import ZoneInfo

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("nails-booking-bot")

# =========================
# ENV (Railway Variables)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID_STR = os.getenv("ADMIN_ID", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required (Railway Variables).")
if not ADMIN_ID_STR:
    raise RuntimeError("ADMIN_ID is required (Railway Variables).")

try:
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    raise RuntimeError("ADMIN_ID must be an integer Telegram user id.")

DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip() or "data.sqlite3"
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"

SALON_NAME = os.getenv("SALON_NAME", "Запись к мастеру").strip() or "Запись к мастеру"
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()
YANDEX_MAP_URL = os.getenv("YANDEX_MAP_URL", "").strip()

ADDRESS = os.getenv("ADDRESS", "").strip()
HOW_TO_FIND = os.getenv("HOW_TO_FIND", "").strip()
DOORPHONE = os.getenv("DOORPHONE", "").strip()
GATE = os.getenv("GATE", "").strip()
FLOOR = os.getenv("FLOOR", "").strip()
APARTMENT = os.getenv("APARTMENT", "").strip()

MASTER_NAME = os.getenv("MASTER_NAME", "Мастер").strip() or "Мастер"
MASTER_EXPERIENCE = os.getenv("MASTER_EXPERIENCE", "").strip()
MASTER_TEXT = os.getenv("MASTER_TEXT", "").strip()
MASTER_PHOTO = os.getenv("MASTER_PHOTO", "").strip()  # file_id or URL

WORK_START_STR = os.getenv("WORK_START", "08:00").strip() or "08:00"
WORK_END_STR = os.getenv("WORK_END", "23:00").strip() or "23:00"
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"

# =========================
# SERVICES
# =========================
SERVICES: Dict[str, Tuple[str, int]] = {
    "mn_no": ("Маникюр без покрытия", 1300),
    "mn_cov": ("Маникюр с покрытием", 2500),
    "mn_cov_design": ("Маникюр с покрытием + дизайн", 3000),
    "pd_no": ("Педикюр без покрытия", 2000),
    "pd_cov": ("Педикюр + покрытие", 2800),
    "pd_toes": ("Педикюр пальчики", 1800),
    "pd_heels": ("Педикюр обработка стопы", 1500),
    "ext": ("Наращивание ногтей (от)", 3500),
    "corr": ("Коррекция ногтей (от)", 2800),
    "design": ("Дизайн ногтей (за ноготок, от)", 50),
}
PAGE_SIZE = 5

# =========================
# UI: Reply menu (always available)
# =========================
REPLY_MENU = ReplyKeyboardMarkup(
    keyboard=[
        ["📅 Записаться", "💅 Услуги"],
        ["👩‍🎨 Обо мне", "📋 Мои записи"],
        ["❌ Отменить запись", "❓ Возник вопрос"],
        ["🏠 Меню", "🛠 Админ-панель"],
    ],
    resize_keyboard=True,
)

# =========================
# Helpers: time & parsing
# =========================
def tz() -> ZoneInfo:
    try:
        return ZoneInfo(TZ_NAME)
    except Exception:
        return ZoneInfo("Europe/Moscow")


def now_tz() -> datetime:
    return datetime.now(tz())


def parse_hhmm(s: str, default: dtime) -> dtime:
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s or "")
    if not m:
        return default
    hh = max(0, min(23, int(m.group(1))))
    mm = max(0, min(59, int(m.group(2))))
    return dtime(hour=hh, minute=mm)


WORK_START = parse_hhmm(WORK_START_STR, dtime(8, 0))
WORK_END = parse_hhmm(WORK_END_STR, dtime(23, 0))

# =========================
# DB
# =========================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init() -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                phone TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service_key TEXT,
                service_title TEXT,
                price INTEGER,
                book_date TEXT,
                book_time TEXT,
                comment TEXT,
                status TEXT,
                created_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def db_get_user(tg_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
        return cur.fetchone()
    finally:
        conn.close()


def db_upsert_user(tg_id: int, username: str, full_name: str, phone: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users(tg_id, username, full_name, phone, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                phone=excluded.phone
            """,
            (tg_id, username, full_name, phone, now_tz().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def db_create_booking(
    user_id: int,
    service_key: str,
    service_title: str,
    price: int,
    book_date: str,
    book_time: str,
    comment: str,
) -> int:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bookings(user_id, service_key, service_title, price,
                                 book_date, book_time, comment, status, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                user_id,
                service_key,
                service_title,
                price,
                book_date,
                book_time,
                comment,
                "pending",
                now_tz().isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def db_get_booking(bid: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM bookings WHERE id = ?", (bid,))
        return cur.fetchone()
    finally:
        conn.close()


def db_set_booking_status(bid: int, status: str) -> None:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE bookings SET status=? WHERE id=?", (status, bid))
        conn.commit()
    finally:
        conn.close()


def db_is_slot_busy(book_date: str, book_time: str) -> bool:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM bookings
            WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
            LIMIT 1
            """,
            (book_date, book_time),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def db_user_bookings(tg_id: int, limit: int = 25) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM bookings
            WHERE user_id = ?
            ORDER BY book_date DESC, book_time DESC
            LIMIT ?
            """,
            (tg_id, limit),
        )
        return cur.fetchall()
    finally:
        conn.close()


def db_user_active_bookings(tg_id: int, limit: int = 25) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM bookings
            WHERE user_id = ? AND status IN ('pending','confirmed')
            ORDER BY book_date ASC, book_time ASC
            LIMIT ?
            """,
            (tg_id, limit),
        )
        return cur.fetchall()
    finally:
        conn.close()


def db_admin_upcoming(limit: int = 25) -> List[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT b.*, u.username, u.full_name, u.phone
            FROM bookings b
            LEFT JOIN users u ON u.tg_id = b.user_id
            WHERE b.status IN ('pending','confirmed')
            ORDER BY b.book_date ASC, b.book_time ASC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()
    finally:
        conn.close()


# =========================
# State & Draft
# =========================
@dataclass
class Draft:
    service_key: str = ""
    service_title: str = ""
    price: int = 0
    day: str = ""   # YYYY-MM-DD
    time: str = ""  # HH:MM
    comment: str = ""


def get_stage(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("stage", "")


def set_stage(context: ContextTypes.DEFAULT_TYPE, stage: str) -> None:
    context.user_data["stage"] = stage or ""


def clear_stage(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("stage", None)


def get_draft(context: ContextTypes.DEFAULT_TYPE) -> Draft:
    d = context.user_data.get("draft")
    if isinstance(d, Draft):
        return d
    d = Draft()
    context.user_data["draft"] = d
    return d


def clear_draft(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["draft"] = Draft()


# =========================
# One-screen rendering
# =========================
async def safe_delete(bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def safe_edit_text(bot, chat_id: int, message_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        return True
    except Exception:
        return False


async def render_screen(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Edits one 'screen' message when possible, else sends a new one and stores screen_mid."""
    chat_id = update.effective_chat.id
    bot = context.bot
    screen_mid = context.user_data.get("screen_mid")

    text = text or " "

    # Try edit
    if isinstance(screen_mid, int):
        ok = await safe_edit_text(bot, chat_id, screen_mid, text, reply_markup=keyboard)
        if ok:
            return

    # Send new
    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        context.user_data["screen_mid"] = msg.message_id
    except Exception as e:
        log.exception("Failed to send screen message: %s", e)


async def render_about_with_photo(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """
    If MASTER_PHOTO exists, try to send photo (and delete previous about photo message to avoid clutter).
    Still keep 'screen' message as primary screen.
    """
    chat_id = update.effective_chat.id
    bot = context.bot

    # First update screen (so user instantly sees reaction), then deal with photo
    await render_screen(update, context, text, keyboard)

    if not MASTER_PHOTO:
        return

    old_photo_mid = context.user_data.get("about_photo_mid")
    if isinstance(old_photo_mid, int):
        await safe_delete(bot, chat_id, old_photo_mid)
        context.user_data.pop("about_photo_mid", None)

    try:
        m = await bot.send_photo(chat_id=chat_id, photo=MASTER_PHOTO)
        context.user_data["about_photo_mid"] = m.message_id
    except Exception:
        # if photo fails, silently ignore
        pass


# =========================
# Menu routing (hard-first)
# =========================
def normalize_button(text: str) -> str:
    t = (text or "").strip()

    mapping = {
        "📅 Записаться": "BOOK",
        "💅 Услуги": "SERVICES",
        "👩‍🎨 Обо мне": "ABOUT",
        "📋 Мои записи": "MY",
        "❌ Отменить запись": "CANCEL_MENU",
        "❓ Возник вопрос": "ASK",
        "🏠 Меню": "MENU",
        "🛠 Админ-панель": "ADMIN",
    }
    if t in mapping:
        return mapping[t]

    # fallback by emoji or words, to be resilient
    t_low = t.lower()
    if "запис" in t_low:
        return "BOOK"
    if "услуг" in t_low:
        return "SERVICES"
    if "обо" in t_low or "мастер" in t_low:
        return "ABOUT"
    if "мои" in t_low and "зап" in t_low:
        return "MY"
    if "отмен" in t_low:
        return "CANCEL_MENU"
    if "вопрос" in t_low:
        return "ASK"
    if "меню" in t_low:
        return "MENU"
    if "админ" in t_low:
        return "ADMIN"
    return ""


# =========================
# UI builders: Services, Calendar, Time slots, Lists
# =========================
def money_rub(x: int) -> str:
    try:
        return f"{int(x)} ₽"
    except Exception:
        return f"{x} ₽"


def svc_page_keys(page: int) -> Tuple[List[Tuple[str, str, int]], int, int]:
    keys = list(SERVICES.keys())
    total = len(keys)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(pages - 1, page))
    start = page * PAGE_SIZE
    end = min(total, start + PAGE_SIZE)
    items = []
    for k in keys[start:end]:
        title, price = SERVICES[k]
        items.append((k, title, price))
    return items, page, pages


def kb_services(page: int = 0) -> InlineKeyboardMarkup:
    items, page, pages = svc_page_keys(page)
    rows: List[List[InlineKeyboardButton]] = []
    for k, title, price in items:
        rows.append([InlineKeyboardButton(f"{title} — {money_rub(price)}", callback_data=f"SVC:{k}")])

    nav = []
    if pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"SVC_PAGE:{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{pages}", callback_data="NOP"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"SVC_PAGE:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")])
    return InlineKeyboardMarkup(rows)


def month_title(y: int, m: int) -> str:
    # Russian month names
    months = [
        "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]
    return f"{months[m]} {y}"


def kb_calendar(y: int, m: int) -> InlineKeyboardMarkup:
    today = now_tz().date()
    first_weekday, days_in_month = cal.monthrange(y, m)  # Mon=0 .. Sun=6
    # We'll display Mon..Sun
    rows: List[List[InlineKeyboardButton]] = []

    # Header nav
    prev_y, prev_m = (y, m - 1) if m > 1 else (y - 1, 12)
    next_y, next_m = (y, m + 1) if m < 12 else (y + 1, 1)

    rows.append(
        [
            InlineKeyboardButton("⬅️", callback_data=f"CAL:{prev_y}:{prev_m}"),
            InlineKeyboardButton(month_title(y, m), callback_data="NOP"),
            InlineKeyboardButton("➡️", callback_data=f"CAL:{next_y}:{next_m}"),
        ]
    )

    # Weekdays row
    rows.append(
        [
            InlineKeyboardButton("Пн", callback_data="NOP"),
            InlineKeyboardButton("Вт", callback_data="NOP"),
            InlineKeyboardButton("Ср", callback_data="NOP"),
            InlineKeyboardButton("Чт", callback_data="NOP"),
            InlineKeyboardButton("Пт", callback_data="NOP"),
            InlineKeyboardButton("Сб", callback_data="NOP"),
            InlineKeyboardButton("Вс", callback_data="NOP"),
        ]
    )

    # Days grid
    day = 1
    # first_weekday is Mon=0..Sun=6 -> ok
    for week in range(6):
        row = []
        for wd in range(7):
            if week == 0 and wd < first_weekday:
                row.append(InlineKeyboardButton(" ", callback_data="NOP"))
                continue
            if day > days_in_month:
                row.append(InlineKeyboardButton(" ", callback_data="NOP"))
                continue

            d = date(y, m, day)
            day_str = f"{y:04d}-{m:02d}-{day:02d}"
            if d < today:
                row.append(InlineKeyboardButton("·", callback_data="NOP"))
            else:
                row.append(InlineKeyboardButton(str(day), callback_data=f"DAY:{day_str}"))
            day += 1
        rows.append(row)
        if day > days_in_month:
            break

    rows.append(
        [
            InlineKeyboardButton("📍 Сегодня", callback_data="CAL_TODAY"),
            InlineKeyboardButton("⬅️ Назад", callback_data="GO:SERVICES"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def iter_slots_for_day(day_str: str) -> List[str]:
    """Return list of HH:MM for free slots (respecting work time and current time)."""
    y, m, d = map(int, day_str.split("-"))
    day_date = date(y, m, d)
    tzinfo = tz()
    now = now_tz()

    start_dt = datetime.combine(day_date, WORK_START, tzinfo)
    end_dt = datetime.combine(day_date, WORK_END, tzinfo)

    # Slots are start times, last start < end_dt (end is exclusive)
    slots = []
    cur = start_dt
    while cur < end_dt:
        if day_date == now.date() and cur <= now:
            cur += timedelta(minutes=SLOT_MINUTES)
            continue
        hhmm = cur.strftime("%H:%M")
        if not db_is_slot_busy(day_str, hhmm):
            slots.append(hhmm)
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots


def kb_times(day_str: str) -> InlineKeyboardMarkup:
    slots = iter_slots_for_day(day_str)
    rows: List[List[InlineKeyboardButton]] = []
    if not slots:
        rows.append([InlineKeyboardButton("😕 Нет свободного времени", callback_data="NOP")])
    else:
        # 4 per row
        row: List[InlineKeyboardButton] = []
        for i, t in enumerate(slots, 1):
            row.append(InlineKeyboardButton(t, callback_data=f"TIME:{t}"))
            if i % 4 == 0:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

    rows.append(
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="GO:CAL"),
            InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить запись", callback_data="CONFIRM")],
            [InlineKeyboardButton("✏️ Комментарий", callback_data="GO:COMMENT"),
             InlineKeyboardButton("🚫 Без комментария", callback_data="NO_COMMENT")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="GO:TIME"),
             InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")],
        ]
    )


def fmt_location_block() -> str:
    parts = []
    if ADDRESS:
        parts.append(f"📍 <b>Адрес:</b> {html.escape(ADDRESS)}")
    if HOW_TO_FIND:
        parts.append(f"🧭 <b>Как найти:</b> {html.escape(HOW_TO_FIND)}")
    extra = []
    if DOORPHONE:
        extra.append(f"домофон {html.escape(DOORPHONE)}")
    if GATE:
        extra.append(f"калитка {html.escape(GATE)}")
    if FLOOR:
        extra.append(f"этаж {html.escape(FLOOR)}")
    if APARTMENT:
        extra.append(f"кв. {html.escape(APARTMENT)}")
    if extra:
        parts.append("🔐 <b>Ориентиры:</b> " + ", ".join(extra))
    if YANDEX_MAP_URL:
        parts.append(f"🗺 <b>Карта:</b> {html.escape(YANDEX_MAP_URL)}")
    return "\n".join(parts)


# =========================
# Screens (text builders)
# =========================
def welcome_text(user_row: Optional[sqlite3.Row]) -> str:
    lines = []
    lines.append(f"✨ <b>{html.escape(SALON_NAME)}</b>")
    lines.append("")
    lines.append("Вы можете быстро записаться на маникюр/педикюр, посмотреть услуги и управлять своими записями.")
    lines.append("")
    if user_row:
        lines.append(f"👋 Привет, <b>{html.escape(user_row['full_name'] or 'друг')}</b>!")
        lines.append("Нажмите <b>📅 Записаться</b> или <b>💅 Услуги</b> — и выберем удобное время.")
    else:
        lines.append("Для записи нужна быстрая регистрация (1 раз).")
        lines.append("Сейчас начнём 👇")
    return "\n".join(lines)


def reg_name_text() -> str:
    return (
        f"📝 <b>Регистрация</b>\n\n"
        f"Как к вам обращаться?\n"
        f"Напишите <b>имя</b> (например: <i>Анна</i>)."
    )


def reg_phone_text() -> str:
    return (
        f"📞 <b>Телефон</b>\n\n"
        f"Отправьте номер:\n"
        f"— нажмите кнопку <b>«📱 Отправить контакт»</b>\n"
        f"или\n"
        f"— введите номер вручную (например: <i>+7 999 123-45-67</i>)"
    )


def booking_summary_text(d: Draft) -> str:
    return (
        f"🧾 <b>Проверьте запись</b>\n\n"
        f"💅 <b>Услуга:</b> {html.escape(d.service_title)}\n"
        f"💰 <b>Цена:</b> {money_rub(d.price)}\n"
        f"📅 <b>Дата:</b> {html.escape(d.day)}\n"
        f"⏰ <b>Время:</b> {html.escape(d.time)}\n"
        f"💬 <b>Комментарий:</b> {html.escape(d.comment or '—')}\n\n"
        f"Если всё верно — нажмите <b>✅ Подтвердить</b>."
    )


def services_text(page: int) -> str:
    items, page, pages = svc_page_keys(page)
    return (
        f"💅 <b>Услуги</b>\n\n"
        f"Выберите услугу ниже.\n"
        f"Страница: <b>{page+1}/{pages}</b>"
    )


def calendar_text(d: Draft, y: int, m: int) -> str:
    return (
        f"📅 <b>Выбор даты</b>\n\n"
        f"Услуга: <b>{html.escape(d.service_title)}</b> ({money_rub(d.price)})\n"
        f"Выберите день в календаре."
    )


def time_text(d: Draft) -> str:
    return (
        f"⏰ <b>Выбор времени</b>\n\n"
        f"Услуга: <b>{html.escape(d.service_title)}</b>\n"
        f"Дата: <b>{html.escape(d.day)}</b>\n\n"
        f"Выберите свободное время."
    )


def ask_text_prompt(user_row: Optional[sqlite3.Row]) -> str:
    s = "❓ <b>Вопрос мастеру</b>\n\nНапишите ваш вопрос одним сообщением — я передам мастеру."
    if user_row:
        s += "\n\n(Мастер увидит ваше имя и телефон из профиля.)"
    return s


def my_bookings_text(rows: List[sqlite3.Row]) -> str:
    if not rows:
        return "📋 <b>Мои записи</b>\n\nПока записей нет. Нажмите <b>📅 Записаться</b>."
    lines = ["📋 <b>Мои записи</b>\n"]
    for r in rows[:25]:
        st = r["status"]
        emoji = "⏳" if st == "pending" else ("✅" if st == "confirmed" else "❌")
        lines.append(
            f"{emoji} <b>#{r['id']}</b> — {html.escape(r['service_title'])}\n"
            f"📅 {html.escape(r['book_date'])} ⏰ {html.escape(r['book_time'])}\n"
            f"💬 {html.escape(r['comment'] or '—')}\n"
        )
    return "\n".join(lines).strip()


def cancel_menu_text(rows: List[sqlite3.Row]) -> str:
    if not rows:
        return "❌ <b>Отменить запись</b>\n\nУ вас нет активных записей."
    return "❌ <b>Отменить запись</b>\n\nВыберите запись, которую нужно отменить:"


def about_text() -> str:
    lines = []
    lines.append("👩‍🎨 <b>Обо мне</b>\n")
    lines.append(f"💅 <b>{html.escape(MASTER_NAME)}</b>")
    if MASTER_EXPERIENCE:
        lines.append(f"🏅 <b>Опыт:</b> {html.escape(MASTER_EXPERIENCE)}")
    if MASTER_TEXT:
        lines.append("")
        lines.append(html.escape(MASTER_TEXT))
    loc = fmt_location_block()
    if loc:
        lines.append("\n" + loc)
    return "\n".join(lines).strip()


def admin_panel_text(rows: List[sqlite3.Row]) -> str:
    lines = [f"🛠 <b>Админ-панель</b>\n\nБлижайшие записи (до 25):"]
    if not rows:
        lines.append("\nПока нет активных записей.")
        return "\n".join(lines)

    for r in rows:
        st = r["status"]
        emoji = "⏳" if st == "pending" else "✅"
        uname = (r["username"] or "").strip()
        name = (r["full_name"] or "").strip()
        phone = (r["phone"] or "").strip()
        client = name or ("@" + uname if uname else f"id:{r['user_id']}")
        lines.append(
            f"\n{emoji} <b>#{r['id']}</b> — {html.escape(r['service_title'])} ({money_rub(r['price'])})"
            f"\n📅 {html.escape(r['book_date'])} ⏰ {html.escape(r['book_time'])}"
            f"\n👤 {html.escape(client)} | ☎️ {html.escape(phone or '—')}"
        )
    return "\n".join(lines).strip()


def kb_about() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row1: List[InlineKeyboardButton] = []
    if ADMIN_CONTACT:
        row1.append(InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT))
    if YANDEX_MAP_URL:
        row1.append(InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL))
    if row1:
        rows.append(row1)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="GO:MENU")])
    return InlineKeyboardMarkup(rows)


def kb_cancel_list(rows: List[sqlite3.Row]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for r in rows[:25]:
        st = r["status"]
        emoji = "⏳" if st == "pending" else "✅"
        buttons.append(
            [InlineKeyboardButton(
                f"{emoji} #{r['id']} — {r['book_date']} {r['book_time']}",
                callback_data=f"CANCEL:{r['id']}",
            )]
        )
    buttons.append([InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")])
    return InlineKeyboardMarkup(buttons)


def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить", callback_data="GO:ADMIN")],
            [InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")],
        ]
    )


# =========================
# Admin notify
# =========================
def user_deeplink(username: str, tg_id: int) -> str:
    u = (username or "").strip()
    if u:
        return f"https://t.me/{u.lstrip('@')}"
    return f"tg://user?id={tg_id}"


async def notify_admin_new_booking(context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = db_get_booking(bid)
    if not b:
        return
    u = db_get_user(int(b["user_id"]))
    username = (u["username"] if u else "") if u else ""
    full_name = (u["full_name"] if u else "") if u else ""
    phone = (u["phone"] if u else "") if u else ""

    client_link = user_deeplink(username or "", int(b["user_id"]))
    client_name = full_name or (f"@{username}" if username else f"id:{b['user_id']}")

    text = (
        f"🆕 <b>Новая запись</b>\n\n"
        f"👤 <b>Клиент:</b> {html.escape(client_name)}\n"
        f"☎️ <b>Телефон:</b> {html.escape(phone or '—')}\n"
        f"💅 <b>Услуга:</b> {html.escape(b['service_title'])} ({money_rub(b['price'])})\n"
        f"📅 <b>Дата:</b> {html.escape(b['book_date'])}\n"
        f"⏰ <b>Время:</b> {html.escape(b['book_time'])}\n"
        f"💬 <b>Комментарий:</b> {html.escape(b['comment'] or '—')}\n"
        f"🆔 <b>ID:</b> #{b['id']}"
    )

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_OK:{bid}"),
                InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_NO:{bid}"),
            ],
            [
                InlineKeyboardButton("👤 Профиль клиента", url=client_link),
                InlineKeyboardButton("✉️ Написать клиенту", url=f"tg://user?id={int(b['user_id'])}"),
            ],
        ]
    )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


async def notify_admin_question(context: ContextTypes.DEFAULT_TYPE, user_id: int, question: str) -> None:
    u = db_get_user(user_id)
    username = (u["username"] if u else "") if u else ""
    full_name = (u["full_name"] if u else "") if u else ""
    phone = (u["phone"] if u else "") if u else ""

    link = user_deeplink(username or "", user_id)
    client_name = full_name or (f"@{username}" if username else f"id:{user_id}")

    text = (
        f"📩 <b>Вопрос мастеру</b>\n\n"
        f"👤 <b>Клиент:</b> {html.escape(client_name)}\n"
        f"☎️ <b>Телефон:</b> {html.escape(phone or '—')}\n"
        f"🔗 <b>Профиль:</b> {html.escape(link)}\n\n"
        f"❓ <b>Вопрос:</b>\n{html.escape(question)}"
    )

    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("👤 Профиль клиента", url=link),
                InlineKeyboardButton("✉️ Написать клиенту", url=f"tg://user?id={user_id}"),
            ]
        ]
    )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


async def notify_admin_canceled(context: ContextTypes.DEFAULT_TYPE, bid: int) -> None:
    b = db_get_booking(bid)
    if not b:
        return
    u = db_get_user(int(b["user_id"]))
    username = (u["username"] if u else "") if u else ""
    full_name = (u["full_name"] if u else "") if u else ""
    phone = (u["phone"] if u else "") if u else ""

    client_name = full_name or (f"@{username}" if username else f"id:{b['user_id']}")
    text = (
        f"⚠️ <b>Запись отменена клиентом</b>\n\n"
        f"👤 {html.escape(client_name)} | ☎️ {html.escape(phone or '—')}\n"
        f"💅 {html.escape(b['service_title'])}\n"
        f"📅 {html.escape(b['book_date'])} ⏰ {html.escape(b['book_time'])}\n"
        f"🆔 #{b['id']}"
    )
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode=ParseMode.HTML)
    except Exception:
        pass


# =========================
# Core flows
# =========================
async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tg_id = update.effective_user.id
    user_row = db_get_user(tg_id)

    if not user_row:
        # begin registration
        clear_draft(context)
        set_stage(context, "reg_name")
        await render_screen(update, context, welcome_text(None) + "\n\n" + reg_name_text(), None)
        return

    clear_stage(context)
    await render_screen(update, context, welcome_text(user_row), None)


async def show_services(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0) -> None:
    await render_screen(update, context, services_text(page), kb_services(page))


async def start_booking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    tg_id = update.effective_user.id
    user_row = db_get_user(tg_id)
    if not user_row:
        set_stage(context, "reg_name")
        await render_screen(update, context, welcome_text(None) + "\n\n" + reg_name_text(), None)
        return

    clear_draft(context)
    # choose service
    await show_services(update, context, page=0)


async def show_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE, y: Optional[int] = None, m: Optional[int] = None) -> None:
    d = get_draft(context)
    if not d.service_key:
        await show_services(update, context, page=0)
        return

    dt = now_tz()
    y = y or dt.year
    m = m or dt.month
    await render_screen(update, context, calendar_text(d, y, m), kb_calendar(y, m))


async def show_times(update: Update, context: ContextTypes.DEFAULT_TYPE, day_str: str) -> None:
    d = get_draft(context)
    if not d.service_key:
        await show_services(update, context, page=0)
        return
    d.day = day_str
    await render_screen(update, context, time_text(d), kb_times(day_str))


async def show_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    d = get_draft(context)
    # basic validation
    if not (d.service_key and d.day and d.time):
        await render_screen(update, context, "⚠️ Не хватает данных для записи. Начните заново: 📅 Записаться", None)
        return
    await render_screen(update, context, booking_summary_text(d), kb_confirm())


async def show_my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = db_user_bookings(update.effective_user.id, limit=25)
    await render_screen(update, context, my_bookings_text(rows), InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]))


async def show_cancel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = db_user_active_bookings(update.effective_user.id, limit=25)
    await render_screen(update, context, cancel_menu_text(rows), kb_cancel_list(rows) if rows else InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]))


async def show_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await render_about_with_photo(update, context, about_text(), kb_about())


async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ADMIN_ID:
        await render_screen(update, context, "⛔️ Доступно только администратору.", InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]))
        return
    rows = db_admin_upcoming(limit=25)
    await render_screen(update, context, admin_panel_text(rows), kb_admin_panel())


# =========================
# /start and /menu
# =========================
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Always show reply menu
    try:
        await update.effective_chat.send_action("typing")
    except Exception:
        pass

    # Ensure reply menu is set by sending a lightweight message if no screen yet
    # We'll keep one-screen for content; reply keyboard attaches to user input anyway,
    # but we attach it on any user text by replying sometimes? Here we just send menu on start.
    try:
        await update.message.reply_text("✅ Меню включено.", reply_markup=REPLY_MENU)
    except Exception:
        pass

    await show_menu(update, context)


async def on_menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await update.message.reply_text("🏠", reply_markup=REPLY_MENU)
    except Exception:
        pass
    await show_menu(update, context)


# =========================
# CONTACT handler
# =========================
async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.contact:
        return

    # Menu buttons must always work first, but contact handler is separate.
    # Here, only accept contact during reg_phone stage.
    stage = get_stage(context)
    if stage != "reg_phone":
        # ignore contact outside registration to avoid unwanted overwrite
        if AUTO_DELETE_USER_INPUT:
            await safe_delete(context.bot, update.effective_chat.id, msg.message_id)
        return

    phone = msg.contact.phone_number or ""
    phone = phone.strip()

    # Save
    tg_id = update.effective_user.id
    username = (update.effective_user.username or "").strip()
    full_name = context.user_data.get("reg_full_name", "").strip()
    if not full_name:
        full_name = update.effective_user.full_name or "Клиент"

    db_upsert_user(tg_id, username, full_name, phone)
    context.user_data.pop("reg_full_name", None)
    clear_stage(context)

    # React first (screen), then delete user input
    await render_screen(update, context, f"✅ Спасибо! Регистрация завершена.\n\nНажмите <b>📅 Записаться</b> или <b>💅 Услуги</b>.", None)

    if AUTO_DELETE_USER_INPUT:
        await safe_delete(context.bot, update.effective_chat.id, msg.message_id)


# =========================
# TEXT handler (hard-first buttons)
# =========================
PHONE_RE = re.compile(r"^\s*(\+?\d[\d\-\s\(\)]{6,})\s*$")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    action = normalize_button(text)

    # 1) HARD-FIRST: menu buttons always handled first
    if action:
        # show reaction FIRST
        if action == "MENU":
            await render_screen(update, context, "🏠 Открываю меню…", None)
            await show_menu(update, context)

        elif action == "BOOK":
            await render_screen(update, context, "📅 Начинаем запись…", None)
            await start_booking(update, context)

        elif action == "SERVICES":
            await render_screen(update, context, "💅 Открываю услуги…", None)
            await show_services(update, context, page=0)

        elif action == "ABOUT":
            await render_screen(update, context, "👩‍🎨 Открываю информацию…", None)
            await show_about(update, context)

        elif action == "MY":
            await render_screen(update, context, "📋 Загружаю ваши записи…", None)
            await show_my_bookings(update, context)

        elif action == "CANCEL_MENU":
            await render_screen(update, context, "❌ Открываю отмену записей…", None)
            await show_cancel_menu(update, context)

        elif action == "ASK":
            await render_screen(update, context, "❓ Напишите ваш вопрос…", None)
            set_stage(context, "ask_text")
            user_row = db_get_user(update.effective_user.id)
            await render_screen(update, context, ask_text_prompt(user_row), InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]))

        elif action == "ADMIN":
            await render_screen(update, context, "🛠 Открываю админ-панель…", None)
            await show_admin_panel(update, context)

        # delete user message AFTER reaction
        if AUTO_DELETE_USER_INPUT:
            await safe_delete(context.bot, update.effective_chat.id, msg.message_id)
        return

    # 2) If no hard button: handle stages (registration / comment / ask)
    stage = get_stage(context)

    if stage == "reg_name":
        name = text.strip()
        if len(name) < 2:
            await render_screen(update, context, "⚠️ Имя слишком короткое. Напишите имя ещё раз.", None)
        else:
            context.user_data["reg_full_name"] = name
            set_stage(context, "reg_phone")
            # prompt with contact button
            kb = ReplyKeyboardMarkup(
                [[KeyboardButton("📱 Отправить контакт", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True,
            )
            # show reaction on screen first, then send helper message with contact keyboard
            await render_screen(update, context, "✅ Отлично. Теперь нужен телефон.", None)
            try:
                await msg.reply_text(reg_phone_text(), reply_markup=kb, parse_mode=ParseMode.HTML)
            except Exception:
                await render_screen(update, context, reg_phone_text(), None)

    elif stage == "reg_phone":
        m = PHONE_RE.match(text)
        if not m:
            await render_screen(update, context, "⚠️ Не похоже на номер телефона. Попробуйте ещё раз или нажмите «📱 Отправить контакт».", None)
        else:
            phone = m.group(1).strip()
            tg_id = update.effective_user.id
            username = (update.effective_user.username or "").strip()
            full_name = context.user_data.get("reg_full_name", "").strip() or (update.effective_user.full_name or "Клиент")
            db_upsert_user(tg_id, username, full_name, phone)
            context.user_data.pop("reg_full_name", None)
            clear_stage(context)
            await render_screen(update, context, f"✅ Спасибо! Регистрация завершена.\n\nТеперь нажмите <b>📅 Записаться</b>.", None)

            # restore main menu reply keyboard
            try:
                await msg.reply_text("🏠 Меню", reply_markup=REPLY_MENU)
            except Exception:
                pass

    elif stage == "comment":
        d = get_draft(context)
        d.comment = text.strip()
        clear_stage(context)
        await render_screen(update, context, "✅ Комментарий сохранён.", None)
        await show_confirm(update, context)

    elif stage == "ask_text":
        question = text.strip()
        if len(question) < 3:
            await render_screen(update, context, "⚠️ Вопрос слишком короткий. Напишите подробнее.", None)
        else:
            clear_stage(context)
            await render_screen(update, context, "✅ Отправил мастеру. Скоро вам ответят.", None)
            await notify_admin_question(context, update.effective_user.id, question)

    else:
        # default: show hint, but don't break
        user_row = db_get_user(update.effective_user.id)
        if not user_row:
            set_stage(context, "reg_name")
            await render_screen(update, context, reg_name_text(), None)
        else:
            await render_screen(update, context, "Подсказка: нажмите <b>📅 Записаться</b> или <b>💅 Услуги</b>.", None)

    # delete user message AFTER reaction
    if AUTO_DELETE_USER_INPUT:
        await safe_delete(context.bot, update.effective_chat.id, msg.message_id)


# =========================
# Callback handler
# =========================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.data:
        return

    data = q.data
    await q.answer()  # quick ack

    # NOP
    if data == "NOP":
        return

    # GO routes
    if data.startswith("GO:"):
        dest = data.split(":", 1)[1]
        if dest == "MENU":
            await render_screen(update, context, "🏠 Открываю меню…", None)
            await show_menu(update, context)
            return
        if dest == "SERVICES":
            await render_screen(update, context, "💅 Открываю услуги…", None)
            await show_services(update, context, page=0)
            return
        if dest == "CAL":
            await render_screen(update, context, "📅 Открываю календарь…", None)
            await show_calendar(update, context)
            return
        if dest == "TIME":
            d = get_draft(context)
            if d.day:
                await render_screen(update, context, "⏰ Выбор времени…", None)
                await show_times(update, context, d.day)
            else:
                await show_calendar(update, context)
            return
        if dest == "COMMENT":
            set_stage(context, "comment")
            await render_screen(update, context, "✏️ Напишите комментарий к записи одним сообщением.", None)
            return
        if dest == "ADMIN":
            await show_admin_panel(update, context)
            return

    # Services pagination
    if data.startswith("SVC_PAGE:"):
        try:
            page = int(data.split(":")[1])
        except Exception:
            page = 0
        await show_services(update, context, page=page)
        return

    # Select service
    if data.startswith("SVC:"):
        key = data.split(":", 1)[1]
        if key not in SERVICES:
            await render_screen(update, context, "⚠️ Услуга не найдена.", None)
            return
        title, price = SERVICES[key]
        d = get_draft(context)
        d.service_key = key
        d.service_title = title
        d.price = int(price)
        d.day = ""
        d.time = ""
        d.comment = ""

        await render_screen(update, context, f"✅ Вы выбрали: <b>{html.escape(title)}</b> ({money_rub(price)})\n\nТеперь выберите дату.", None)
        await show_calendar(update, context)
        return

    # Calendar navigation
    if data == "CAL_TODAY":
        dt = now_tz()
        await show_calendar(update, context, y=dt.year, m=dt.month)
        return

    if data.startswith("CAL:"):
        try:
            _, ys, ms = data.split(":")
            y = int(ys)
            m = int(ms)
        except Exception:
            dt = now_tz()
            y, m = dt.year, dt.month
        await show_calendar(update, context, y=y, m=m)
        return

    # Pick day
    if data.startswith("DAY:"):
        day_str = data.split(":", 1)[1]
        # Validate not past
        try:
            y, m, d = map(int, day_str.split("-"))
            chosen = date(y, m, d)
        except Exception:
            await render_screen(update, context, "⚠️ Некорректная дата.", None)
            return
        if chosen < now_tz().date():
            await render_screen(update, context, "⚠️ Нельзя выбрать прошедшую дату.", None)
            return
        await render_screen(update, context, f"✅ Дата: <b>{html.escape(day_str)}</b>\n\nТеперь выберите время.", None)
        await show_times(update, context, day_str)
        return

    # Pick time
    if data.startswith("TIME:"):
        t = data.split(":", 1)[1]
        # Validate busy again
        d = get_draft(context)
        if not d.day:
            await show_calendar(update, context)
            return
        if db_is_slot_busy(d.day, t):
            await render_screen(update, context, "⛔️ Этот слот уже занят. Выберите другое время.", None)
            await show_times(update, context, d.day)
            return
        d.time = t
        if not d.comment:
            d.comment = ""
        await render_screen(update, context, f"✅ Время: <b>{html.escape(t)}</b>\n\nДобавим комментарий?", None)
        await show_confirm(update, context)
        return

    # No comment
    if data == "NO_COMMENT":
        d = get_draft(context)
        d.comment = d.comment or "Без комментария"
        clear_stage(context)
        await render_screen(update, context, "✅ Ок, без комментария.", None)
        await show_confirm(update, context)
        return

    # Confirm booking
    if data == "CONFIRM":
        tg_id = update.effective_user.id
        user_row = db_get_user(tg_id)
        if not user_row:
            set_stage(context, "reg_name")
            await render_screen(update, context, "⚠️ Нужно завершить регистрацию.\n\n" + reg_name_text(), None)
            return

        d = get_draft(context)
        if not (d.service_key and d.day and d.time):
            await render_screen(update, context, "⚠️ Не хватает данных. Начните заново: 📅 Записаться", None)
            return

        # Re-check availability
        if db_is_slot_busy(d.day, d.time):
            await render_screen(update, context, "⛔️ Этот слот уже занят. Выберите другое время.", None)
            await show_times(update, context, d.day)
            return

        comment = d.comment or "Без комментария"
        bid = db_create_booking(
            user_id=tg_id,
            service_key=d.service_key,
            service_title=d.service_title,
            price=d.price,
            book_date=d.day,
            book_time=d.time,
            comment=comment,
        )

        clear_draft(context)
        clear_stage(context)

        # User message + admin notify
        await render_screen(
            update,
            context,
            "🎉 <b>Заявка отправлена!</b>\n\n"
            "⏳ Пожалуйста, ожидайте подтверждения мастера.\n\n"
            "Вы можете посмотреть статус в разделе <b>📋 Мои записи</b>.",
            InlineKeyboardMarkup([[InlineKeyboardButton("📋 Мои записи", callback_data="GO:MY")],
                                  [InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]),
        )
        await notify_admin_new_booking(context, bid)
        return

    # GO:MY (from confirm screen)
    if data == "GO:MY":
        await show_my_bookings(update, context)
        return

    # Cancel booking by user
    if data.startswith("CANCEL:"):
        try:
            bid = int(data.split(":")[1])
        except Exception:
            await render_screen(update, context, "⚠️ Некорректный ID записи.", None)
            return

        b = db_get_booking(bid)
        if not b or int(b["user_id"]) != update.effective_user.id:
            await render_screen(update, context, "⚠️ Запись не найдена.", None)
            return
        if b["status"] not in ("pending", "confirmed"):
            await render_screen(update, context, "ℹ️ Эта запись уже не активна.", None)
            await show_cancel_menu(update, context)
            return

        db_set_booking_status(bid, "canceled")
        await render_screen(update, context, f"✅ Запись <b>#{bid}</b> отменена.", None)
        await notify_admin_canceled(context, bid)
        await show_cancel_menu(update, context)
        return

    # Admin actions
    if data.startswith("ADM_OK:") or data.startswith("ADM_NO:"):
        if update.effective_user.id != ADMIN_ID:
            await render_screen(update, context, "⛔️ Доступно только администратору.", None)
            return

        ok = data.startswith("ADM_OK:")
        try:
            bid = int(data.split(":")[1])
        except Exception:
            return

        b = db_get_booking(bid)
        if not b:
            await render_screen(update, context, "⚠️ Запись не найдена.", None)
            return

        user_id = int(b["user_id"])
        if ok:
            db_set_booking_status(bid, "confirmed")
            # notify user
            msg_user = (
                f"✅ <b>Запись подтверждена!</b>\n\n"
                f"💅 {html.escape(b['service_title'])} ({money_rub(b['price'])})\n"
                f"📅 {html.escape(b['book_date'])} ⏰ {html.escape(b['book_time'])}\n\n"
            )
            loc = fmt_location_block()
            if loc:
                msg_user += loc + "\n\n"
            msg_user += "До встречи! 💛"
            try:
                await context.bot.send_message(chat_id=user_id, text=msg_user, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception:
                pass

            await render_screen(update, context, f"✅ Запись <b>#{bid}</b> подтверждена.", None)
        else:
            db_set_booking_status(bid, "canceled")
            # notify user
            msg_user = (
                f"❌ <b>Запись отменена мастером</b>\n\n"
                f"💅 {html.escape(b['service_title'])}\n"
                f"📅 {html.escape(b['book_date'])} ⏰ {html.escape(b['book_time'])}\n\n"
                f"Если хотите — попробуйте выбрать другое время: <b>📅 Записаться</b>."
            )
            try:
                await context.bot.send_message(chat_id=user_id, text=msg_user, parse_mode=ParseMode.HTML)
            except Exception:
                pass

            await render_screen(update, context, f"❌ Запись <b>#{bid}</b> отменена.", None)

        return

    # Fallback
    await render_screen(update, context, "ℹ️ Действие не распознано. Нажмите 🏠 Меню.", InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="GO:MENU")]]))


# =========================
# Main
# =========================
def main() -> None:
    db_init()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CommandHandler("menu", on_menu_cmd))

    app.add_handler(CallbackQueryHandler(on_callback))

    # Contact first (separate type)
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))

    # Text last
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    log.info("Bot started. TZ=%s DB=%s", TZ_NAME, DB_PATH)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
