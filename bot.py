# bot.py — ПОЛНЫЙ ОДИН ФАЙЛ
# python-telegram-bot >= 21
# Railway: настройки через Variables (ENV). .env не нужен.

import os
import re
import sqlite3
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("beauty-bot")


# =========================================================
# STORAGE (SQLite)
# =========================================================
@dataclass
class User:
    tg_id: int
    username: str
    full_name: str
    phone: str
    created_at: str


@dataclass
class Booking:
    id: int
    user_id: int
    service_key: str
    service_title: str
    price: int
    book_date: str
    book_time: str
    comment: str
    status: str
    created_at: str
    reminder_sent: int


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
            c.execute("""
            CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT '',
                full_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS bookings(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                service_key TEXT NOT NULL,
                service_title TEXT NOT NULL,
                price INTEGER NOT NULL,
                book_date TEXT NOT NULL,
                book_time TEXT NOT NULL,
                comment TEXT DEFAULT '',
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS blocked_slots(
                book_date TEXT NOT NULL,
                book_time TEXT NOT NULL,
                PRIMARY KEY(book_date, book_time)
            );
            """)
            cols = [r["name"] for r in c.execute("PRAGMA table_info(bookings)").fetchall()]
            if "reminder_sent" not in cols:
                c.execute("ALTER TABLE bookings ADD COLUMN reminder_sent INTEGER NOT NULL DEFAULT 0;")

    # users
    def upsert_user(self, tg_id: int, username: str, full_name: str, phone: str) -> None:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as c:
            c.execute("""
            INSERT INTO users(tg_id, username, full_name, phone, created_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                phone=excluded.phone
            """, (tg_id, username or "", full_name.strip(), phone.strip(), now))

    def get_user(self, tg_id: int) -> Optional[User]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()
            if not row:
                return None
            return User(
                tg_id=row["tg_id"],
                username=row["username"] or "",
                full_name=row["full_name"],
                phone=row["phone"],
                created_at=row["created_at"]
            )

    def delete_user(self, tg_id: int) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
            c.execute("DELETE FROM bookings WHERE user_id=?", (tg_id,))

    # slots
    def block_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR IGNORE INTO blocked_slots(book_date, book_time) VALUES(?,?)", (book_date, book_time))

    def unblock_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM blocked_slots WHERE book_date=? AND book_time=?", (book_date, book_time))

    def list_blocked_for_day(self, book_date: str) -> List[str]:
        with self._conn() as c:
            rows = c.execute("""
                SELECT book_time FROM blocked_slots
                WHERE book_date=?
                ORDER BY book_time
            """, (book_date,)).fetchall()
            return [r["book_time"] for r in rows]

    def is_slot_blocked(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?",
                            (book_date, book_time)).fetchone()
            return row is not None

    def is_slot_taken(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
            """, (book_date, book_time)).fetchone()
            return row is not None

    # bookings
    def create_booking(self, user_id: int, service_key: str, service_title: str, price: int,
                       book_date: str, book_time: str, comment: str) -> int:
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as c:
            cur = c.execute("""
            INSERT INTO bookings(user_id, service_key, service_title, price, book_date, book_time, comment, status, created_at, reminder_sent)
            VALUES(?,?,?,?,?,?,?,?,?,0)
            """, (user_id, service_key, service_title, int(price), book_date, book_time, comment or "", "pending", now))
            return int(cur.lastrowid)

    def get_booking(self, booking_id: int) -> Optional[Booking]:
        with self._conn() as c:
            row = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
            if not row:
                return None
            return self._row_to_booking(row)

    def set_booking_status(self, booking_id: int, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))

    def mark_reminder_sent(self, booking_id: int) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET reminder_sent=1 WHERE id=?", (booking_id,))

    def list_user_upcoming(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE user_id=? AND status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            """, (user_id,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_day(self, day: str) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE book_date=? AND status IN ('pending','confirmed')
            ORDER BY book_time
            """, (day,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_range(self, day_from: str, day_to: str) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE book_date>=? AND book_date<=? AND status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            """, (day_from, day_to)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            LIMIT ?
            """, (int(limit),)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_for_reminders(self) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed') AND reminder_sent=0
            ORDER BY book_date, book_time
            """).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def _row_to_booking(self, row: sqlite3.Row) -> Booking:
        return Booking(
            id=row["id"],
            user_id=row["user_id"],
            service_key=row["service_key"],
            service_title=row["service_title"],
            price=row["price"],
            book_date=row["book_date"],
            book_time=row["book_time"],
            comment=row["comment"] or "",
            status=row["status"],
            created_at=row["created_at"],
            reminder_sent=int(row["reminder_sent"]) if "reminder_sent" in row.keys() else 0,
        )


# =========================================================
# ENV (Railway Variables)
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
SALON_NAME = os.getenv("SALON_NAME", "Beauty Lounge").strip()
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

ADDRESS = os.getenv("ADDRESS", "Адрес: (впишите адрес)").strip()
HOW_TO_FIND = os.getenv("HOW_TO_FIND", "Как нас найти: (ориентиры)").strip()
YANDEX_MAP_URL = os.getenv("YANDEX_MAP_URL", "").strip()
DOORPHONE = os.getenv("DOORPHONE", "Домофон: 69").strip()
GATE = os.getenv("GATE", "Калитка: —").strip()
FLOOR = os.getenv("FLOOR", "Этаж: 10").strip()
APARTMENT = os.getenv("APARTMENT", "Квартира: 69").strip()

MASTER_NAME = os.getenv("MASTER_NAME", "Ваш мастер").strip()
MASTER_EXPERIENCE = os.getenv("MASTER_EXPERIENCE", "Опыт: 5 лет").strip()
MASTER_TEXT = os.getenv(
    "MASTER_TEXT",
    "Аккуратно, стерильно и с любовью к деталям ✨\n"
    "Подберу форму и покрытие под ваш стиль, чтобы носилось красиво и долго.\n\n"
    "Почему выбирают меня:\n"
    "• стерильность и безопасность\n"
    "• ровное покрытие и аккуратная кутикула\n"
    "• комфортная атмосфера\n"
).strip()
MASTER_PHOTO = os.getenv("MASTER_PHOTO", "").strip()  # file_id или URL (опционально)

WORK_START = os.getenv("WORK_START", "08:00").strip()
WORK_END = os.getenv("WORK_END", "23:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

ALLOWED_MANUAL_MINUTES = {0, 15, 30, 45}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")

tz = ZoneInfo(TZ_NAME)
store = Storage(DB_PATH)


# =========================================================
# SERVICES
# =========================================================
SERVICES = {
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

# stages
STAGE_NONE = "none"
STAGE_REG_NAME = "reg_name"
STAGE_REG_PHONE = "reg_phone"
STAGE_BOOK_COMMENT = "book_comment"
STAGE_MANUAL_TIME = "manual_time"
STAGE_ASK_MASTER = "ask_master"


# =========================================================
# HELPERS
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def now_local() -> datetime:
    return datetime.now(tz)


def fmt_date_ru(iso_date: str) -> str:
    return date.fromisoformat(iso_date).strftime("%d.%m.%Y")


def fmt_dt_ru(iso_date: str, hhmm: str) -> str:
    return f"{fmt_date_ru(iso_date)} {hhmm}"


def parse_phone(text: str) -> str:
    t = re.sub(r"[^\d+]", "", text or "")
    if t.startswith("8") and len(t) >= 11:
        t = "+7" + t[1:]
    if t.startswith("7") and len(t) == 11:
        t = "+" + t
    return t


def user_link(user_id: int, username: str) -> str:
    return f"https://t.me/{username}" if username else f"tg://user?id={user_id}"


def parse_hhmm(text: str) -> Optional[str]:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", text or "")
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"


def work_bounds_for_day(day_iso: str) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(day_iso)
    ws = datetime.strptime(WORK_START, "%H:%M").time()
    we = datetime.strptime(WORK_END, "%H:%M").time()
    start_dt = datetime.combine(d, ws).replace(tzinfo=tz)
    end_dt = datetime.combine(d, we).replace(tzinfo=tz)
    return start_dt, end_dt


def is_time_allowed_for_booking(day_iso: str, hhmm: str) -> Tuple[bool, str]:
    parsed = parse_hhmm(hhmm)
    if not parsed:
        return False, "Введите время в формате HH:MM (например 17:15)."

    hh, mm = map(int, parsed.split(":"))
    if mm not in ALLOWED_MANUAL_MINUTES:
        return False, "Минуты должны быть 00 / 15 / 30 / 45 (например 17:15)."

    start_dt, end_dt = work_bounds_for_day(day_iso)
    slot_dt = datetime.combine(date.fromisoformat(day_iso), dtime(hh, mm)).replace(tzinfo=tz)

    if slot_dt < start_dt or slot_dt > end_dt:
        return False, f"Время должно быть в пределах {WORK_START}–{WORK_END}."

    if day_iso == now_local().date().isoformat() and slot_dt <= now_local():
        return False, "Это время уже прошло. Выберите время позже текущего."

    if store.is_slot_blocked(day_iso, parsed):
        return False, "Это время заблокировано. Выберите другое."
    if store.is_slot_taken(day_iso, parsed):
        return False, "Это время уже занято. Выберите другое."

    return True, parsed


def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩‍🎨 Обо мне", "📍 Контакты"],
        ["👤 Профиль", "📩 Вопрос мастеру"],
        ["❌ Отменить запись"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)


def after_start_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💅 Записаться", callback_data="go_book")],
        [InlineKeyboardButton("👤 Профиль", callback_data="go_profile")],
    ])


def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )


def about_master_text() -> str:
    return (
        f"👩‍🎨 *{MASTER_NAME}*\n"
        f"*{MASTER_EXPERIENCE}*\n\n"
        f"{MASTER_TEXT}\n\n"
        "Запись: нажмите *💅 Записаться* ✅"
    )


def prices_text() -> str:
    lines = [
        "💳 *Цены*",
        "",
        "✨ Маникюр",
        f"• {SERVICES['mn_no'][0]} — *{SERVICES['mn_no'][1]} ₽*",
        f"• {SERVICES['mn_cov'][0]} — *{SERVICES['mn_cov'][1]} ₽*",
        f"• {SERVICES['mn_cov_design'][0]} — *{SERVICES['mn_cov_design'][1]} ₽*",
        "",
        "🦶 Педикюр",
        f"• {SERVICES['pd_no'][0]} — *{SERVICES['pd_no'][1]} ₽*",
        f"• {SERVICES['pd_cov'][0]} — *{SERVICES['pd_cov'][1]} ₽*",
        f"• {SERVICES['pd_toes'][0]} — *{SERVICES['pd_toes'][1]} ₽*",
        f"• {SERVICES['pd_heels'][0]} — *{SERVICES['pd_heels'][1]} ₽*",
        "",
        "🌟 Дополнительно",
        f"• {SERVICES['ext'][0]} — *{SERVICES['ext'][1]} ₽*",
        f"• {SERVICES['corr'][0]} — *{SERVICES['corr'][1]} ₽*",
        f"• {SERVICES['design'][0]} — *{SERVICES['design'][1]} ₽*",
    ]
    return "\n".join(lines)


def contacts_text() -> str:
    t = "📍 *Как нас найти*\n\n"
    t += f"• {ADDRESS}\n"
    t += f"• {HOW_TO_FIND}\n"
    t += f"• {DOORPHONE}\n"
    t += f"• {GATE}\n"
    t += f"• {FLOOR}\n"
    t += f"• {APARTMENT}\n"
    t += f"\n🕘 Время работы: *{WORK_START}–{WORK_END}*\n"
    return t


def contacts_inline() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if YANDEX_MAP_URL:
        rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None


# =========================================================
# CLEAN CHAT
# =========================================================
async def safe_delete(bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


def track_bot_msg(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    ids = context.chat_data.get("bot_msg_ids")
    if not isinstance(ids, list):
        ids = []
    ids.append(message_id)
    context.chat_data["bot_msg_ids"] = ids[-25:]


async def clear_bot_msgs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ids = context.chat_data.get("bot_msg_ids", [])
    if isinstance(ids, list):
        for mid in ids:
            await safe_delete(context.bot, chat_id, mid)
    context.chat_data["bot_msg_ids"] = []


async def delete_user_message_if_possible(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)


async def send_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *, parse_mode=None, reply_markup=None, clean_before=True):
    if clean_before:
        await clear_bot_msgs(update, context)
    msg = await context.bot.send_message(update.effective_chat.id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
    track_bot_msg(context, msg.message_id)
    return msg


async def send_photo_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, photo: str, caption: str, *, parse_mode=None, reply_markup=None, clean_before=True):
    if clean_before:
        await clear_bot_msgs(update, context)
    msg = await context.bot.send_photo(update.effective_chat.id, photo=photo, caption=caption, parse_mode=parse_mode, reply_markup=reply_markup)
    track_bot_msg(context, msg.message_id)
    return msg


# =========================================================
# FLOW-ID защита от старых кнопок
# =========================================================
def new_flow_id() -> str:
    return secrets.token_urlsafe(6)


def set_flow(context: ContextTypes.DEFAULT_TYPE) -> str:
    fid = new_flow_id()
    context.user_data["flow_id"] = fid
    return fid


def get_flow(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("flow_id", ""))


def cb(fid: str, action: str, payload: str = "") -> str:
    # формат: action|fid|payload
    return f"{action}|{fid}|{payload}"


def parse_cb(data: str) -> Tuple[str, str, str]:
    # action|fid|payload
    parts = (data or "").split("|", 2)
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return parts[0], parts[1], parts[2]


def is_fid_ok(context: ContextTypes.DEFAULT_TYPE, fid: str) -> bool:
    return fid and fid == get_flow(context)


# =========================================================
# UI builders
# =========================================================
def month_days_from_today(days_ahead: int = 31) -> list[str]:
    today = now_local().date()
    days = []
    d = today
    for _ in range(days_ahead):
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def build_days_kb(fid: str) -> InlineKeyboardMarkup:
    days = month_days_from_today(31)
    rows, row = [], []
    for iso in days:
        row.append(InlineKeyboardButton(fmt_date_ru(iso), callback_data=cb(fid, "day", iso)))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data=cb(fid, "menu"))])
    return InlineKeyboardMarkup(rows)


def generate_time_slots(day_iso: str) -> list[str]:
    start_dt, end_dt = work_bounds_for_day(day_iso)
    slots = []
    cur = start_dt
    while cur + timedelta(minutes=SLOT_MINUTES) <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots


def build_times_kb(fid: str, day_iso: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)

    # сегодня: только будущее
    if day_iso == now_local().date().isoformat():
        now_dt = now_local()
        filtered = []
        for hhmm in slots:
            hh, mm = map(int, hhmm.split(":"))
            slot_dt = datetime.combine(now_dt.date(), dtime(hh, mm)).replace(tzinfo=tz)
            if slot_dt > now_dt:
                filtered.append(hhmm)
        slots = filtered

    rows, row = [], []
    for t in slots:
        if store.is_slot_blocked(day_iso, t) or store.is_slot_taken(day_iso, t):
            continue
        row.append(InlineKeyboardButton(t, callback_data=cb(fid, "time", t)))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data=cb(fid, "noop"))]]

    rows.append([InlineKeyboardButton("✍️ Ввести время вручную", callback_data=cb(fid, "manual_time"))])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_days")),
                 InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))])
    return InlineKeyboardMarkup(rows)


def service_cats_kb(fid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Маникюр", callback_data=cb(fid, "cat", "mn"))],
        [InlineKeyboardButton("🦶 Педикюр", callback_data=cb(fid, "cat", "pd"))],
        [InlineKeyboardButton("🌟 Дополнительно", callback_data=cb(fid, "cat", "extra"))],
        [InlineKeyboardButton("⬅️ В меню", callback_data=cb(fid, "menu"))],
    ])


def services_list_kb(fid: str, cat: str) -> InlineKeyboardMarkup:
    if cat == "mn":
        keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd":
        keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else:
        keys = ["ext", "corr", "design"]

    rows = []
    for k in keys:
        title, price = SERVICES[k]
        rows.append([InlineKeyboardButton(f"{title} — {price} ₽", callback_data=cb(fid, "svc", k))])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_cats"))])
    rows.append([InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))])
    return InlineKeyboardMarkup(rows)


def admin_panel_inline(fid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Ближайшие (25)", callback_data=cb(fid, "adm_next"))],
        [InlineKeyboardButton("📅 Сегодня", callback_data=cb(fid, "adm_today")),
         InlineKeyboardButton("📆 7 дней", callback_data=cb(fid, "adm_7days"))],
        [InlineKeyboardButton("⛔ Блок слот", callback_data=cb(fid, "adm_block")),
         InlineKeyboardButton("✅ Разблок", callback_data=cb(fid, "adm_unblock"))],
        [InlineKeyboardButton("⬅️ В меню", callback_data=cb(fid, "menu"))],
    ])


# =========================================================
# REMINDERS
# =========================================================
def booking_start_dt(iso_date: str, hhmm: str) -> datetime:
    d = date.fromisoformat(iso_date)
    hh, mm = map(int, hhmm.split(":"))
    return datetime.combine(d, dtime(hh, mm)).replace(tzinfo=tz)


async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    booking_id = int(context.job.data.get("booking_id"))
    b = store.get_booking(booking_id)
    if not b or b.reminder_sent == 1 or b.status not in ("pending", "confirmed"):
        return

    text = (
        "⏰ *Напоминание о записи*\n\n"
        f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
        f"• {b.service_title}\n\n"
        f"{contacts_text()}\n"
        "Если планы изменились — нажмите *❌ Отменить запись*."
    )
    try:
        kb = contacts_inline()
        await context.bot.send_message(b.user_id, text, parse_mode="Markdown", reply_markup=kb)
        store.mark_reminder_sent(b.id)
    except Exception as e:
        log.exception("Reminder send failed: %s", e)


def schedule_reminder(app: Application, booking_id: int, iso_date: str, hhmm: str):
    start_dt = booking_start_dt(iso_date, hhmm)
    remind_at = start_dt - timedelta(hours=24)
    now = now_local()
    if remind_at <= now:
        return

    delay = (remind_at - now).total_seconds()
    name = f"rem_{booking_id}"
    for j in app.job_queue.jobs():
        if j.name == name:
            return
    app.job_queue.run_once(reminder_job, when=delay, data={"booking_id": booking_id}, name=name)


def reschedule_all_reminders(app: Application):
    now = now_local()
    for b in store.list_for_reminders():
        start_dt = booking_start_dt(b.book_date, b.book_time)
        if start_dt <= now:
            continue
        schedule_reminder(app, b.id, b.book_date, b.book_time)


# =========================================================
# REGISTRATION
# =========================================================
async def begin_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await send_screen(update, context, "📝 Регистрация\n\nКак к вам обращаться? (имя)", reply_markup=None)


async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return
    await delete_user_message_if_possible(update, context)

    phone = update.message.contact.phone_number if update.message.contact else ""
    phone = parse_phone(phone)
    if len(re.sub(r"\D", "", phone)) < 10:
        await send_screen(update, context, "Не вижу номер 😕 Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)

    context.user_data["stage"] = STAGE_NONE
    await send_screen(update, context, f"✅ Готово, *{name}*!\nТеперь можно записаться 💅",
                      parse_mode="Markdown", reply_markup=None, clean_before=True)


# =========================================================
# START — НЕ просим регистрацию сразу!
# =========================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # сброс сценариев (чтобы старые кнопки не ломали)
    context.user_data["stage"] = STAGE_NONE
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat", "flow_id"]:
        context.user_data.pop(k, None)

    await send_screen(
        update, context,
        f"✨ *{SALON_NAME}*\n\n"
        "Я помогу быстро записаться на маникюр/педикюр.\n\n"
        "Выберите действие в меню 👇",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(uid),
        clean_before=True
    )

    u = store.get_user(uid)
    if not u:
        msg = await context.bot.send_message(
            update.effective_chat.id,
            "Для записи нужна регистрация (1 раз).",
            reply_markup=after_start_inline()
        )
        track_bot_msg(context, msg.message_id)
    else:
        msg = await context.bot.send_message(
            update.effective_chat.id,
            "Хотите записаться прямо сейчас? 👇",
            reply_markup=after_start_inline()
        )
        track_bot_msg(context, msg.message_id)


# =========================================================
# CONFIRM TEXT
# =========================================================
def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d_iso = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    if not title or not d_iso or not t:
        return (
            "⚠️ Этот сценарий устарел (например, вы нажали /start или кнопки старые).\n\n"
            "Нажмите *💅 Записаться* и пройдите выбор заново 🙂"
        )

    if price is None:
        price = 0

    return (
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{fmt_dt_ru(d_iso, t)}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и заявка уйдёт администратору ✅"
    )


# =========================================================
# TEXT HANDLER
# =========================================================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # -------- REG NAME --------
    if stage == STAGE_REG_NAME:
        await delete_user_message_if_possible(update, context)
        if len(text) < 2:
            await send_screen(update, context, "Напишите имя чуть понятнее 🙂", reply_markup=None)
            return
        context.user_data["reg_name"] = text
        context.user_data["stage"] = STAGE_REG_PHONE
        await send_screen(update, context, "Отправьте номер телефона кнопкой ниже:", reply_markup=phone_request_kb())
        return

    # -------- REG PHONE TEXT --------
    if stage == STAGE_REG_PHONE:
        await delete_user_message_if_possible(update, context)
        phone = parse_phone(text)
        if len(re.sub(r"\D", "", phone)) < 10:
            await send_screen(update, context, "Нажмите «📱 Отправить номер» (так без ошибок).", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await send_screen(update, context, f"✅ Готово, *{name}*!\nТеперь можно записаться 💅",
                          parse_mode="Markdown", reply_markup=None)
        return

    # -------- MANUAL TIME --------
    if stage == STAGE_MANUAL_TIME:
        await delete_user_message_if_possible(update, context)
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            context.user_data["stage"] = STAGE_NONE
            await send_screen(update, context, "Сначала выберите дату 🙂")
            return
        ok, result = is_time_allowed_for_booking(day_iso, text)
        if not ok:
            await send_screen(update, context, f"😕 {result}\n\nВведите время ещё раз (например 17:15).")
            return
        context.user_data["book_time"] = result
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        fid = get_flow(context) or set_flow(context)
        await send_screen(
            update, context,
            "Добавьте комментарий (необязательно).\n\n"
            "Пример: «снятие», «укрепление», «френч».\n\n"
            "Можно нажать «Без комментария».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Без комментария", callback_data=cb(fid, "comment", "-"))],
                [InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_times")),
                 InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))],
            ])
        )
        return

    # -------- ASK MASTER --------
    if stage == STAGE_ASK_MASTER:
        await delete_user_message_if_possible(update, context)
        uu = store.get_user(uid)
        who = f"{uu.full_name} ({uu.phone})" if uu else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 *Вопрос мастеру*\nОт: *{who}*\n{link}\n\n{text}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
        except Exception:
            pass
        context.user_data["stage"] = STAGE_NONE
        await send_screen(update, context, "✅ Сообщение отправлено мастеру. Мы ответим вам скоро 🙂")
        return

    # -------- BOOK COMMENT (text) --------
    if stage == STAGE_BOOK_COMMENT:
        await delete_user_message_if_possible(update, context)
        context.user_data["comment"] = "" if text == "-" else text
        context.user_data["stage"] = STAGE_NONE
        fid = get_flow(context) or set_flow(context)
        await send_screen(
            update, context,
            build_confirm_text(context),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запись", callback_data=cb(fid, "confirm", ""))],
                [InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_times")),
                 InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))],
            ])
        )
        return

    # -------- MENU BUTTONS --------
    if text == "💅 Записаться":
        await delete_user_message_if_possible(update, context)
        if not u:
            await send_screen(update, context, "Для записи нужна регистрация (1 раз).", reply_markup=after_start_inline())
            return
        fid = set_flow(context)
        await send_screen(update, context, "Выберите категорию услуги:", reply_markup=service_cats_kb(fid))
        return

    if text == "💳 Цены":
        await delete_user_message_if_possible(update, context)
        await send_screen(update, context, prices_text(), parse_mode="Markdown")
        return

    if text == "👩‍🎨 Обо мне":
        await delete_user_message_if_possible(update, context)
        if MASTER_PHOTO:
            await send_photo_screen(update, context, MASTER_PHOTO, about_master_text(), parse_mode="Markdown")
        else:
            await send_screen(update, context, about_master_text(), parse_mode="Markdown")
        return

    if text == "📍 Контакты":
        await delete_user_message_if_possible(update, context)
        await send_screen(update, context, contacts_text(), parse_mode="Markdown", reply_markup=contacts_inline())
        return

    if text == "👤 Профиль":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        link = user_link(u.tg_id, u.username)
        await send_screen(
            update, context,
            "👤 *Профиль*\n\n"
            f"• Имя: *{u.full_name}*\n"
            f"• Телефон: *{u.phone}*\n"
            f"• Telegram: {link}\n\n"
            "Чтобы сбросить профиль — напишите: `Сброс профиля`",
            parse_mode="Markdown"
        )
        return

    if text.lower() in ["сброс профиля", "сброс", "сбросить профиль"]:
        await delete_user_message_if_possible(update, context)
        store.delete_user(uid)
        await send_screen(update, context, "✅ Профиль сброшен. Нажмите /start.", reply_markup=None)
        return

    if text == "📩 Вопрос мастеру":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        context.user_data["stage"] = STAGE_ASK_MASTER
        await send_screen(update, context, "Напишите ваш вопрос мастеру одним сообщением ✍️")
        return

    if text == "❌ Отменить запись":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        upcoming = store.list_user_upcoming(uid)
        if not upcoming:
            await send_screen(update, context, "У вас нет активных записей 🙂")
            return
        fid = set_flow(context)
        rows = []
        for b in upcoming:
            rows.append([InlineKeyboardButton(
                f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
                callback_data=cb(fid, "ucancel", str(b.id))
            )])
        rows.append([InlineKeyboardButton("⬅️ В меню", callback_data=cb(fid, "menu"))])
        await send_screen(update, context, "Выберите запись для отмены:", reply_markup=InlineKeyboardMarkup(rows))
        return

    if text == "🛠 Админ-панель" and is_admin(uid):
        await delete_user_message_if_possible(update, context)
        fid = set_flow(context)
        await send_screen(update, context, "🛠 *Админ-панель*", parse_mode="Markdown", reply_markup=admin_panel_inline(fid))
        return

    # другое
    await delete_user_message_if_possible(update, context)
    await send_screen(update, context, "Выберите действие в меню 👇")


# =========================================================
# CALLBACKS
# =========================================================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data or ""

    action, fid, payload = parse_cb(data)

    # noop
    if action == "noop":
        return

    # старые кнопки
    if action not in ("go_book", "go_profile") and fid:
        if not is_fid_ok(context, fid):
            await q.message.reply_text("⚠️ Кнопки устарели. Нажмите *💅 Записаться* заново 🙂", parse_mode="Markdown")
            return

    # menu
    if action == "menu":
        context.user_data["stage"] = STAGE_NONE
        await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))
        return

    # quick actions from /start inline
    if action == "go_book":
        if not store.get_user(uid):
            await q.message.reply_text("Для записи нужна регистрация (1 раз). Нажмите *👤 Профиль* или пришлите имя 🙂", parse_mode="Markdown")
            await begin_registration(update, context)
            return
        fid2 = set_flow(context)
        await q.message.reply_text("Выберите категорию услуги:", reply_markup=service_cats_kb(fid2))
        return

    if action == "go_profile":
        if not store.get_user(uid):
            await begin_registration(update, context)
        else:
            await q.message.reply_text("Профиль доступен в кнопке *👤 Профиль* в меню.", parse_mode="Markdown")
        return

    # service flow
    if action == "back_cats":
        await q.message.reply_text("Выберите категорию услуги:", reply_markup=service_cats_kb(fid))
        return

    if action == "cat":
        context.user_data["book_cat"] = payload
        await q.message.reply_text("Выберите услугу:", reply_markup=services_list_kb(fid, payload))
        return

    if action == "svc":
        key = payload
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)
        context.user_data.pop("book_date", None)
        context.user_data.pop("book_time", None)
        context.user_data.pop("comment", None)
        await q.message.reply_text(
            f"Вы выбрали:\n*{title}* — *{price} ₽*\n\nВыберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(fid)
        )
        return

    if action == "back_days":
        await q.message.reply_text("Выберите дату:", reply_markup=build_days_kb(fid))
        return

    if action == "day":
        context.user_data["book_date"] = payload
        context.user_data.pop("book_time", None)
        await q.message.reply_text(
            f"Дата: *{fmt_date_ru(payload)}*\nВыберите время (свободные слоты):",
            parse_mode="Markdown",
            reply_markup=build_times_kb(fid, payload)
        )
        return

    if action == "back_times":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.message.reply_text("Выберите дату:", reply_markup=build_days_kb(fid))
            return
        await q.message.reply_text("Выберите время:", reply_markup=build_times_kb(fid, day_iso))
        return

    if action == "manual_time":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.message.reply_text("Сначала выберите дату 🙂", reply_markup=build_days_kb(fid))
            return
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await q.message.reply_text(
            "✍️ Введите время вручную (например 17:15)\n"
            "Минуты: 00/15/30/45",
        )
        return

    if action == "time":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.message.reply_text("Сначала выберите дату 🙂", reply_markup=build_days_kb(fid))
            return
        ok, res = is_time_allowed_for_booking(day_iso, payload)
        if not ok:
            await q.message.reply_text(f"😕 {res}")
            await q.message.reply_text("Выберите время:", reply_markup=build_times_kb(fid, day_iso))
            return

        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT

        await q.message.reply_text(
            "Добавьте комментарий (необязательно).\n\n"
            "Пример: «снятие», «укрепление», «френч».\n\n"
            "Можно нажать «Без комментария».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Без комментария", callback_data=cb(fid, "comment", "-"))],
                [InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_times")),
                 InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))],
            ])
        )
        return

    if action == "comment":
        context.user_data["comment"] = "" if payload == "-" else payload
        context.user_data["stage"] = STAGE_NONE
        await q.message.reply_text(
            build_confirm_text(context),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запись", callback_data=cb(fid, "confirm", ""))],
                [InlineKeyboardButton("⬅️ Назад", callback_data=cb(fid, "back_times")),
                 InlineKeyboardButton("В меню", callback_data=cb(fid, "menu"))],
            ])
        )
        return

    if action == "confirm":
        u = store.get_user(uid)
        if not u:
            await q.message.reply_text("Нужна регистрация (1 раз).")
            await begin_registration(update, context)
            return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0) or 0)
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        if not (key and title and d_iso and t and price > 0):
            await q.message.reply_text("⚠️ Сценарий устарел. Нажмите *💅 Записаться* заново 🙂", parse_mode="Markdown")
            return

        ok, checked_time = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await q.message.reply_text(f"😕 {checked_time}")
            await q.message.reply_text("Выберите время:", reply_markup=build_times_kb(fid, d_iso))
            return

        booking_id = store.create_booking(u.tg_id, key, title, price, d_iso, checked_time, comment)
        schedule_reminder(context.application, booking_id, d_iso, checked_time)

        await q.message.reply_text(
            "✅ *Заявка отправлена администратору!*\n\n"
            f"• {fmt_dt_ru(d_iso, checked_time)}\n"
            f"• {title}\n\n"
            "Мы подтвердим запись и пришлём уведомление 🙂",
            parse_mode="Markdown"
        )

        link = user_link(u.tg_id, u.username)
        admin_text = (
            "🆕 *Новая запись*\n\n"
            f"• Клиент: *{u.full_name}*\n"
            f"• Телефон: *{u.phone}*\n"
            f"• TG: {link}\n"
            f"• Услуга: *{title}* — *{price} ₽*\n"
            f"• Дата/время: *{fmt_dt_ru(d_iso, checked_time)}*\n"
            f"• Комментарий: *{comment or '—'}*\n\n"
            f"ID записи: `{booking_id}`"
        )
        admin_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=cb(fid, "adm_confirm", str(booking_id))),
             InlineKeyboardButton("❌ Отменить", callback_data=cb(fid, "adm_cancel", str(booking_id)))],
            [InlineKeyboardButton("💬 Написать клиенту", url=link)],
        ])
        try:
            await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown", reply_markup=admin_kb)
        except Exception:
            pass

        # очистка черновика
        for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat"]:
            context.user_data.pop(k, None)
        return

    if action == "ucancel":
        booking_id = int(payload or "0")
        b = store.get_booking(booking_id)
        if not b or b.user_id != uid:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "cancelled")
        await q.message.reply_text("✅ Запись отменена.")
        return

    # ================= ADMIN =================
    if action == "adm_next" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await q.message.reply_text("Ближайших записей нет 🙂")
            return
        lines = ["📌 *Ближайшие записи*", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• *{fmt_dt_ru(b.book_date, b.book_time)}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if action == "adm_today" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await q.message.reply_text("На сегодня записей нет 🙂")
            return
        lines = [f"📅 *Сегодня* ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if action == "adm_7days" and is_admin(uid):
        today = now_local().date()
        day_from = today.isoformat()
        day_to = (today + timedelta(days=6)).isoformat()
        items = store.list_range(day_from, day_to)
        if not items:
            await q.message.reply_text("На ближайшие 7 дней записей нет 🙂")
            return

        lines = [f"📆 *Записи на 7 дней* ({fmt_date_ru(day_from)} — {fmt_date_ru(day_to)})", ""]
        cur_day = ""
        for b in items:
            if b.book_date != cur_day:
                cur_day = b.book_date
                lines.append(f"\n*{fmt_date_ru(cur_day)}*")
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if action == "adm_block" and is_admin(uid):
        context.user_data["adm_mode"] = "block"
        await q.message.reply_text("⛔ *Блокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                   reply_markup=build_days_kb(set_flow(context)))
        return

    if action == "adm_unblock" and is_admin(uid):
        context.user_data["adm_mode"] = "unblock"
        await q.message.reply_text("✅ *Разблокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                   reply_markup=build_days_kb(set_flow(context)))
        return

    if action == "adm_confirm" and is_admin(uid):
        booking_id = int(payload or "0")
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "confirmed")
        try:
            await context.bot.send_message(
                b.user_id,
                "✅ *Запись подтверждена!*\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {b.service_title}\n\n"
                f"{contacts_text()}",
                parse_mode="Markdown",
                reply_markup=contacts_inline()
            )
        except Exception:
            pass
        await q.message.reply_text(f"✅ Подтверждено (ID {booking_id})")
        return

    if action == "adm_cancel" and is_admin(uid):
        booking_id = int(payload or "0")
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "cancelled")
        try:
            await context.bot.send_message(
                b.user_id,
                "❌ *Запись отменена администратором.*\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {b.service_title}\n\n"
                "Хотите — подберём другое время 🙂",
                parse_mode="Markdown",
            )
        except Exception:
            pass
        await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
        return

    return


# =========================================================
# STARTUP / ERROR / APP
# =========================================================
async def on_startup(app: Application):
    reschedule_all_reminders(app)
    log.info("Startup: reminders rescheduled.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)
    app.post_init = on_startup

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    return app


def main():
    app = build_app()
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
