# bot.py
# python-telegram-bot >= 21
# Railway ENV:
# BOT_TOKEN, ADMIN_ID, TZ, DB_PATH, SALON_NAME, ...
# Optional:
# WORK_START, WORK_END, SLOT_MINUTES, TIME_STEP_MINUTES, AUTO_DELETE_USER_INPUT

import os
import re
import html
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
    ReplyKeyboardRemove,
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


# =========================
# Models
# =========================
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


# =========================
# Storage
# =========================
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
                created_at TEXT NOT NULL,
                reminder_sent INTEGER NOT NULL DEFAULT 0
            );
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS blocked_slots(
                book_date TEXT NOT NULL,
                book_time TEXT NOT NULL,
                PRIMARY KEY(book_date, book_time)
            );
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_day_time ON bookings(book_date, book_time);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id);")

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
                created_at=row["created_at"],
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
            return self._row_to_booking(row) if row else None

    def set_booking_status(self, booking_id: int, status: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))

    def mark_reminder_sent(self, booking_id: int) -> None:
        with self._conn() as c:
            c.execute("UPDATE bookings SET reminder_sent=1 WHERE id=?", (booking_id,))

    def list_day_active(self, day: str) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE book_date=? AND status IN ('pending','confirmed')
            ORDER BY book_time
            """, (day,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_range_active(self, day_from: str, day_to: str) -> List[Booking]:
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

    def list_user_upcoming(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE user_id=? AND status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            """, (user_id,)).fetchall()
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
            reminder_sent=int(row["reminder_sent"]),
        )


# =========================
# ENV
# =========================
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
    "Подберу форму и покрытие под ваш стиль, чтобы носилось красиво и долго."
).strip()
MASTER_PHOTO = os.getenv("MASTER_PHOTO", "").strip()

WORK_START = os.getenv("WORK_START", "08:00").strip()
WORK_END = os.getenv("WORK_END", "23:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")         # длительность записи
TIME_STEP_MINUTES = int(os.getenv("TIME_STEP_MINUTES", "15").strip() or "15") # шаг выбора времени
AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")
if 60 % TIME_STEP_MINUTES != 0:
    raise RuntimeError("TIME_STEP_MINUTES должен делить 60 (например 5/10/15/20/30)")

tz = ZoneInfo(TZ_NAME)
store = Storage(DB_PATH)


# =========================
# Services
# =========================
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

STAGE_NONE = "none"
STAGE_REG_NAME = "reg_name"
STAGE_REG_PHONE = "reg_phone"
STAGE_BOOK_COMMENT = "book_comment"
STAGE_MANUAL_TIME = "manual_time"
STAGE_ASK_MASTER = "ask_master"


# =========================
# Helpers
# =========================
def h(text: str) -> str:
    return html.escape(str(text or ""))

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
    return (
        datetime.combine(d, ws).replace(tzinfo=tz),
        datetime.combine(d, we).replace(tzinfo=tz),
    )

def booking_start_dt(day_iso: str, hhmm: str) -> datetime:
    d = date.fromisoformat(day_iso)
    hh, mm = map(int, hhmm.split(":"))
    return datetime.combine(d, dtime(hh, mm)).replace(tzinfo=tz)

def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)

def is_slot_taken_overlap(day_iso: str, hhmm: str) -> bool:
    start = booking_start_dt(day_iso, hhmm)
    end = start + timedelta(minutes=SLOT_MINUTES)
    for b in store.list_day_active(day_iso):
        bs = booking_start_dt(b.book_date, b.book_time)
        be = bs + timedelta(minutes=SLOT_MINUTES)
        if overlaps(start, end, bs, be):
            return True
    return False

def is_time_allowed_for_booking(day_iso: str, hhmm: str) -> Tuple[bool, str]:
    parsed = parse_hhmm(hhmm)
    if not parsed:
        return False, "Введите время в формате HH:MM (например 17:15)."

    hh, mm = map(int, parsed.split(":"))
    if mm % TIME_STEP_MINUTES != 0:
        return False, f"Минуты должны быть кратны шагу {TIME_STEP_MINUTES} (например 17:{TIME_STEP_MINUTES:02d})."

    start_dt, end_dt = work_bounds_for_day(day_iso)
    slot_start = booking_start_dt(day_iso, parsed)
    slot_end = slot_start + timedelta(minutes=SLOT_MINUTES)

    if slot_start < start_dt or slot_end > end_dt:
        return False, f"Время должно быть в пределах {WORK_START}–{WORK_END} (с учетом длительности {SLOT_MINUTES} мин)."

    if day_iso == now_local().date().isoformat() and slot_start <= now_local():
        return False, "Это время уже прошло."

    if store.is_slot_blocked(day_iso, parsed):
        return False, "Это время заблокировано."
    if is_slot_taken_overlap(day_iso, parsed):
        return False, "Это время уже занято."

    return True, parsed

def enc_time(hhmm: str) -> str:
    return hhmm.replace(":", "")

def dec_time(hhmm4: str) -> str:
    return f"{hhmm4[:2]}:{hhmm4[2:]}"


# =========================
# UI
# =========================
def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩‍🎨 Обо мне", "📍 Контакты"],
        ["👤 Профиль", "📩 Вопрос мастеру"],
        ["❌ Отменить запись", "🏠 Меню"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def about_master_text() -> str:
    return (
        f"👩‍🎨 <b>{h(MASTER_NAME)}</b>\n"
        f"<b>{h(MASTER_EXPERIENCE)}</b>\n\n"
        f"{h(MASTER_TEXT)}\n\n"
        "Нажмите <b>💅 Записаться</b> ✅"
    )

def prices_text() -> str:
    lines = [
        "💳 <b>Цены</b>", "",
        "✨ Маникюр",
        f"• {h(SERVICES['mn_no'][0])} — <b>{SERVICES['mn_no'][1]} ₽</b>",
        f"• {h(SERVICES['mn_cov'][0])} — <b>{SERVICES['mn_cov'][1]} ₽</b>",
        f"• {h(SERVICES['mn_cov_design'][0])} — <b>{SERVICES['mn_cov_design'][1]} ₽</b>",
        "",
        "🦶 Педикюр",
        f"• {h(SERVICES['pd_no'][0])} — <b>{SERVICES['pd_no'][1]} ₽</b>",
        f"• {h(SERVICES['pd_cov'][0])} — <b>{SERVICES['pd_cov'][1]} ₽</b>",
        f"• {h(SERVICES['pd_toes'][0])} — <b>{SERVICES['pd_toes'][1]} ₽</b>",
        f"• {h(SERVICES['pd_heels'][0])} — <b>{SERVICES['pd_heels'][1]} ₽</b>",
        "",
        "🌟 Дополнительно",
        f"• {h(SERVICES['ext'][0])} — <b>{SERVICES['ext'][1]} ₽</b>",
        f"• {h(SERVICES['corr'][0])} — <b>{SERVICES['corr'][1]} ₽</b>",
        f"• {h(SERVICES['design'][0])} — <b>{SERVICES['design'][1]} ₽</b>",
    ]
    return "\n".join(lines)

def contacts_text() -> str:
    return (
        "📍 <b>Как нас найти</b>\n\n"
        f"• {h(ADDRESS)}\n"
        f"• {h(HOW_TO_FIND)}\n"
        f"• {h(DOORPHONE)}\n"
        f"• {h(GATE)}\n"
        f"• {h(FLOOR)}\n"
        f"• {h(APARTMENT)}\n"
        f"\n🕘 Время работы: <b>{h(WORK_START)}–{h(WORK_END)}</b>\n"
    )

def contacts_inline() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if YANDEX_MAP_URL:
        rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None


# =========================
# Draft / flow
# =========================
def new_draft_id() -> str:
    return secrets.token_hex(3)

def set_draft(context: ContextTypes.DEFAULT_TYPE) -> str:
    did = new_draft_id()
    context.user_data["draft_id"] = did
    return did

def get_draft(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.user_data.get("draft_id", ""))

def must_draft(context: ContextTypes.DEFAULT_TYPE, did: str) -> bool:
    return did and did == get_draft(context)

def clear_booking_draft(context: ContextTypes.DEFAULT_TYPE):
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time",
              "comment", "book_cat", "draft_id", "stage"]:
        context.user_data.pop(k, None)


# =========================
# Keyboards
# =========================
def kb_start_for_new_user() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Регистрация", callback_data="REG_START")],
        [InlineKeyboardButton("💅 Записаться", callback_data="BOOK_START")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_service_cats(did: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Маникюр", callback_data=f"CAT:{did}:mn")],
        [InlineKeyboardButton("🦶 Педикюр", callback_data=f"CAT:{did}:pd")],
        [InlineKeyboardButton("🌟 Дополнительно", callback_data=f"CAT:{did}:extra")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_services(did: str, cat: str) -> InlineKeyboardMarkup:
    keys = {"mn": ["mn_no", "mn_cov", "mn_cov_design"],
            "pd": ["pd_no", "pd_cov", "pd_toes", "pd_heels"]}.get(cat, ["ext", "corr", "design"])
    rows = []
    for k in keys:
        title, price = SERVICES[k]
        rows.append([InlineKeyboardButton(f"{title} — {price} ₽", callback_data=f"SVC:{did}:{k}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_CATS:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def days_from_today(n: int = 31) -> List[str]:
    d = now_local().date()
    return [(d + timedelta(days=i)).isoformat() for i in range(n)]

def kb_days(did: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for iso in days_from_today(31):
        row.append(InlineKeyboardButton(fmt_date_ru(iso), callback_data=f"DAY:{did}:{iso}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_SVC:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def generate_time_slots(day_iso: str) -> List[str]:
    start_dt, end_dt = work_bounds_for_day(day_iso)
    slots = []
    cur = start_dt
    while cur + timedelta(minutes=SLOT_MINUTES) <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=TIME_STEP_MINUTES)
    return slots

def kb_times(did: str, day_iso: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)
    rows, row = [], []
    for t in slots:
        ok, _ = is_time_allowed_for_booking(day_iso, t)
        if not ok:
            continue
        row.append(InlineKeyboardButton(t, callback_data=f"TIME:{did}:{enc_time(t)}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data="NOOP")]]

    rows.append([InlineKeyboardButton("✍️ Ввести время вручную", callback_data=f"MANUAL_TIME:{did}:1")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_DAYS:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_comment(did: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Без комментария", callback_data=f"COMMENT:{did}:-")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_TIMES:{did}")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_confirm(did: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить запись", callback_data=f"CONFIRM:{did}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_TIMES:{did}")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

# Admin
def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Ближайшие (25)", callback_data="ADM_NEXT")],
        [InlineKeyboardButton("📅 Сегодня", callback_data="ADM_TODAY"),
         InlineKeyboardButton("📆 7 дней", callback_data="ADM_7D")],
        [InlineKeyboardButton("⛔ Блок слот", callback_data="ADM_BLOCK"),
         InlineKeyboardButton("✅ Разблок", callback_data="ADM_UNBLOCK")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_adm_confirm_cancel(booking_id: int, client_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_CONFIRM:{booking_id}"),
         InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_CANCEL:{booking_id}")],
        [InlineKeyboardButton("💬 Написать клиенту", url=client_url)],
    ])

def kb_admin_days(did: str, mode: str) -> InlineKeyboardMarkup:
    prefix = "ADM_DAYB" if mode == "block" else "ADM_DAYU"
    rows, row = [], []
    for iso in days_from_today(31):
        row.append(InlineKeyboardButton(fmt_date_ru(iso), callback_data=f"{prefix}:{did}:{iso}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="ADM_PANEL")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_admin_slots(did: str, day_iso: str, mode: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    blocked = set(store.list_blocked_for_day(day_iso))
    for t in generate_time_slots(day_iso):
        if mode == "block":
            if t in blocked:
                continue
            if is_slot_taken_overlap(day_iso, t):
                continue
            cb = f"ADM_BSET:{did}:{enc_time(t)}"
            label = f"⛔ {t}"
        else:
            if t not in blocked:
                continue
            cb = f"ADM_USET:{did}:{enc_time(t)}"
            label = f"✅ {t}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if not rows:
        rows = [[InlineKeyboardButton("Нет слотов", callback_data="NOOP")]]

    back_cb = "ADM_BLOCK" if mode == "block" else "ADM_UNBLOCK"
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)


# =========================
# Message clean helpers
# =========================
async def safe_delete_message(bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def cleanup_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not AUTO_DELETE_USER_INPUT:
        return
    if update.message:
        await safe_delete_message(context.bot, update.effective_chat.id, update.message.message_id)

async def show_flow_message(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str,
                            reply_markup=None, remove_reply=False):
    old_id = context.user_data.get("flow_msg_id")
    if old_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=old_id, text=text, parse_mode="HTML", reply_markup=reply_markup
            )
            return
        except Exception:
            pass
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=reply_markup if not remove_reply else ReplyKeyboardRemove()
    )
    context.user_data["flow_msg_id"] = msg.message_id

async def reset_to_menu(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE, text: str = "Меню 👇"):
    context.user_data["stage"] = STAGE_NONE
    context.user_data.pop("flow_msg_id", None)
    await context.bot.send_message(chat_id, text, reply_markup=main_menu_kb(user_id))


# =========================
# Build texts
# =========================
def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d_iso = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    if not title or not d_iso or not t:
        return "⚠️ Сценарий устарел. Нажмите <b>💅 Записаться</b> заново."

    return (
        "Проверьте, всё верно:\n\n"
        f"• Услуга: <b>{h(title)}</b>\n"
        f"• Цена: <b>{price} ₽</b>\n"
        f"• Дата/время: <b>{fmt_dt_ru(d_iso, t)}</b>\n"
        f"• Комментарий: <b>{h(comment)}</b>\n\n"
        "Нажмите <b>Подтвердить запись</b> ✅"
    )


# =========================
# Reminders
# =========================
async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    booking_id = int(context.job.data.get("booking_id"))
    b = store.get_booking(booking_id)
    if not b or b.reminder_sent == 1 or b.status not in ("pending", "confirmed"):
        return

    text = (
        "⏰ <b>Напоминание о записи</b>\n\n"
        f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
        f"• {h(b.service_title)}\n\n"
        f"{contacts_text()}\n"
        "Если планы изменились — нажмите <b>❌ Отменить запись</b>."
    )
    try:
        await context.bot.send_message(b.user_id, text, parse_mode="HTML", reply_markup=contacts_inline())
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
        if booking_start_dt(b.book_date, b.book_time) > now:
            schedule_reminder(app, b.id, b.book_date, b.book_time)


# =========================
# Handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    clear_booking_draft(context)

    intro = (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Я помогу записаться в пару кликов:\n"
        "1) выбрать услугу\n"
        "2) выбрать дату и время\n"
        "3) подтвердить запись ✅"
    )
    await update.message.reply_text(intro, parse_mode="HTML", reply_markup=main_menu_kb(uid))
    u = store.get_user(uid)
    if not u:
        await update.message.reply_text("Чтобы записаться, нужна регистрация (1 раз).", reply_markup=kb_start_for_new_user())
    else:
        await update.message.reply_text("Можно сразу нажать <b>💅 Записаться</b> 🙂", parse_mode="HTML")

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reset_to_menu(update.effective_chat.id, update.effective_user.id, context)

async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await show_flow_message(
        chat_id=update.effective_chat.id,
        context=context,
        text="📝 Регистрация (1 раз)\n\nКак к вам обращаться? (имя)",
        remove_reply=True,
    )

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return
    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await update.message.reply_text("Номер не распознан. Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = STAGE_NONE

    await reset_to_menu(update.effective_chat.id, uid, context, f"✅ Готово, {h(name)}! Теперь можно записаться 💅")
    await cleanup_user_input(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt = (update.message.text or "").strip()
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # registration flow
    if stage == STAGE_REG_NAME:
        if len(txt) < 2:
            await update.message.reply_text("Напишите имя чуть понятнее 🙂")
            return
        context.user_data["reg_name"] = txt
        context.user_data["stage"] = STAGE_REG_PHONE
        await update.message.reply_text("Отправьте номер кнопкой ниже:", reply_markup=phone_request_kb())
        await cleanup_user_input(update, context)
        return

    if stage == STAGE_REG_PHONE:
        phone = parse_phone(txt)
        if len(re.sub(r"\D", "", phone)) < 10:
            await update.message.reply_text("Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await reset_to_menu(update.effective_chat.id, uid, context, f"✅ Готово, {h(name)}! Теперь можно записаться 💅")
        await cleanup_user_input(update, context)
        return

    # manual time
    if stage == STAGE_MANUAL_TIME:
        did = get_draft(context)
        day_iso = context.user_data.get("book_date")
        if not did or not day_iso:
            context.user_data["stage"] = STAGE_NONE
            await reset_to_menu(update.effective_chat.id, uid, context, "Сценарий устарел. Нажмите 💅 Записаться заново.")
            return
        ok, res = is_time_allowed_for_booking(day_iso, txt)
        if not ok:
            await update.message.reply_text(f"😕 {res}\nВведите время ещё раз.")
            return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await show_flow_message(update.effective_chat.id, context,
                                "Добавьте комментарий (необязательно) или нажмите «Без комментария».",
                                reply_markup=kb_comment(did))
        await cleanup_user_input(update, context)
        return

    # comment text
    if stage == STAGE_BOOK_COMMENT:
        did = get_draft(context)
        if not did:
            context.user_data["stage"] = STAGE_NONE
            await reset_to_menu(update.effective_chat.id, uid, context, "Сценарий устарел. Нажмите 💅 Записаться заново.")
            return
        context.user_data["comment"] = txt
        context.user_data["stage"] = STAGE_NONE
        await show_flow_message(update.effective_chat.id, context, build_confirm_text(context), reply_markup=kb_confirm(did))
        await cleanup_user_input(update, context)
        return

    # ask master
    if stage == STAGE_ASK_MASTER:
        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n{h(link)}\n\n{h(txt)}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
            await reset_to_menu(update.effective_chat.id, uid, context, "✅ Отправил мастеру. Скоро ответим 🙂")
        except Exception as e:
            log.exception("Send to admin failed: %s", e)
            await reset_to_menu(update.effective_chat.id, uid, context,
                                "⚠️ Не удалось отправить. Проверьте ADMIN_ID и /start у мастера.")
        await cleanup_user_input(update, context)
        return

    # menu buttons
    if txt in ("🏠 Меню",):
        await reset_to_menu(update.effective_chat.id, uid, context)
        await cleanup_user_input(update, context)
        return

    if txt == "💅 Записаться":
        if not u:
            await update.message.reply_text("Сначала регистрация 🙂", reply_markup=kb_start_for_new_user())
            return
        did = set_draft(context)
        await show_flow_message(update.effective_chat.id, context, "Выберите категорию услуги:",
                                reply_markup=kb_service_cats(did), remove_reply=True)
        await cleanup_user_input(update, context)
        return

    if txt == "💳 Цены":
        await update.message.reply_text(prices_text(), parse_mode="HTML", reply_markup=main_menu_kb(uid))
        return

    if txt == "👩‍🎨 Обо мне":
        if MASTER_PHOTO:
            await update.message.reply_photo(MASTER_PHOTO, caption=about_master_text(), parse_mode="HTML",
                                             reply_markup=main_menu_kb(uid))
        else:
            await update.message.reply_text(about_master_text(), parse_mode="HTML", reply_markup=main_menu_kb(uid))
        return

    if txt == "📍 Контакты":
        await update.message.reply_text(contacts_text(), parse_mode="HTML",
                                        reply_markup=contacts_inline() or main_menu_kb(uid))
        return

    if txt == "👤 Профиль":
        if not u:
            await update.message.reply_text("Профиля нет. Нажмите «📝 Регистрация».", reply_markup=kb_start_for_new_user())
            return
        link = user_link(u.tg_id, u.username)
        await update.message.reply_text(
            "👤 <b>Профиль</b>\n\n"
            f"• Имя: <b>{h(u.full_name)}</b>\n"
            f"• Телефон: <b>{h(u.phone)}</b>\n"
            f"• Telegram: {h(link)}\n\n"
            "Чтобы сбросить профиль — напишите: <code>Сброс профиля</code>",
            parse_mode="HTML",
            reply_markup=main_menu_kb(uid)
        )
        return

    if txt.lower() in ["сброс профиля", "сброс", "сбросить профиль"]:
        store.delete_user(uid)
        await update.message.reply_text("✅ Профиль сброшен. Нажмите /start.", reply_markup=main_menu_kb(uid))
        return

    if txt == "📩 Вопрос мастеру":
        if not u:
            await update.message.reply_text("Сначала регистрация 🙂", reply_markup=kb_start_for_new_user())
            return
        context.user_data["stage"] = STAGE_ASK_MASTER
        await update.message.reply_text("Напишите вопрос одним сообщением ✍️", reply_markup=ReplyKeyboardRemove())
        return

    if txt == "❌ Отменить запись":
        if not u:
            await update.message.reply_text("Сначала регистрация 🙂", reply_markup=kb_start_for_new_user())
            return
        items = store.list_user_upcoming(uid)
        if not items:
            await update.message.reply_text("У вас нет активных записей 🙂", reply_markup=main_menu_kb(uid))
            return
        did = set_draft(context)
        rows = [[InlineKeyboardButton(f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
                                      callback_data=f"UCANCEL:{did}:{b.id}")]
                for b in items]
        rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
        await show_flow_message(update.effective_chat.id, context, "Выберите запись для отмены:",
                                reply_markup=InlineKeyboardMarkup(rows), remove_reply=True)
        return

    if txt == "🛠 Админ-панель" and is_admin(uid):
        await show_flow_message(update.effective_chat.id, context, "🛠 Админ-панель",
                                reply_markup=kb_admin_panel(), remove_reply=True)
        return

    await update.message.reply_text("Выберите действие в меню 👇", reply_markup=main_menu_kb(uid))


async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    chat_id = q.message.chat_id
    data = q.data or ""

    def split3(prefix: str) -> Optional[Tuple[str, str]]:
        if not data.startswith(prefix + ":"):
            return None
        parts = data.split(":", 2)
        return (parts[1], parts[2]) if len(parts) == 3 else None

    if data == "NOOP":
        return

    if data == "MENU":
        clear_booking_draft(context)
        await reset_to_menu(chat_id, uid, context)
        return

    if data == "ADM_PANEL" and is_admin(uid):
        await show_flow_message(chat_id, context, "🛠 Админ-панель", reply_markup=kb_admin_panel())
        return

    # registration quick
    if data == "REG_START":
        await reg_start(update, context)
        return

    if data == "BOOK_START":
        u = store.get_user(uid)
        if not u:
            await show_flow_message(chat_id, context,
                                    "Для записи нужна регистрация (1 раз).\nСейчас спрошу имя и телефон 🙂")
            await reg_start(update, context)
            return
        did = set_draft(context)
        await show_flow_message(chat_id, context, "Выберите категорию услуги:",
                                reply_markup=kb_service_cats(did), remove_reply=True)
        return

    # back
    if data.startswith("BACK_CATS:"):
        did = data.split(":", 1)[1]
        if must_draft(context, did):
            await show_flow_message(chat_id, context, "Выберите категорию услуги:", reply_markup=kb_service_cats(did))
        return

    if data.startswith("BACK_SVC:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            return
        cat = context.user_data.get("book_cat")
        if not cat:
            await show_flow_message(chat_id, context, "Выберите категорию услуги:", reply_markup=kb_service_cats(did))
            return
        await show_flow_message(chat_id, context, "Выберите услугу:", reply_markup=kb_services(did, cat))
        return

    if data.startswith("BACK_DAYS:"):
        did = data.split(":", 1)[1]
        if must_draft(context, did):
            await show_flow_message(chat_id, context, "Выберите дату:", reply_markup=kb_days(did))
        return

    if data.startswith("BACK_TIMES:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            return
        day_iso = context.user_data.get("book_date")
        if day_iso:
            await show_flow_message(chat_id, context, "Выберите время:", reply_markup=kb_times(did, day_iso))
        return

    # category
    p = split3("CAT")
    if p:
        did, cat = p
        if not must_draft(context, did):
            return
        context.user_data["book_cat"] = cat
        await show_flow_message(chat_id, context, "Выберите услугу:", reply_markup=kb_services(did, cat))
        return

    # service
    p = split3("SVC")
    if p:
        did, key = p
        if not must_draft(context, did):
            return
        if key not in SERVICES:
            return
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)
        context.user_data.pop("book_date", None)
        context.user_data.pop("book_time", None)
        context.user_data.pop("comment", None)
        await show_flow_message(chat_id, context,
                                f"Вы выбрали:\n<b>{h(title)}</b> — <b>{price} ₽</b>\n\nВыберите дату:",
                                reply_markup=kb_days(did))
        return

    # day
    p = split3("DAY")
    if p:
        did, day_iso = p
        if not must_draft(context, did):
            return
        context.user_data["book_date"] = day_iso
        context.user_data.pop("book_time", None)
        await show_flow_message(chat_id, context,
                                f"Дата: <b>{fmt_date_ru(day_iso)}</b>\nВыберите время:",
                                reply_markup=kb_times(did, day_iso))
        return

    # time
    p = split3("TIME")
    if p:
        did, hhmm4 = p
        if not must_draft(context, did):
            return
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await show_flow_message(chat_id, context, "Сначала выберите дату:", reply_markup=kb_days(did))
            return
        hhmm = dec_time(hhmm4)
        ok, res = is_time_allowed_for_booking(day_iso, hhmm)
        if not ok:
            await show_flow_message(chat_id, context, f"😕 {res}\nВыберите другое время:", reply_markup=kb_times(did, day_iso))
            return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await show_flow_message(chat_id, context,
                                "Добавьте комментарий (необязательно) или нажмите «Без комментария».",
                                reply_markup=kb_comment(did))
        return

    p = split3("MANUAL_TIME")
    if p:
        did, _ = p
        if not must_draft(context, did):
            return
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await show_flow_message(chat_id, context,
                                f"Введите время вручную (например 17:15).\nШаг: {TIME_STEP_MINUTES} минут.")
        return

    # comment
    if data.startswith("COMMENT:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, payload = parts[1], parts[2]
        if not must_draft(context, did):
            return
        context.user_data["comment"] = "" if payload == "-" else payload
        context.user_data["stage"] = STAGE_NONE
        await show_flow_message(chat_id, context, build_confirm_text(context), reply_markup=kb_confirm(did))
        return

    # confirm booking by client
    if data.startswith("CONFIRM:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await show_flow_message(chat_id, context, "Кнопки устарели. Нажмите 💅 Записаться заново.")
            return

        u = store.get_user(uid)
        if not u:
            await show_flow_message(chat_id, context, "Сначала регистрация.", reply_markup=kb_start_for_new_user())
            return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0) or 0)
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        if not (key and title and d_iso and t and price > 0):
            await show_flow_message(chat_id, context, "⚠️ Сценарий устарел. Нажмите 💅 Записаться заново.")
            return

        ok, checked = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await show_flow_message(chat_id, context, f"😕 {checked}\nВыберите другое время:", reply_markup=kb_times(did, d_iso))
            return

        booking_id = store.create_booking(u.tg_id, key, title, price, d_iso, checked, comment)
        schedule_reminder(context.application, booking_id, d_iso, checked)

        await show_flow_message(
            chat_id, context,
            "✅ <b>Запись создана!</b>\n\n"
            f"• {fmt_dt_ru(d_iso, checked)}\n"
            f"• {h(title)}\n\n"
            "Ожидайте подтверждение мастера 🙂"
        )

        # notify admin (важно: HTML escaping, чтобы не падало на спецсимволах)
        client_url = user_link(u.tg_id, u.username)
        admin_text = (
            "🆕 <b>Новая запись</b>\n\n"
            f"• Клиент: <b>{h(u.full_name)}</b>\n"
            f"• Телефон: <b>{h(u.phone)}</b>\n"
            f"• TG: {h(client_url)}\n"
            f"• Услуга: <b>{h(title)}</b> — <b>{price} ₽</b>\n"
            f"• Дата/время: <b>{fmt_dt_ru(d_iso, checked)}</b>\n"
            f"• Комментарий: <b>{h(comment or '—')}</b>\n\n"
            f"ID записи: <code>{booking_id}</code>"
        )
        try:
            await context.bot.send_message(
                ADMIN_ID, admin_text, parse_mode="HTML",
                reply_markup=kb_adm_confirm_cancel(booking_id, client_url)
            )
        except Exception as e:
            log.exception("Send booking to admin failed: %s", e)
            await context.bot.send_message(
                chat_id,
                "⚠️ Запись сохранена, но уведомление мастеру не отправилось.\n"
                "Проверьте ADMIN_ID и что мастер нажал /start в боте."
            )

        clear_booking_draft(context)
        await context.bot.send_message(chat_id, "🏠 Вернуться в меню", reply_markup=main_menu_kb(uid))
        return

    # user cancel booking
    if data.startswith("UCANCEL:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, bid_s = parts[1], parts[2]
        if not must_draft(context, did):
            return
        b = store.get_booking(int(bid_s))
        if not b or b.user_id != uid:
            await show_flow_message(chat_id, context, "Запись не найдена.")
            return
        store.set_booking_status(b.id, "cancelled")
        await show_flow_message(chat_id, context, "✅ Запись отменена.")
        await context.bot.send_message(chat_id, "Меню 👇", reply_markup=main_menu_kb(uid))
        return

    # ======= ADMIN =======
    if data == "ADM_NEXT" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await show_flow_message(chat_id, context, "Ближайших записей нет 🙂")
            return
        lines = ["📌 <b>Ближайшие записи</b>", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await show_flow_message(chat_id, context, "\n".join(lines))
        return

    if data == "ADM_TODAY" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day_active(today_iso)
        if not items:
            await show_flow_message(chat_id, context, "На сегодня записей нет 🙂")
            return
        lines = [f"📅 <b>Сегодня</b> ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await show_flow_message(chat_id, context, "\n".join(lines))
        return

    if data == "ADM_7D" and is_admin(uid):
        today = now_local().date()
        day_from = today.isoformat()
        day_to = (today + timedelta(days=6)).isoformat()
        items = store.list_range_active(day_from, day_to)
        if not items:
            await show_flow_message(chat_id, context, "На ближайшие 7 дней записей нет 🙂")
            return
        lines = [f"📆 <b>Записи на 7 дней</b> ({fmt_date_ru(day_from)} — {fmt_date_ru(day_to)})", ""]
        cur = ""
        for b in items:
            if b.book_date != cur:
                cur = b.book_date
                lines.append(f"\n<b>{fmt_date_ru(cur)}</b>")
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await show_flow_message(chat_id, context, "\n".join(lines))
        return

    if data.startswith("ADM_CONFIRM:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await show_flow_message(chat_id, context, "Запись не найдена.")
            return
        store.set_booking_status(booking_id, "confirmed")

        # notify user that booking is confirmed
        try:
            await context.bot.send_message(
                b.user_id,
                "✅ <b>Запись подтверждена!</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\n"
                f"{contacts_text()}",
                parse_mode="HTML",
                reply_markup=contacts_inline(),
            )
        except Exception as e:
            log.warning("Failed to notify user about confirmation: %s", e)

        await show_flow_message(chat_id, context, f"✅ Подтверждено (ID {booking_id})")
        return

    if data.startswith("ADM_CANCEL:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await show_flow_message(chat_id, context, "Запись не найдена.")
            return
        store.set_booking_status(booking_id, "cancelled")
        try:
            await context.bot.send_message(
                b.user_id,
                "❌ <b>Запись отменена мастером.</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\n"
                "Хотите — подберём другое время 🙂",
                parse_mode="HTML"
            )
        except Exception:
            pass
        await show_flow_message(chat_id, context, f"❌ Отменено (ID {booking_id})")
        return

    # admin block/unblock
    if data == "ADM_BLOCK" and is_admin(uid):
        did = set_draft(context)
        context.user_data["adm_mode"] = "block"
        await show_flow_message(chat_id, context, "⛔ Выберите дату для блокировки:",
                                reply_markup=kb_admin_days(did, "block"))
        return

    if data == "ADM_UNBLOCK" and is_admin(uid):
        did = set_draft(context)
        context.user_data["adm_mode"] = "unblock"
        await show_flow_message(chat_id, context, "✅ Выберите дату для разблокировки:",
                                reply_markup=kb_admin_days(did, "unblock"))
        return

    p = split3("ADM_DAYB")
    if p and is_admin(uid):
        did, day_iso = p
        if not must_draft(context, did):
            return
        context.user_data["adm_day"] = day_iso
        await show_flow_message(chat_id, context, f"⛔ Блокировка: {fmt_date_ru(day_iso)}",
                                reply_markup=kb_admin_slots(did, day_iso, "block"))
        return

    p = split3("ADM_DAYU")
    if p and is_admin(uid):
        did, day_iso = p
        if not must_draft(context, did):
            return
        context.user_data["adm_day"] = day_iso
        await show_flow_message(chat_id, context, f"✅ Разблокировка: {fmt_date_ru(day_iso)}",
                                reply_markup=kb_admin_slots(did, day_iso, "unblock"))
        return

    p = split3("ADM_BSET")
    if p and is_admin(uid):
        did, hhmm4 = p
        if not must_draft(context, did):
            return
        day_iso = context.user_data.get("adm_day")
        t = dec_time(hhmm4)
        if not day_iso:
            return
        if is_slot_taken_overlap(day_iso, t):
            await show_flow_message(chat_id, context, "Слот уже занят записью, блокировать нельзя.")
            return
        store.block_slot(day_iso, t)
        await show_flow_message(chat_id, context, f"⛔ Заблокировано: {fmt_dt_ru(day_iso, t)}",
                                reply_markup=kb_admin_slots(did, day_iso, "block"))
        return

    p = split3("ADM_USET")
    if p and is_admin(uid):
        did, hhmm4 = p
        if not must_draft(context, did):
            return
        day_iso = context.user_data.get("adm_day")
        t = dec_time(hhmm4)
        if not day_iso:
            return
        store.unblock_slot(day_iso, t)
        await show_flow_message(chat_id, context, f"✅ Разблокировано: {fmt_dt_ru(day_iso, t)}",
                                reply_markup=kb_admin_slots(did, day_iso, "unblock"))
        return


# =========================
# Startup / errors / app
# =========================
async def on_startup(app: Application):
    reschedule_all_reminders(app)
    log.info("Startup complete: reminders rescheduled.")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    return app

def main():
    app = build_app()
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
