# bot.py — ПОЛНЫЙ КОД В ОДНОМ ФАЙЛЕ (без storage.py)
# python-telegram-bot >= 21
# Railway: все настройки через Variables (ENV)

import os
import re
import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, List
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
# STORAGE (SQLite) — ВНУТРИ ОДНОГО ФАЙЛА
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
    book_date: str   # YYYY-MM-DD
    book_time: str   # HH:MM
    comment: str
    status: str      # pending/confirmed/cancelled
    created_at: str
    reminder_sent: int  # 0/1


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
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()  # https://t.me/username
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
    "Аккуратный маникюр/педикюр, стерильно и с любовью к деталям ✨\n"
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

# ручной ввод времени: шаг 15 минут (17:15 и т.п.)
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
    d = date.fromisoformat(iso_date)
    return d.strftime("%d.%m.%Y")


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
    if username:
        return f"https://t.me/{username}"
    return f"tg://user?id={user_id}"


def parse_hhmm(text: str) -> str | None:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", text or "")
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"


def work_bounds_for_day(day_iso: str) -> tuple[datetime, datetime]:
    d = date.fromisoformat(day_iso)
    ws = datetime.strptime(WORK_START, "%H:%M").time()
    we = datetime.strptime(WORK_END, "%H:%M").time()
    start_dt = datetime.combine(d, ws).replace(tzinfo=tz)
    end_dt = datetime.combine(d, we).replace(tzinfo=tz)
    return start_dt, end_dt


def is_time_allowed_for_booking(day_iso: str, hhmm: str) -> tuple[bool, str]:
    parsed = parse_hhmm(hhmm)
    if not parsed:
        return False, "Введите время в формате HH:MM, например 17:15."

    hh, mm = map(int, parsed.split(":"))
    if mm not in ALLOWED_MANUAL_MINUTES:
        return False, "Минуты должны быть 00, 15, 30 или 45 (например 17:15)."

    start_dt, end_dt = work_bounds_for_day(day_iso)
    slot_dt = datetime.combine(date.fromisoformat(day_iso), dtime(hh, mm)).replace(tzinfo=tz)

    if slot_dt < start_dt or slot_dt > end_dt:
        return False, f"Время должно быть в пределах {WORK_START}–{WORK_END}."

    if day_iso == now_local().date().isoformat():
        if slot_dt <= now_local():
            return False, "Это время уже прошло. Выберите время позже текущего."

    if store.is_slot_blocked(day_iso, parsed):
        return False, "Это время заблокировано. Выберите другое."
    if store.is_slot_taken(day_iso, parsed):
        return False, "Это время уже занято. Выберите другое."

    return True, parsed


def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    # Компактнее: 2 в ряд
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩‍🎨 Обо мне", "📍 Контакты"],
        ["📩 Вопрос мастеру", "❌ Отменить запись"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)


def after_reg_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💅 Записаться", callback_data="go_book")],
        [InlineKeyboardButton("💳 Цены", callback_data="go_prices")],
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
        "Записаться можно кнопкой *💅 Записаться* ✅"
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
        "",
        "Запись в пару кликов — нажмите *💅 Записаться* 🙂",
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


def contacts_inline() -> InlineKeyboardMarkup | None:
    rows = []
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if YANDEX_MAP_URL:
        rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None


# =========================================================
# CLEAN CHAT (реально чистим экраны)
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
    try:
        if update.message:
            await safe_delete(context.bot, update.effective_chat.id, update.message.message_id)
    except Exception:
        pass


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
# CALENDAR / SLOTS
# =========================================================
def month_days_from_today() -> list[str]:
    today = now_local().date()
    first_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    last_day = first_next_month - timedelta(days=1)
    days = []
    d = today
    while d <= last_day:
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def build_days_kb(prefix: str) -> InlineKeyboardMarkup:
    days = month_days_from_today()
    rows, row = [], []
    for iso in days:
        row.append(InlineKeyboardButton(fmt_date_ru(iso), callback_data=f"{prefix}:{iso}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)


def generate_time_slots(day_iso: str) -> list[str]:
    start_dt, end_dt = work_bounds_for_day(day_iso)
    slots = []
    cur = start_dt
    while cur + timedelta(minutes=SLOT_MINUTES) <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots


def build_times_kb(day_iso: str, mode: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)

    # сегодняшние прошедшие не показываем
    if mode == "client" and day_iso == now_local().date().isoformat():
        now_dt = now_local()
        filtered = []
        for hhmm in slots:
            hh, mm = map(int, hhmm.split(":"))
            slot_dt = datetime.combine(now_dt.date(), dtime(hh, mm)).replace(tzinfo=tz)
            if slot_dt > now_dt:
                filtered.append(hhmm)
        slots = filtered

    rows, row = [], []

    # админ-разблок — показываем только заблокированные
    if mode == "adm_unblock":
        blocked = set(store.list_blocked_for_day(day_iso))
        shown = [t for t in slots if t in blocked]
        if not shown:
            return InlineKeyboardMarkup([
                [InlineKeyboardButton("😕 Нет заблокированных слотов", callback_data="noop")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")],
            ])
        for t in shown:
            row.append(InlineKeyboardButton(t, callback_data=f"adm_unblock_time:{day_iso}|{t}"))
            if len(row) == 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")])
        return InlineKeyboardMarkup(rows)

    # клиент/админ-блок
    for t in slots:
        if mode == "client":
            if store.is_slot_blocked(day_iso, t) or store.is_slot_taken(day_iso, t):
                continue
            cb = f"time:{t}"
        elif mode == "adm_block":
            if store.is_slot_taken(day_iso, t):
                continue
            cb = f"adm_block_time:{day_iso}|{t}"
        else:
            cb = "noop"

        row.append(InlineKeyboardButton(t, callback_data=cb))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if mode == "client":
        rows.append([InlineKeyboardButton("✍️ Ввести время вручную", callback_data="manual_time")])

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data="noop")]]
        if mode == "client":
            rows.append([InlineKeyboardButton("✍️ Ввести время вручную", callback_data="manual_time")])

    if mode == "client":
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_days"),
                     InlineKeyboardButton("В меню", callback_data="back_to_menu")])
    else:
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")])

    return InlineKeyboardMarkup(rows)


def service_cats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Маникюр", callback_data="cat:mn")],
        [InlineKeyboardButton("🦶 Педикюр", callback_data="cat:pd")],
        [InlineKeyboardButton("🌟 Дополнительно", callback_data="cat:extra")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")],
    ])


def services_list_kb(cat: str) -> InlineKeyboardMarkup:
    if cat == "mn":
        keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd":
        keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else:
        keys = ["ext", "corr", "design"]

    rows = []
    for k in keys:
        title, price = SERVICES[k]
        rows.append([InlineKeyboardButton(f"{title} — {price} ₽", callback_data=f"svc:{k}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_service_cats")])
    rows.append([InlineKeyboardButton("В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)


def admin_panel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записи на сегодня", callback_data="adm_today"),
         InlineKeyboardButton("⏭ Ближайшие", callback_data="adm_next")],
        [InlineKeyboardButton("⛔ Заблокировать слот", callback_data="adm_block"),
         InlineKeyboardButton("✅ Разблокировать слот", callback_data="adm_unblock")],
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
        "Если планы изменились — откройте меню и нажмите *❌ Отменить запись*."
    )
    try:
        kb = contacts_inline()
        await context.bot.send_message(b.user_id, text, parse_mode="Markdown", reply_markup=kb or main_menu_kb(b.user_id))
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
# REGISTRATION + FLOW
# =========================================================
async def begin_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await send_screen(update, context, "Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data["stage"] = STAGE_NONE

    await send_screen(
        update,
        context,
        f"✨ *{SALON_NAME}*\n\n"
        "Запись на маникюр и педикюр — быстро и удобно.\n"
        "Нажмите *💅 Записаться* и выберите услугу.",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(uid),
        clean_before=True
    )

    if not store.get_user(uid):
        await begin_registration(update, context)
        return

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Хотите записаться прямо сейчас? 👇",
        reply_markup=after_reg_inline()
    )
    track_bot_msg(context, msg.message_id)


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
    await send_screen(update, context, f"✅ Отлично, *{name}*!\n\nТеперь можно записаться 👇",
                      parse_mode="Markdown", reply_markup=main_menu_kb(uid))
    msg = await context.bot.send_message(chat_id=update.effective_chat.id, text="Нажмите кнопку:", reply_markup=after_reg_inline())
    track_bot_msg(context, msg.message_id)


def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"
    if price is None:
        price = 0
    return (
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{fmt_dt_ru(d, t)}*\n"
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

    # REG NAME
    if stage == STAGE_REG_NAME:
        await delete_user_message_if_possible(update, context)
        if len(text) < 2:
            await send_screen(update, context, "Напишите имя чуть понятнее 🙂")
            return
        context.user_data["reg_name"] = text
        context.user_data["stage"] = STAGE_REG_PHONE
        await send_screen(update, context, "Отправьте номер телефона кнопкой ниже:", reply_markup=phone_request_kb())
        return

    # REG PHONE (text)
    if stage == STAGE_REG_PHONE:
        await delete_user_message_if_possible(update, context)
        phone = parse_phone(text)
        if len(re.sub(r"\D", "", phone)) < 10:
            await send_screen(update, context, "Нажмите «📱 Отправить номер» (так без ошибок).", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await send_screen(update, context, f"✅ Отлично, *{name}*!\n\nТеперь можно записаться 👇",
                          parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        msg = await context.bot.send_message(chat_id=update.effective_chat.id, text="Нажмите кнопку:", reply_markup=after_reg_inline())
        track_bot_msg(context, msg.message_id)
        return

    # MANUAL TIME
    if stage == STAGE_MANUAL_TIME:
        await delete_user_message_if_possible(update, context)
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            context.user_data["stage"] = STAGE_NONE
            await send_screen(update, context, "Сначала выберите дату 🙂", reply_markup=build_days_kb(prefix="day"))
            return

        ok, result = is_time_allowed_for_booking(day_iso, text)
        if not ok:
            await send_screen(
                update, context,
                f"😕 {result}\n\nВведите время ещё раз (например 17:15).",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")],
                    [InlineKeyboardButton("В меню", callback_data="back_to_menu")],
                ])
            )
            return

        context.user_data["book_time"] = result
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await send_screen(
            update, context,
            "Добавьте комментарий (необязательно).\n\n"
            "Например: «снятие», «укрепление», «френч», «пожелания по форме».\n\n"
            "Можно нажать «Без комментария».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Без комментария", callback_data="comment:-")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")],
            ])
        )
        return

    # ASK MASTER
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
        await send_screen(update, context, "✅ Сообщение отправлено мастеру. Мы ответим вам в ближайшее время 🙂",
                          reply_markup=main_menu_kb(uid))
        return

    # BOOK COMMENT
    if stage == STAGE_BOOK_COMMENT:
        await delete_user_message_if_possible(update, context)
        context.user_data["comment"] = "" if text == "-" else text
        context.user_data["stage"] = STAGE_NONE
        await send_screen(
            update, context,
            build_confirm_text(context),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
                 InlineKeyboardButton("В меню", callback_data="back_to_menu")]
            ])
        )
        return

    # MENU
    if text == "💅 Записаться":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        await send_screen(update, context, "Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    if text == "💳 Цены":
        await delete_user_message_if_possible(update, context)
        await send_screen(update, context, prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        return

    if text == "👩‍🎨 Обо мне":
        await delete_user_message_if_possible(update, context)
        if MASTER_PHOTO:
            await send_photo_screen(update, context, MASTER_PHOTO, about_master_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        else:
            await send_screen(update, context, about_master_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        return

    if text == "📍 Контакты":
        await delete_user_message_if_possible(update, context)
        await send_screen(update, context, contacts_text(), parse_mode="Markdown", reply_markup=contacts_inline() or main_menu_kb(uid))
        return

    if text == "📩 Вопрос мастеру":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        context.user_data["stage"] = STAGE_ASK_MASTER
        await send_screen(
            update, context,
            "Напишите ваш вопрос мастеру одним сообщением ✍️\n\n"
            "Например: «Можно ли записаться на снятие + укрепление?»",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")]])
        )
        return

    if text == "❌ Отменить запись":
        await delete_user_message_if_possible(update, context)
        if not u:
            await begin_registration(update, context)
            return
        upcoming = store.list_user_upcoming(uid)
        if not upcoming:
            await send_screen(update, context, "У вас нет активных записей 🙂", reply_markup=main_menu_kb(uid))
            return
        rows = []
        for b in upcoming:
            rows.append([InlineKeyboardButton(f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}", callback_data=f"ucancel:{b.id}")])
        rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
        await send_screen(update, context, "Выберите запись для отмены:", reply_markup=InlineKeyboardMarkup(rows))
        return

    if text == "🛠 Админ-панель" and is_admin(uid):
        await delete_user_message_if_possible(update, context)
        await send_screen(update, context, "🛠 *Админ-панель*", parse_mode="Markdown", reply_markup=admin_panel_inline())
        return

    # Остальное: не шлём админу (чтобы не спамило)
    await delete_user_message_if_possible(update, context)
    await send_screen(update, context, "Выберите действие в меню 👇", reply_markup=main_menu_kb(uid))


# =========================================================
# CALLBACKS
# =========================================================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data

    if data == "noop":
        return

    if data == "back_to_menu":
        context.user_data["stage"] = STAGE_NONE
        await send_screen(update, context, "Меню 👇", reply_markup=main_menu_kb(uid))
        return

    if data == "go_prices":
        await send_screen(update, context, prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        return

    if data == "go_book":
        if not store.get_user(uid):
            await begin_registration(update, context)
            return
        await send_screen(update, context, "Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    if data == "back_to_service_cats":
        await send_screen(update, context, "Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    if data.startswith("cat:"):
        cat = data.split(":", 1)[1]
        context.user_data["book_cat"] = cat
        await send_screen(update, context, "Выберите услугу:", reply_markup=services_list_kb(cat))
        return

    if data.startswith("svc:"):
        key = data.split(":", 1)[1]
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)
        await send_screen(
            update, context,
            f"Вы выбрали:\n*{title}* — *{price} ₽*\n\nВыберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(prefix="day")
        )
        return

    if data == "back_to_days":
        await send_screen(update, context, "Выберите дату:", reply_markup=build_days_kb(prefix="day"))
        return

    if data.startswith("day:"):
        day_iso = data.split(":", 1)[1]
        context.user_data["book_date"] = day_iso
        context.user_data["stage"] = STAGE_NONE
        await send_screen(
            update, context,
            f"Дата: *{fmt_date_ru(day_iso)}*\nВыберите время (свободные слоты):",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="client")
        )
        return

    if data == "back_to_times":
        context.user_data["stage"] = STAGE_NONE
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await send_screen(update, context, "Выберите дату заново 🙂", reply_markup=build_days_kb(prefix="day"))
            return
        await send_screen(update, context, "Выберите время:", reply_markup=build_times_kb(day_iso, mode="client"))
        return

    if data == "manual_time":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await send_screen(update, context, "Сначала выберите дату 🙂", reply_markup=build_days_kb(prefix="day"))
            return
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await send_screen(
            update, context,
            f"✍️ Введите время вручную для даты *{fmt_date_ru(day_iso)}*\n\n"
            "Формат: *HH:MM* (например 17:15)\n"
            "Минуты: 00/15/30/45\n"
            f"Работаем: {WORK_START}–{WORK_END}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")],
                [InlineKeyboardButton("В меню", callback_data="back_to_menu")],
            ])
        )
        return

    if data.startswith("time:"):
        t = data.split(":", 1)[1]
        d_iso = context.user_data.get("book_date")
        if not d_iso:
            await send_screen(update, context, "Сначала выберите дату 🙂", reply_markup=build_days_kb(prefix="day"))
            return

        ok, res = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await send_screen(update, context, f"😕 {res}", reply_markup=build_times_kb(d_iso, mode="client"))
            return

        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await send_screen(
            update, context,
            "Добавьте комментарий (необязательно).\n\n"
            "Например: «снятие», «укрепление», «френч», «пожелания по форме».\n\n"
            "Можно нажать «Без комментария».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Без комментария", callback_data="comment:-")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")],
            ])
        )
        return

    if data.startswith("comment:"):
        c = data.split(":", 1)[1]
        context.user_data["comment"] = "" if c == "-" else c
        context.user_data["stage"] = STAGE_NONE
        await send_screen(
            update, context,
            build_confirm_text(context),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
                 InlineKeyboardButton("В меню", callback_data="back_to_menu")]
            ])
        )
        return

    if data == "confirm_booking":
        u = store.get_user(uid)
        if not u:
            # аккуратно запускаем регистрацию, не "ломаем" сценарий
            await begin_registration(update, context)
            return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0) or 0)
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        if not all([key, title, d_iso, t]) or price <= 0:
            await send_screen(update, context, "Ошибка данных. Нажмите 💅 Записаться ещё раз.", reply_markup=main_menu_kb(uid))
            return

        ok, checked_time = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await send_screen(update, context, f"😕 {checked_time}\nВыберите другое время.", reply_markup=build_times_kb(d_iso, mode="client"))
            return

        booking_id = store.create_booking(u.tg_id, key, title, price, d_iso, checked_time, comment)
        schedule_reminder(context.application, booking_id, d_iso, checked_time)

        await send_screen(
            update, context,
            "✅ *Заявка отправлена администратору!*\n\n"
            f"• {fmt_dt_ru(d_iso, checked_time)}\n"
            f"• {title}\n\n"
            "Мы подтвердим запись и пришлём уведомление 🙂",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(uid)
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
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm_confirm:{booking_id}"),
             InlineKeyboardButton("❌ Отменить", callback_data=f"adm_cancel:{booking_id}")],
            [InlineKeyboardButton("💬 Написать клиенту", url=link)],
        ])
        try:
            await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown", reply_markup=admin_kb)
        except Exception:
            pass

        for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat"]:
            context.user_data.pop(k, None)
        return

    if data.startswith("ucancel:"):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b or b.user_id != uid:
            await send_screen(update, context, "Запись не найдена.", reply_markup=main_menu_kb(uid))
            return
        store.set_booking_status(booking_id, "cancelled")
        await send_screen(update, context, "✅ Запись отменена. Если нужно — запишитесь заново через меню.", reply_markup=main_menu_kb(uid))
        return

    # ---------------- ADMIN ----------------
    if data == "adm_today" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await context.bot.send_message(uid, "На сегодня записей нет 🙂")
            return
        lines = [f"📅 *Записи на сегодня* ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await context.bot.send_message(uid, "\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm_next" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await context.bot.send_message(uid, "Ближайших записей нет 🙂")
            return
        lines = ["⏭ *Ближайшие записи*", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• *{fmt_dt_ru(b.book_date, b.book_time)}* — {b.service_title} — {who} (ID `{b.id}`)")
        await context.bot.send_message(uid, "\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm_block" and is_admin(uid):
        context.user_data["adm_mode"] = "block"
        await context.bot.send_message(uid, "⛔ *Блокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                       reply_markup=build_days_kb(prefix="adm_block_day"))
        return

    if data == "adm_unblock" and is_admin(uid):
        context.user_data["adm_mode"] = "unblock"
        await context.bot.send_message(uid, "✅ *Разблокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                       reply_markup=build_days_kb(prefix="adm_unblock_day"))
        return

    if data == "adm_back_days" and is_admin(uid):
        mode = context.user_data.get("adm_mode", "block")
        if mode == "block":
            await context.bot.send_message(uid, "⛔ *Блокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                           reply_markup=build_days_kb(prefix="adm_block_day"))
        else:
            await context.bot.send_message(uid, "✅ *Разблокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                           reply_markup=build_days_kb(prefix="adm_unblock_day"))
        return

    if data.startswith("adm_block_day:") and is_admin(uid):
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "block"
        await context.bot.send_message(
            uid,
            f"⛔ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для блокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_block")
        )
        return

    if data.startswith("adm_block_time:") and is_admin(uid):
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)
        if store.is_slot_taken(day_iso, t):
            await context.bot.send_message(uid, "Этот слот уже занят записью. Нельзя заблокировать.")
            return
        store.block_slot(day_iso, t)
        await context.bot.send_message(uid, f"✅ Заблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return

    if data.startswith("adm_unblock_day:") and is_admin(uid):
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "unblock"
        await context.bot.send_message(
            uid,
            f"✅ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для разблокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_unblock")
        )
        return

    if data.startswith("adm_unblock_time:") and is_admin(uid):
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)
        store.unblock_slot(day_iso, t)
        await context.bot.send_message(uid, f"✅ Разблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return

    if data.startswith("adm_confirm:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await context.bot.send_message(uid, "Запись не найдена.")
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
                reply_markup=contacts_inline() or main_menu_kb(b.user_id)
            )
        except Exception:
            pass
        await context.bot.send_message(uid, f"✅ Подтверждено (ID {booking_id})")
        return

    if data.startswith("adm_cancel:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await context.bot.send_message(uid, "Запись не найдена.")
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
                reply_markup=contacts_inline() or main_menu_kb(b.user_id)
            )
        except Exception:
            pass
        await context.bot.send_message(uid, f"❌ Отменено (ID {booking_id})")
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
