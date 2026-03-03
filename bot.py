# bot.py
# python-telegram-bot >= 21

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
        conn = sqlite3.connect(self.path, timeout=20)
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

    def is_slot_blocked(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?",
                (book_date, book_time)
            ).fetchone()
            return row is not None

    # bookings
    def is_slot_taken_exact(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
                LIMIT 1
            """, (book_date, book_time)).fetchone()
            return row is not None

    def create_booking_safe(
        self,
        user_id: int,
        service_key: str,
        service_title: str,
        price: int,
        book_date: str,
        book_time: str,
        comment: str
    ) -> Tuple[bool, Optional[int], str]:
        """
        Атомарная вставка:
        - проверяем блок/занято
        - вставляем pending
        - защищаем от double-click (если у этого же пользователя уже есть pending/confirmed на этот слот)
        """
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")

            row = conn.execute(
                "SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=? LIMIT 1",
                (book_date, book_time)
            ).fetchone()
            if row:
                conn.rollback()
                return False, None, "Это время заблокировано."

            row = conn.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
                LIMIT 1
            """, (book_date, book_time)).fetchone()
            if row:
                conn.rollback()
                return False, None, "Это время уже занято."

            row = conn.execute("""
                SELECT id FROM bookings
                WHERE user_id=? AND book_date=? AND book_time=? AND status IN ('pending','confirmed')
                ORDER BY id DESC LIMIT 1
            """, (user_id, book_date, book_time)).fetchone()
            if row:
                conn.rollback()
                return True, int(row["id"]), "ok"

            cur = conn.execute("""
                INSERT INTO bookings(user_id, service_key, service_title, price, book_date, book_time, comment, status, created_at, reminder_sent)
                VALUES(?,?,?,?,?,?,?,?,?,0)
            """, (user_id, service_key, service_title, int(price), book_date, book_time, comment or "", "pending", now))
            bid = int(cur.lastrowid)
            conn.commit()
            return True, bid, "ok"
        except Exception as e:
            conn.rollback()
            log.exception("create_booking_safe error: %s", e)
            return False, None, "Ошибка БД при создании записи."
        finally:
            conn.close()

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
            reminder_sent=int(row["reminder_sent"]) if "reminder_sent" in row.keys() else 0,
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
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

# Чистота чата:
AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"
AUTO_DELETE_BOT_MESSAGES = os.getenv("AUTO_DELETE_BOT_MESSAGES", "1").strip() == "1"
KEEP_LAST_BOT_MESSAGES = int(os.getenv("KEEP_LAST_BOT_MESSAGES", "2").strip() or "2")  # сколько последних сообщений бота НЕ удалять (кроме flow)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")

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
def h(s: str) -> str:
    return html.escape(str(s or ""))

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
        return False, "Введите время в формате HH:MM (например 17:00)."

    start_dt, end_dt = work_bounds_for_day(day_iso)
    hh, mm = map(int, parsed.split(":"))
    slot_dt = datetime.combine(date.fromisoformat(day_iso), dtime(hh, mm)).replace(tzinfo=tz)

    if slot_dt < start_dt or slot_dt > end_dt:
        return False, f"Время должно быть в пределах {WORK_START}–{WORK_END}."
    if day_iso == now_local().date().isoformat() and slot_dt <= now_local():
        return False, "Это время уже прошло."
    if store.is_slot_blocked(day_iso, parsed):
        return False, "Это время заблокировано."
    if store.is_slot_taken_exact(day_iso, parsed):
        return False, "Это время уже занято."

    return True, parsed

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

def clear_draft(context: ContextTypes.DEFAULT_TYPE):
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat", "draft_id"]:
        context.user_data.pop(k, None)
    context.user_data["stage"] = STAGE_NONE

def remember_bot_msg(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list):
        ids = []
    ids.append(int(message_id))
    # ограничим рост списка
    if len(ids) > 50:
        ids = ids[-50:]
    context.user_data["bot_msg_ids"] = ids

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def delete_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not AUTO_DELETE_USER_INPUT or not update.message:
        return
    await safe_delete_message(context, update.effective_chat.id, update.message.message_id)

async def cleanup_bot_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int, keep_flow: bool = True):
    if not AUTO_DELETE_BOT_MESSAGES:
        return
    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list) or not ids:
        return
    flow_id = context.user_data.get("flow_msg_id")
    keep_set = set()
    if keep_flow and flow_id:
        keep_set.add(int(flow_id))

    # оставим последние KEEP_LAST_BOT_MESSAGES (кроме flow)
    tail = []
    for mid in ids[::-1]:
        if int(mid) in keep_set:
            continue
        tail.append(int(mid))
        if len(tail) >= KEEP_LAST_BOT_MESSAGES:
            break
    keep_set.update(tail)

    to_delete = [int(mid) for mid in ids if int(mid) not in keep_set]
    for mid in to_delete:
        await safe_delete_message(context, chat_id, mid)

    # пересохраним только те, что остались
    context.user_data["bot_msg_ids"] = [mid for mid in ids if int(mid) in keep_set]

async def send_clean(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    parse_mode: str = "HTML",
    reply_markup=None,
    remove_reply: bool = False,
    clean_before: bool = True
):
    chat_id = update.effective_chat.id
    if clean_before:
        await cleanup_bot_messages(context, chat_id, keep_flow=True)
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=parse_mode,
        reply_markup=ReplyKeyboardRemove() if remove_reply else reply_markup,
        disable_web_page_preview=True
    )
    remember_bot_msg(context, msg.message_id)
    return msg


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

def contacts_inline() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if YANDEX_MAP_URL:
        rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None

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

def prices_text() -> str:
    return (
        "💳 <b>Цены</b>\n\n"
        "✨ Маникюр\n"
        f"• {h(SERVICES['mn_no'][0])} — <b>{SERVICES['mn_no'][1]} ₽</b>\n"
        f"• {h(SERVICES['mn_cov'][0])} — <b>{SERVICES['mn_cov'][1]} ₽</b>\n"
        f"• {h(SERVICES['mn_cov_design'][0])} — <b>{SERVICES['mn_cov_design'][1]} ₽</b>\n\n"
        "🦶 Педикюр\n"
        f"• {h(SERVICES['pd_no'][0])} — <b>{SERVICES['pd_no'][1]} ₽</b>\n"
        f"• {h(SERVICES['pd_cov'][0])} — <b>{SERVICES['pd_cov'][1]} ₽</b>\n"
        f"• {h(SERVICES['pd_toes'][0])} — <b>{SERVICES['pd_toes'][1]} ₽</b>\n"
        f"• {h(SERVICES['pd_heels'][0])} — <b>{SERVICES['pd_heels'][1]} ₽</b>\n\n"
        "🌟 Дополнительно\n"
        f"• {h(SERVICES['ext'][0])} — <b>{SERVICES['ext'][1]} ₽</b>\n"
        f"• {h(SERVICES['corr'][0])} — <b>{SERVICES['corr'][1]} ₽</b>\n"
        f"• {h(SERVICES['design'][0])} — <b>{SERVICES['design'][1]} ₽</b>"
    )

def about_master_text() -> str:
    return (
        f"👩‍🎨 <b>{h(MASTER_NAME)}</b>\n"
        f"<b>{h(MASTER_EXPERIENCE)}</b>\n\n"
        f"{h(MASTER_TEXT)}"
    )

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
    if cat == "mn":
        keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd":
        keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else:
        keys = ["ext", "corr", "design"]

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
    while cur <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots

def kb_times(did: str, day_iso: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)
    rows, row = [], []
    for t in slots:
        ok, _ = is_time_allowed_for_booking(day_iso, t)
        if not ok:
            continue
        row.append(InlineKeyboardButton(t, callback_data=f"TIME:{did}:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data="NOOP")]]

    rows.append([InlineKeyboardButton("✍️ Ввести время вручную", callback_data=f"MANUAL_TIME:{did}")])
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

def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Ближайшие (25)", callback_data="ADM_NEXT")],
        [InlineKeyboardButton("📅 Сегодня", callback_data="ADM_TODAY"),
         InlineKeyboardButton("📆 7 дней", callback_data="ADM_7D")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_adm_confirm_cancel(booking_id: int, client_url: str, client_user_id: int) -> InlineKeyboardMarkup:
    # tg://user?id=... работает даже без username
    direct_chat = f"tg://user?id={client_user_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_CONFIRM:{booking_id}"),
         InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_CANCEL:{booking_id}")],
        [InlineKeyboardButton("💬 Написать клиенту", url=client_url)],
        [InlineKeyboardButton("✍️ Написать клиенту в боте", url=direct_chat)],
    ])


# =========================
# Flow message (1 message)
# =========================
async def flow_show(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, markup=None, remove_reply=False):
    """
    Держим один flow message:
    - если есть старый flow message -> редактируем
    - иначе отправляем новый
    """
    chat_id = update.effective_chat.id
    flow_msg_id = context.user_data.get("flow_msg_id")

    if flow_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(flow_msg_id),
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True
            )
            return
        except Exception:
            pass

    # перед отправкой подчистим старый мусор бота
    await cleanup_bot_messages(context, chat_id, keep_flow=False)

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove() if remove_reply else markup,
        disable_web_page_preview=True
    )
    context.user_data["flow_msg_id"] = msg.message_id
    remember_bot_msg(context, msg.message_id)

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
        "Нажмите <b>✅ Подтвердить запись</b> — и заявка уйдёт мастеру."
    )


# =========================
# Reminders
# =========================
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
    if app.job_queue is None:
        return
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
    if app.job_queue is None:
        log.warning("JobQueue is None. Reminders disabled.")
        return
    now = now_local()
    for b in store.list_for_reminders():
        start_dt = booking_start_dt(b.book_date, b.book_time)
        if start_dt > now:
            schedule_reminder(app, b.id, b.book_date, b.book_time)


# =========================
# Flows
# =========================
async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await flow_show(update, context, "📝 Регистрация (только 1 раз)\n\nКак к вам обращаться? (имя)", remove_reply=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    clear_draft(context)

    intro = (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Я помогу записаться в пару кликов:\n"
        "1) выбрать услугу\n"
        "2) выбрать дату и время\n"
        "3) подтвердить ✅\n\n"
        "Меню снизу 👇"
    )

    await cleanup_bot_messages(context, update.effective_chat.id, keep_flow=True)
    await send_clean(update, context, intro, reply_markup=main_menu_kb(uid), clean_before=False)

    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Чтобы записаться, нужна регистрация (1 раз).", kb_start_for_new_user())
    else:
        await flow_show(update, context, "Можно сразу нажать <b>💅 Записаться</b> 🙂", None)

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_draft(context)
    await cleanup_bot_messages(context, update.effective_chat.id, keep_flow=True)
    await send_clean(update, context, "🏠 Меню 👇", reply_markup=main_menu_kb(update.effective_user.id))

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return

    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await send_clean(update, context, "Номер не распознан 😕 Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = STAGE_NONE

    await send_clean(
        update, context,
        f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅",
        reply_markup=main_menu_kb(uid)
    )
    await delete_user_input(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt = (update.message.text or "").strip()
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # REG name
    if stage == STAGE_REG_NAME:
        if len(txt) < 2:
            await send_clean(update, context, "Напишите имя чуть понятнее 🙂")
            return
        context.user_data["reg_name"] = txt
        context.user_data["stage"] = STAGE_REG_PHONE
        await send_clean(update, context, "Отправьте номер кнопкой ниже:", reply_markup=phone_request_kb())
        await delete_user_input(update, context)
        return

    # REG phone by text
    if stage == STAGE_REG_PHONE:
        phone = parse_phone(txt)
        if len(re.sub(r"\D", "", phone)) < 10:
            await send_clean(update, context, "Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await send_clean(
            update, context,
            f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅",
            reply_markup=main_menu_kb(uid)
        )
        await delete_user_input(update, context)
        return

    # manual time
    if stage == STAGE_MANUAL_TIME:
        did = get_draft(context)
        day_iso = context.user_data.get("book_date")
        if not did or not day_iso:
            clear_draft(context)
            await send_clean(update, context, "Сценарий устарел. Нажмите 💅 Записаться заново.", reply_markup=main_menu_kb(uid))
            return

        ok, res = is_time_allowed_for_booking(day_iso, txt)
        if not ok:
            await send_clean(update, context, f"😕 {res}\nВведите время ещё раз (например 17:00).")
            return

        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show(update, context, "Добавьте комментарий (необязательно) или нажмите «Без комментария».", kb_comment(did))
        await delete_user_input(update, context)
        return

    # comment as text
    if stage == STAGE_BOOK_COMMENT:
        did = get_draft(context)
        if not did:
            clear_draft(context)
            await send_clean(update, context, "Сценарий устарел. Нажмите 💅 Записаться заново.", reply_markup=main_menu_kb(uid))
            return
        context.user_data["comment"] = txt
        context.user_data["stage"] = STAGE_NONE
        await flow_show(update, context, build_confirm_text(context), kb_confirm(did))
        await delete_user_input(update, context)
        return

    # ask master
    if stage == STAGE_ASK_MASTER:
        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n{h(link)}\n\n{h(txt)}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
            await send_clean(update, context, "✅ Отправил мастеру. Мы ответим вам скоро 🙂", reply_markup=main_menu_kb(uid))
        except Exception as e:
            log.exception("Send to admin failed: %s", e)
            await send_clean(
                update, context,
                "⚠️ Не удалось отправить мастеру.\n"
                "Проверьте ADMIN_ID и что мастер нажал /start в этом боте.",
                reply_markup=main_menu_kb(uid)
            )
        context.user_data["stage"] = STAGE_NONE
        await delete_user_input(update, context)
        return

    # меню
    if txt in ("🏠 Меню",):
        clear_draft(context)
        await send_clean(update, context, "Меню 👇", reply_markup=main_menu_kb(uid))
        await delete_user_input(update, context)
        return

    if txt == "💅 Записаться":
        if not u:
            await flow_show(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
            await delete_user_input(update, context)
            return
        did = set_draft(context)
        await flow_show(update, context, "Выберите категорию услуги:", kb_service_cats(did), remove_reply=True)
        await delete_user_input(update, context)
        return

    if txt == "💳 Цены":
        await send_clean(update, context, prices_text(), reply_markup=main_menu_kb(uid))
        await delete_user_input(update, context)
        return

    if txt == "👩‍🎨 Обо мне":
        await cleanup_bot_messages(context, update.effective_chat.id, keep_flow=True)
        if MASTER_PHOTO:
            msg = await update.message.reply_photo(MASTER_PHOTO, caption=about_master_text(), parse_mode="HTML",
                                                  reply_markup=main_menu_kb(uid))
            remember_bot_msg(context, msg.message_id)
        else:
            await send_clean(update, context, about_master_text(), reply_markup=main_menu_kb(uid))
        await delete_user_input(update, context)
        return

    if txt == "📍 Контакты":
        await send_clean(update, context, contacts_text(), reply_markup=contacts_inline() or main_menu_kb(uid))
        await delete_user_input(update, context)
        return

    if txt == "👤 Профиль":
        if not u:
            await flow_show(update, context, "Профиля нет. Нажмите «📝 Регистрация».", kb_start_for_new_user())
            await delete_user_input(update, context)
            return
        link = user_link(u.tg_id, u.username)
        await send_clean(
            update, context,
            "👤 <b>Профиль</b>\n\n"
            f"• Имя: <b>{h(u.full_name)}</b>\n"
            f"• Телефон: <b>{h(u.phone)}</b>\n"
            f"• Telegram: {h(link)}\n\n"
            "Чтобы сбросить профиль — напишите: <code>Сброс профиля</code>",
            reply_markup=main_menu_kb(uid)
        )
        await delete_user_input(update, context)
        return

    if txt.lower() in ["сброс профиля", "сброс", "сбросить профиль"]:
        store.delete_user(uid)
        await send_clean(update, context, "✅ Профиль сброшен. Нажмите /start.", reply_markup=main_menu_kb(uid))
        await delete_user_input(update, context)
        return

    if txt == "📩 Вопрос мастеру":
        if not u:
            await flow_show(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
            await delete_user_input(update, context)
            return
        context.user_data["stage"] = STAGE_ASK_MASTER
        await send_clean(update, context, "Напишите ваш вопрос мастеру одним сообщением ✍️", remove_reply=True)
        await delete_user_input(update, context)
        return

    if txt == "❌ Отменить запись":
        if not u:
            await flow_show(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
            await delete_user_input(update, context)
            return
        items = store.list_user_upcoming(uid)
        if not items:
            await send_clean(update, context, "У вас нет активных записей 🙂", reply_markup=main_menu_kb(uid))
            await delete_user_input(update, context)
            return
        did = set_draft(context)
        rows = []
        for b in items:
            rows.append([InlineKeyboardButton(
                f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
                callback_data=f"UCANCEL:{did}:{b.id}"
            )])
        rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
        await flow_show(update, context, "Выберите запись для отмены:", InlineKeyboardMarkup(rows), remove_reply=True)
        await delete_user_input(update, context)
        return

    if txt == "🛠 Админ-панель" and is_admin(uid):
        await flow_show(update, context, "🛠 Админ-панель", kb_admin_panel(), remove_reply=True)
        await delete_user_input(update, context)
        return

    await send_clean(update, context, "Выберите действие в меню 👇", reply_markup=main_menu_kb(uid))
    await delete_user_input(update, context)


# =========================
# Callbacks
# =========================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data or ""

    def split3(prefix: str) -> Optional[Tuple[str, str]]:
        if not data.startswith(prefix + ":"):
            return None
        arr = data.split(":", 2)
        if len(arr) != 3:
            return None
        return arr[1], arr[2]

    # universal
    if data == "MENU":
        clear_draft(context)
        # убираем инлайн кнопки у сообщения, откуда нажали
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await send_clean(update, context, "🏠 Меню 👇", reply_markup=main_menu_kb(uid))
        return

    if data == "NOOP":
        return

    # registration buttons
    if data == "REG_START":
        await reg_start(update, context)
        return

    if data == "BOOK_START":
        u = store.get_user(uid)
        if not u:
            await flow_show(update, context, "Для записи нужна регистрация (1 раз).\nСейчас спрошу имя и телефон 🙂")
            await reg_start(update, context)
            return
        did = set_draft(context)
        await flow_show(update, context, "Выберите категорию услуги:", kb_service_cats(did), remove_reply=True)
        return

    # back buttons
    if data.startswith("BACK_CATS:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        await flow_show(update, context, "Выберите категорию услуги:", kb_service_cats(did))
        return

    if data.startswith("BACK_SVC:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        cat = context.user_data.get("book_cat")
        if not cat:
            await flow_show(update, context, "Выберите категорию услуги:", kb_service_cats(did))
            return
        await flow_show(update, context, "Выберите услугу:", kb_services(did, cat))
        return

    if data.startswith("BACK_DAYS:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        await flow_show(update, context, "Выберите дату:", kb_days(did))
        return

    if data.startswith("BACK_TIMES:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show(update, context, "Выберите дату:", kb_days(did))
            return
        await flow_show(update, context, "Выберите время:", kb_times(did, day_iso))
        return

    # category
    p = split3("CAT")
    if p:
        did, cat = p
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        context.user_data["book_cat"] = cat
        await flow_show(update, context, "Выберите услугу:", kb_services(did, cat))
        return

    # service
    p = split3("SVC")
    if p:
        did, key = p
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
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
        await flow_show(
            update, context,
            f"Вы выбрали:\n<b>{h(title)}</b> — <b>{price} ₽</b>\n\nВыберите дату:",
            kb_days(did)
        )
        return

    # day
    p = split3("DAY")
    if p:
        did, day_iso = p
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        context.user_data["book_date"] = day_iso
        context.user_data.pop("book_time", None)
        await flow_show(
            update, context,
            f"Дата: <b>{fmt_date_ru(day_iso)}</b>\nВыберите время:",
            kb_times(did, day_iso)
        )
        return

    # time
    p = split3("TIME")
    if p:
        did, hhmm = p
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show(update, context, "Выберите дату:", kb_days(did))
            return
        ok, res = is_time_allowed_for_booking(day_iso, hhmm)
        if not ok:
            await flow_show(update, context, f"😕 {res}\nВыберите другое время:", kb_times(did, day_iso))
            return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show(update, context, "Добавьте комментарий (необязательно) или нажмите «Без комментария».", kb_comment(did))
        return

    # manual time
    if data.startswith("MANUAL_TIME:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        if not context.user_data.get("book_date"):
            await flow_show(update, context, "Сначала выберите дату:", kb_days(did))
            return
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await flow_show(update, context, "Введите время вручную (например 17:00).")
        return

    # comment
    if data.startswith("COMMENT:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, payload = parts[1], parts[2]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return
        context.user_data["comment"] = "" if payload == "-" else payload
        context.user_data["stage"] = STAGE_NONE
        await flow_show(update, context, build_confirm_text(context), kb_confirm(did))
        return

    # confirm booking by client
    if data.startswith("CONFIRM:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂")
            return

        # СРАЗУ отключаем кнопки у сообщения, чтобы не было повторных кликов и “устарели”
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        u = store.get_user(uid)
        if not u:
            await flow_show(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
            return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0) or 0)
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        if not (key and title and d_iso and t and price > 0):
            await flow_show(update, context, "⚠️ Сценарий устарел. Нажмите 💅 Записаться заново 🙂")
            return

        ok, checked = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await flow_show(update, context, f"😕 {checked}\nВыберите другое время:", kb_times(did, d_iso))
            return

        await flow_show(update, context, "⏳ Создаю заявку...")

        done, booking_id, msg = store.create_booking_safe(
            user_id=u.tg_id,
            service_key=key,
            service_title=title,
            price=price,
            book_date=d_iso,
            book_time=checked,
            comment=comment
        )
        if not done or not booking_id:
            await flow_show(update, context, f"😕 {h(msg)}\nПопробуйте выбрать другое время.", kb_times(did, d_iso))
            return

        try:
            schedule_reminder(context.application, booking_id, d_iso, checked)
        except Exception as e:
            log.warning("schedule_reminder failed: %s", e)

        # ✅ Главное: текст клиенту после подтверждения
        await flow_show(
            update, context,
            "✅ <b>Заявка отправлена мастеру!</b>\n\n"
            f"• {fmt_dt_ru(d_iso, checked)}\n"
            f"• {h(title)}\n\n"
            "⏳ <b>Ожидайте подтверждения от мастера.</b>\n"
            "Как только мастер подтвердит — я вам напишу 🙂"
        )

        # notify admin
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
                ADMIN_ID,
                admin_text,
                parse_mode="HTML",
                reply_markup=kb_adm_confirm_cancel(booking_id, client_url, u.tg_id),
                disable_web_page_preview=True
            )
        except Exception as e:
            log.exception("Send booking to admin failed: %s", e)
            await send_clean(
                update, context,
                "⚠️ Заявка сохранена, но мастеру не отправилось уведомление.\n"
                "Проверьте ADMIN_ID и что мастер нажал /start в этом боте.",
                reply_markup=main_menu_kb(uid)
            )

        clear_draft(context)
        await send_clean(update, context, "🏠 Меню 👇", reply_markup=main_menu_kb(uid))
        return

    # cancel by user
    if data.startswith("UCANCEL:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, bid_s = parts[1], parts[2]
        if not must_draft(context, did):
            await flow_show(update, context, "Кнопки устарели. Откройте «❌ Отменить запись» заново 🙂")
            return
        b = store.get_booking(int(bid_s))
        if not b or b.user_id != uid:
            await flow_show(update, context, "Запись не найдена.")
            return
        store.set_booking_status(b.id, "cancelled")
        await flow_show(update, context, "✅ Запись отменена.")
        await send_clean(update, context, "Меню 👇", reply_markup=main_menu_kb(uid))
        return

    # admin panel lists
    if data == "ADM_NEXT" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await flow_show(update, context, "Ближайших записей нет 🙂")
            return
        lines = ["📌 <b>Ближайшие записи</b>", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await flow_show(update, context, "\n".join(lines))
        return

    if data == "ADM_TODAY" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await flow_show(update, context, "На сегодня записей нет 🙂")
            return
        lines = [f"📅 <b>Сегодня</b> ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await flow_show(update, context, "\n".join(lines))
        return

    if data == "ADM_7D" and is_admin(uid):
        today = now_local().date()
        day_from = today.isoformat()
        day_to = (today + timedelta(days=6)).isoformat()
        items = store.list_range(day_from, day_to)
        if not items:
            await flow_show(update, context, "На ближайшие 7 дней записей нет 🙂")
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
        await flow_show(update, context, "\n".join(lines))
        return

    # admin confirm/cancel
    if data.startswith("ADM_CONFIRM:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "confirmed")
        try:
            await context.bot.send_message(
                b.user_id,
                "✅ <b>Запись подтверждена!</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\n"
                f"{contacts_text()}",
                parse_mode="HTML",
                reply_markup=contacts_inline(),
                disable_web_page_preview=True
            )
        except Exception as e:
            log.warning("Send confirm to user failed: %s", e)

        await q.message.reply_text(f"✅ Подтверждено (ID {booking_id})")
        return

    if data.startswith("ADM_CANCEL:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "cancelled")
        try:
            await context.bot.send_message(
                b.user_id,
                "❌ <b>Запись отменена мастером.</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\n"
                "Хотите — подберём другое время 🙂",
                parse_mode="HTML",
                disable_web_page_preview=True
            )
        except Exception:
            pass
        await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
        return


# =========================
# Startup / error / app
# =========================
async def on_startup(app: Application):
    try:
        reschedule_all_reminders(app)
    except Exception as e:
        log.warning("Startup reminders failed: %s", e)
    log.info("Startup complete")

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
