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
    BotCommand,
    InputMediaPhoto,
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

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            LIMIT ?
            """, (int(limit),)).fetchall()
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

# Галерея мастера: список file_id / URL через запятую
MASTER_GALLERY = [x.strip() for x in os.getenv("MASTER_GALLERY", "").split(",") if x.strip()]

REVIEWS_TEXT = os.getenv(
    "REVIEWS_TEXT",
    "⭐ <b>Отзывы</b>\n\n"
    "Спасибо за доверие! Если вы уже были у меня — буду рада вашему отзыву ❤️\n"
    "Можете написать сюда в чат или отправить сообщение администратору."
).strip()
REVIEWS_URL = os.getenv("REVIEWS_URL", "").strip()

WORK_START = os.getenv("WORK_START", "08:00").strip()
WORK_END = os.getenv("WORK_END", "23:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

# Чистота чата:
AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"
AUTO_DELETE_BOT_MESSAGES = os.getenv("AUTO_DELETE_BOT_MESSAGES", "1").strip() == "1"
KEEP_LAST_BOT_MESSAGES = int(os.getenv("KEEP_LAST_BOT_MESSAGES", "2").strip() or "2")

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

def clear_draft(context: ContextTypes.DEFAULT_TYPE):
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat", "draft_id"]:
        context.user_data.pop(k, None)
    context.user_data["stage"] = STAGE_NONE

# =========================
# Chat cleanup
# =========================
def remember_bot_msg(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list):
        ids = []
    ids.append(int(message_id))
    if len(ids) > 80:
        ids = ids[-80:]
    context.user_data["bot_msg_ids"] = ids

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def delete_user_input_later(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ВАЖНО: удаляем ПОСЛЕ того, как бот уже ответил
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

    # keep last N messages (plus flow)
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
# Flow message (single editable message)
# =========================
async def flow_show(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, markup=None, remove_reply: bool = False):
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

    # если не смогли отредактировать — чистим и отправляем новый flow
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

# =========================
# UI
# =========================
def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩🎨 Обо мне", "⭐ Отзывы"],
        ["📍 Контакты", "👤 Профиль"],
        ["📩 Вопрос мастеру", "❌ Отменить запись"],
        ["🏠 Меню"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Поделиться номером", request_contact=True)]],
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
        "💳 <b>Прайс</b>\n\n"
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
        f"👩🎨 <b>{h(MASTER_NAME)}</b>\n"
        f"<b>{h(MASTER_EXPERIENCE)}</b>\n\n"
        f"{h(MASTER_TEXT)}\n\n"
        "Нажмите <b>🖼 Фотогалерея</b>, чтобы посмотреть работы 👇"
    )

def kb_about() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🖼 Фотогалерея", callback_data="ABOUT_GALLERY")],
        [InlineKeyboardButton("⬅️ Назад в меню", callback_data="MENU")],
    ])

def kb_start_for_new_user() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Регистрация", callback_data="REG_START")],
        [InlineKeyboardButton("💅 Записаться", callback_data="BOOK_START")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_reviews() -> InlineKeyboardMarkup:
    rows = []
    if REVIEWS_URL:
        rows.append([InlineKeyboardButton("⭐ Оставить отзыв", url=REVIEWS_URL)])
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    rows.append([InlineKeyboardButton("⬅️ Назад в меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

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
        [InlineKeyboardButton("✅ Отправить мастеру", callback_data=f"CONFIRM:{did}")],
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
    direct_chat = f"tg://user?id={client_user_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_CONFIRM:{booking_id}"),
         InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_CANCEL:{booking_id}")],
        [InlineKeyboardButton("💬 Профиль клиента", url=client_url)],
        [InlineKeyboardButton("✍️ Написать клиенту в боте", url=direct_chat)],
    ])

def kb_cancel_list(bookings: List[Booking]) -> InlineKeyboardMarkup:
    rows = []
    for b in bookings[:12]:
        rows.append([InlineKeyboardButton(
            f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
            callback_data=f"CANCEL:{b.id}"
        )])
    rows.append([InlineKeyboardButton("⬅️ Назад в меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

# =========================
# Text builders
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
        "✅ <b>Проверьте детали</b>\n\n"
        f"• Услуга: <b>{h(title)}</b>\n"
        f"• Цена: <b>{price} ₽</b>\n"
        f"• Дата и время: <b>{fmt_dt_ru(d_iso, t)}</b>\n"
        f"• Комментарий: <b>{h(comment)}</b>\n\n"
        "Если всё верно — нажмите <b>✅ Отправить мастеру</b>."
    )

def client_booking_sent_text(name: str, d_iso: str, t: str, title: str, price: int) -> str:
    return (
        f"🎉 <b>{h(name)}</b>, заявка отправлена мастеру!\n\n"
        f"• {fmt_dt_ru(d_iso, t)}\n"
        f"• {h(title)} — <b>{price} ₽</b>\n\n"
        "⏳ Статус: <b>ожидайте подтверждения мастера</b>.\n"
        "Как только мастер подтвердит — я сразу напишу 🙂"
    )

def admin_new_booking_text(u: User, d_iso: str, t: str, title: str, price: int, comment: str) -> str:
    link = user_link(u.tg_id, u.username)
    return (
        "🆕 <b>Новая запись</b>\n\n"
        f"👤 Клиент: <b>{h(u.full_name)}</b>\n"
        f"📞 Телефон: <b>{h(u.phone)}</b>\n"
        f"🔗 Telegram: {h(link)}\n\n"
        f"💅 Услуга: <b>{h(title)}</b>\n"
        f"💳 Цена: <b>{price} ₽</b>\n"
        f"🗓 Дата/время: <b>{fmt_dt_ru(d_iso, t)}</b>\n"
        f"📝 Комментарий: <b>{h(comment or '—')}</b>\n\n"
        "Выберите действие:"
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
# Flows / Commands
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    clear_draft(context)

    intro = (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Я помогу записаться быстро и без лишних сообщений:\n"
        "1) выберите услугу\n"
        "2) выберите дату и время\n"
        "3) отправьте заявку мастеру ✅\n\n"
        "Меню снизу 👇"
    )

    await send_clean(update, context, intro, reply_markup=main_menu_kb(uid), clean_before=True)

    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "📝 Для записи нужна регистрация (один раз).", kb_start_for_new_user(), remove_reply=True)
    else:
        await flow_show(update, context, "Готово 🙂 Нажмите <b>💅 Записаться</b>.", None, remove_reply=True)

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_draft(context)
    uid = update.effective_user.id
    await send_clean(update, context, "🏠 <b>Меню</b>", reply_markup=main_menu_kb(uid), clean_before=True)
    await flow_show(update, context, "Выберите раздел 👇", None, remove_reply=True)

async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await flow_show(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None, remove_reply=True)

async def book_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Сначала регистрация 🙂", kb_start_for_new_user(), remove_reply=True)
        return
    did = set_draft(context)
    await flow_show(update, context, "💅 <b>Запись</b>\n\nВыберите категорию услуги:", kb_service_cats(did), remove_reply=True)

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await flow_show(update, context, "⛔️ Этот раздел доступен только мастеру.", None, remove_reply=True)
        return
    clear_draft(context)
    await flow_show(update, context, "🛠 <b>Админ-панель</b>", kb_admin_panel(), remove_reply=True)

async def prices_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await send_clean(update, context, prices_text(), reply_markup=main_menu_kb(uid), clean_before=True)

async def contacts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await send_clean(update, context, contacts_text(), reply_markup=main_menu_kb(uid), clean_before=True)
    if contacts_inline():
        await flow_show(update, context, "Нажмите кнопку ниже 👇", contacts_inline(), remove_reply=True)

async def about_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # показываем текст в flow + кнопки галереи/назад
    await send_clean(update, context, about_master_text(), reply_markup=main_menu_kb(uid), clean_before=True)
    await flow_show(update, context, "👇", kb_about(), remove_reply=True)

async def reviews_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await send_clean(update, context, REVIEWS_TEXT, reply_markup=main_menu_kb(uid), clean_before=True)
    await flow_show(update, context, "👇", kb_reviews(), remove_reply=True)

async def profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Профиля нет. Нажмите «📝 Регистрация».", kb_start_for_new_user(), remove_reply=True)
        return
    link = user_link(u.tg_id, u.username)
    await send_clean(
        update, context,
        "👤 <b>Профиль</b>\n\n"
        f"• Имя: <b>{h(u.full_name)}</b>\n"
        f"• Телефон: <b>{h(u.phone)}</b>\n"
        f"• Telegram: {h(link)}\n",
        reply_markup=main_menu_kb(uid),
        clean_before=True
    )

async def ask_master_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Сначала регистрация 🙂", kb_start_for_new_user(), remove_reply=True)
        return
    context.user_data["stage"] = STAGE_ASK_MASTER
    await flow_show(update, context, "📩 Напишите ваш вопрос мастеру одним сообщением:", None, remove_reply=True)

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Сначала регистрация 🙂", kb_start_for_new_user(), remove_reply=True)
        return
    bookings = store.list_user_upcoming(uid)
    if not bookings:
        await flow_show(update, context, "У вас нет активных записей 🙂", None, remove_reply=True)
        return
    await flow_show(update, context, "Выберите запись для отмены:", kb_cancel_list(bookings), remove_reply=True)

# =========================
# Callback handler
# =========================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    data = q.data or ""
    await q.answer()

    uid = update.effective_user.id

    # MENU
    if data == "MENU":
        clear_draft(context)
        await send_clean(update, context, "🏠 <b>Меню</b>", reply_markup=main_menu_kb(uid), clean_before=True)
        await flow_show(update, context, "Выберите раздел 👇", None, remove_reply=True)
        return

    # REG
    if data == "REG_START":
        await reg_start(update, context)
        return

    # ABOUT gallery
    if data == "ABOUT_GALLERY":
        if not MASTER_GALLERY:
            await flow_show(update, context, "🖼 Галерея пока не заполнена. Добавьте MASTER_GALLERY в переменные Railway.", kb_about(), remove_reply=True)
            return
        media = []
        for item in MASTER_GALLERY[:10]:
            media.append(InputMediaPhoto(media=item))
        try:
            await context.bot.send_media_group(chat_id=update.effective_chat.id, media=media)
        except Exception:
            # fallback: по одной
            for item in MASTER_GALLERY[:10]:
                try:
                    m = await context.bot.send_photo(chat_id=update.effective_chat.id, photo=item)
                    remember_bot_msg(context, m.message_id)
                except Exception:
                    pass
        await flow_show(update, context, "Готово 🙂", kb_about(), remove_reply=True)
        return

    # BOOK START
    if data == "BOOK_START":
        await book_cmd(update, context)
        return

    # NOOP
    if data == "NOOP":
        return

    # Admin panel callbacks
    if data == "ADM_NEXT":
        if not is_admin(uid):
            return
        items = store.list_next(25)
        if not items:
            await flow_show(update, context, "📌 Ближайших записей нет.", kb_admin_panel(), remove_reply=True)
            return
        lines = ["📌 <b>Ближайшие записи</b>\n"]
        for b in items:
            lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {b.price} ₽ (user {b.user_id})")
        await flow_show(update, context, "\n".join(lines), kb_admin_panel(), remove_reply=True)
        return

    if data == "ADM_TODAY":
        if not is_admin(uid):
            return
        today = now_local().date().isoformat()
        items = store.list_day(today)
        if not items:
            await flow_show(update, context, "📅 Сегодня записей нет.", kb_admin_panel(), remove_reply=True)
            return
        lines = [f"📅 <b>Сегодня ({fmt_date_ru(today)})</b>\n"]
        for b in items:
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {b.price} ₽ (user {b.user_id})")
        await flow_show(update, context, "\n".join(lines), kb_admin_panel(), remove_reply=True)
        return

    if data == "ADM_7D":
        if not is_admin(uid):
            return
        d0 = now_local().date()
        d1 = d0 + timedelta(days=7)
        items = store.list_range(d0.isoformat(), d1.isoformat())
        if not items:
            await flow_show(update, context, "📆 На ближайшие 7 дней записей нет.", kb_admin_panel(), remove_reply=True)
            return
        lines = [f"📆 <b>Записи на 7 дней</b>\n"]
        for b in items:
            lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {b.price} ₽ (user {b.user_id})")
        await flow_show(update, context, "\n".join(lines), kb_admin_panel(), remove_reply=True)
        return

    # Cancel by client
    if data.startswith("CANCEL:"):
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b or b.user_id != uid or b.status not in ("pending", "confirmed"):
            await flow_show(update, context, "Эта запись уже недоступна.", None, remove_reply=True)
            return
        store.set_booking_status(bid, "canceled")
        await flow_show(update, context, "✅ Запись отменена.", None, remove_reply=True)
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"❌ <b>Клиент отменил запись</b>\n• {fmt_dt_ru(b.book_date, b.book_time)}\n• {h(b.service_title)}\n• user_id: {b.user_id}",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    # Booking flow: CAT / SVC / DAY / TIME / MANUAL / COMMENT / CONFIRM / BACK_*
    if data.startswith("CAT:"):
        _, did, cat = data.split(":", 2)
        # обновляем draft (чтобы кнопки не умирали “после первого раза”)
        if did != get_draft(context):
            context.user_data["draft_id"] = did
        context.user_data["book_cat"] = cat
        await flow_show(update, context, "Выберите услугу:", kb_services(did, cat), remove_reply=True)
        return

    if data.startswith("BACK_CATS:"):
        _, did = data.split(":", 1)
        await flow_show(update, context, "Выберите категорию услуги:", kb_service_cats(did), remove_reply=True)
        return

    if data.startswith("SVC:"):
        _, did, key = data.split(":", 2)
        if key not in SERVICES:
            await flow_show(update, context, "⚠️ Услуга не найдена. Нажмите 💅 Записаться заново.", None, remove_reply=True)
            return
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = price
        await flow_show(update, context, "Выберите дату:", kb_days(did), remove_reply=True)
        return

    if data.startswith("BACK_SVC:"):
        _, did = data.split(":", 1)
        cat = context.user_data.get("book_cat", "mn")
        await flow_show(update, context, "Выберите услугу:", kb_services(did, cat), remove_reply=True)
        return

    if data.startswith("DAY:"):
        _, did, day_iso = data.split(":", 2)
        context.user_data["book_date"] = day_iso
        await flow_show(update, context, f"Выберите время на <b>{fmt_date_ru(day_iso)}</b>:", kb_times(did, day_iso), remove_reply=True)
        return

    if data.startswith("BACK_DAYS:"):
        _, did = data.split(":", 1)
        await flow_show(update, context, "Выберите дату:", kb_days(did), remove_reply=True)
        return

    if data.startswith("TIME:"):
        _, did, hhmm = data.split(":", 2)
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show(update, context, "⚠️ Сценарий устарел. Нажмите 💅 Записаться заново.", None, remove_reply=True)
            return
        ok, res = is_time_allowed_for_booking(day_iso, hhmm)
        if not ok:
            await flow_show(update, context, f"😕 {res}\nВыберите другое время:", kb_times(did, day_iso), remove_reply=True)
            return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show(update, context, "Добавьте комментарий (необязательно) или нажмите «Без комментария».", kb_comment(did), remove_reply=True)
        return

    if data.startswith("MANUAL_TIME:"):
        _, did = data.split(":", 1)
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await flow_show(update, context, "✍️ Введите время вручную в формате <b>HH:MM</b> (например 17:00):", None, remove_reply=True)
        return

    if data.startswith("BACK_TIMES:"):
        _, did = data.split(":", 1)
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show(update, context, "⚠️ Сценарий устарел. Нажмите 💅 Записаться заново.", None, remove_reply=True)
            return
        await flow_show(update, context, f"Выберите время на <b>{fmt_date_ru(day_iso)}</b>:", kb_times(did, day_iso), remove_reply=True)
        return

    if data.startswith("COMMENT:"):
        _, did, val = data.split(":", 2)
        if val == "-":
            context.user_data["comment"] = ""
        context.user_data["stage"] = STAGE_NONE
        await flow_show(update, context, build_confirm_text(context), kb_confirm(did), remove_reply=True)
        return

    if data.startswith("CONFIRM:"):
        _, did = data.split(":", 1)
        u = store.get_user(uid)
        if not u:
            await flow_show(update, context, "Сначала регистрация 🙂", kb_start_for_new_user(), remove_reply=True)
            return

        title = context.user_data.get("service_title")
        price = context.user_data.get("service_price")
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment") or ""

        if not title or not d_iso or not t:
            await flow_show(update, context, "⚠️ Сценарий устарел. Нажмите 💅 Записаться заново.", None, remove_reply=True)
            return

        ok, bid, msg = store.create_booking_safe(uid, context.user_data.get("service_key", ""), title, int(price), d_iso, t, comment)
        if not ok:
            await flow_show(update, context, f"😕 {h(msg)}\nВыберите другое время:", kb_times(did, d_iso), remove_reply=True)
            return

        # Клиент
        await flow_show(update, context, client_booking_sent_text(u.full_name, d_iso, t, title, int(price)), None, remove_reply=True)

        # Админ
        try:
            client_url = user_link(u.tg_id, u.username)
            admin_text = admin_new_booking_text(u, d_iso, t, title, int(price), comment)
            await context.bot.send_message(
                ADMIN_ID,
                admin_text,
                parse_mode="HTML",
                reply_markup=kb_adm_confirm_cancel(int(bid), client_url, u.tg_id),
                disable_web_page_preview=True
            )
        except Exception as e:
            log.exception("Send to admin failed: %s", e)
            await flow_show(update, context, "⚠️ Не удалось уведомить мастера. Проверь ADMIN_ID и чтобы мастер нажал /start.", None, remove_reply=True)

        # напоминание
        schedule_reminder(context.application, int(bid), d_iso, t)

        clear_draft(context)
        return

    # Admin confirm/cancel booking
    if data.startswith("ADM_CONFIRM:"):
        if not is_admin(uid):
            return
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b:
            await flow_show(update, context, "Запись не найдена.", kb_admin_panel(), remove_reply=True)
            return
        store.set_booking_status(bid, "confirmed")
        try:
            await context.bot.send_message(
                b.user_id,
                f"✅ <b>Запись подтверждена!</b>\n\n• {fmt_dt_ru(b.book_date, b.book_time)}\n• {h(b.service_title)}\n\nДо встречи 🙂",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    if data.startswith("ADM_CANCEL:"):
        if not is_admin(uid):
            return
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b:
            await flow_show(update, context, "Запись не найдена.", kb_admin_panel(), remove_reply=True)
            return
        store.set_booking_status(bid, "canceled")
        try:
            await context.bot.send_message(
                b.user_id,
                f"❌ <b>Запись отменена мастером</b>\n\n• {fmt_dt_ru(b.book_date, b.book_time)}\n• {h(b.service_title)}\n\nНапишите, если хотите подобрать другое время 🙂",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

# =========================
# Registration / Contact handler
# =========================
async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return
    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await send_clean(update, context, "Номер не распознан 😕 Можно ввести вручную или нажать кнопку ниже.", reply_markup=phone_request_kb(), clean_before=True)
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = STAGE_NONE

    await send_clean(
        update, context,
        f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅",
        reply_markup=main_menu_kb(uid),
        clean_before=True
    )
    await delete_user_input_later(update, context)

# =========================
# Text handler
# =========================
def _normalize_btn(text: str) -> str:
    t = (text or "").strip()
    t_clean = re.sub(r'[^\w\s-]', '', t.lower()).strip()

    if t_clean in ("записаться", "запись", "записатcя"):
        return "💅 Записаться"
    if t_clean in ("цены", "прайс"):
        return "💳 Цены"
    if t_clean in ("контакты", "адрес"):
        return "📍 Контакты"
    if t_clean in ("обо мне", "мастер"):
        return "👩🎨 Обо мне"
    if t_clean in ("отзывы", "review", "reviews"):
        return "⭐ Отзывы"
    if t_clean in ("профиль",):
        return "👤 Профиль"
    if t_clean in ("вопрос мастеру", "вопрос", "написать мастеру"):
        return "📩 Вопрос мастеру"
    if t_clean in ("отменить запись", "отмена"):
        return "❌ Отменить запись"
    if t_clean in ("меню", "home"):
        return "🏠 Меню"
    if t_clean in ("админ", "админ-панель", "админ панель", "админпанель"):
        return "🛠 Админ-панель"

    return t

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt_raw = (update.message.text or "").strip()
    txt = _normalize_btn(txt_raw)
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # ====== REG NAME
    if stage == STAGE_REG_NAME:
        if len(txt_raw) < 2:
            await send_clean(update, context, "Напишите имя чуть понятнее 🙂", reply_markup=main_menu_kb(uid), clean_before=True)
            return
        context.user_data["reg_name"] = txt_raw
        context.user_data["stage"] = STAGE_REG_PHONE
        await send_clean(
            update, context,
            "📞 <b>Номер телефона</b>\n\nМожно:\n• ввести номер вручную (например +79991234567)\n• или нажать кнопку ниже 👇",
            reply_markup=phone_request_kb(),
            clean_before=True
        )
        await delete_user_input_later(update, context)
        return

    # ====== REG PHONE (manual text)
    if stage == STAGE_REG_PHONE:
        phone = parse_phone(txt_raw)
        if len(re.sub(r"\D", "", phone)) < 10:
            await send_clean(update, context, "Номер не распознан 😕 Введите вручную или нажмите кнопку ниже.", reply_markup=phone_request_kb(), clean_before=True)
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await send_clean(update, context, f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅", reply_markup=main_menu_kb(uid), clean_before=True)
        await delete_user_input_later(update, context)
        return

    # ====== MANUAL TIME
    if stage == STAGE_MANUAL_TIME:
        day_iso = context.user_data.get("book_date")
        did = get_draft(context)
        if not day_iso or not did:
            clear_draft(context)
            await send_clean(update, context, "Сценарий устарел. Нажмите 💅 Записаться заново.", reply_markup=main_menu_kb(uid), clean_before=True)
            return

        ok, res = is_time_allowed_for_booking(day_iso, txt_raw)
        if not ok:
            await send_clean(update, context, f"😕 {res}\nВведите время ещё раз (например 17:00).", reply_markup=main_menu_kb(uid), clean_before=True)
            return

        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show(update, context, "Добавьте комментарий (необязательно) или нажмите «Без комментария».", kb_comment(did), remove_reply=True)
        await delete_user_input_later(update, context)
        return

    # ====== COMMENT TEXT
    if stage == STAGE_BOOK_COMMENT:
        did = get_draft(context)
        if not did:
            clear_draft(context)
            await send_clean(update, context, "Сценарий устарел. Нажмите 💅 Записаться заново.", reply_markup=main_menu_kb(uid), clean_before=True)
            return
        context.user_data["comment"] = txt_raw
        context.user_data["stage"] = STAGE_NONE
        await flow_show(update, context, build_confirm_text(context), kb_confirm(did), remove_reply=True)
        await delete_user_input_later(update, context)
        return

    # ====== ASK MASTER
    if stage == STAGE_ASK_MASTER:
        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n{h(link)}\n\n{h(txt_raw)}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
            await send_clean(update, context, "✅ Отправил мастеру. Мы ответим вам скоро 🙂", reply_markup=main_menu_kb(uid), clean_before=True)
        except Exception as e:
            log.exception("Send to admin failed: %s", e)
            await send_clean(
                update, context,
                "⚠️ Не удалось отправить мастеру.\n"
                "Проверь ADMIN_ID и что мастер нажал /start в этом боте.",
                reply_markup=main_menu_kb(uid),
                clean_before=True
            )
        context.user_data["stage"] = STAGE_NONE
        await delete_user_input_later(update, context)
        return

    # ====== MENU BUTTONS (важно: сначала действие, потом удаление)
    if txt == "🏠 Меню":
        await menu_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "💅 Записаться":
        await book_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "💳 Цены":
        await prices_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "📍 Контакты":
        await contacts_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "👩🎨 Обо мне":
        await about_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "⭐ Отзывы":
        await reviews_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "👤 Профиль":
        await profile_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "📩 Вопрос мастеру":
        await ask_master_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "❌ Отменить запись":
        await cancel_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    if txt == "🛠 Админ-панель":
        await admin_cmd(update, context)
        await delete_user_input_later(update, context)
        return

    # Если неизвестный текст — не мусорим, просто подсказка в flow
    await flow_show(update, context, "Нажмите кнопку в меню снизу 👇", None, remove_reply=True)
    await delete_user_input_later(update, context)

# =========================
# Post init
# =========================
async def post_init(app: Application):
    # команды в меню Telegram
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Запуск бота"),
            BotCommand("menu", "Меню"),
        ])
    except Exception:
        pass
    reschedule_all_reminders(app)

# =========================
# Main
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu_cmd))

    # callbacks
    app.add_handler(CallbackQueryHandler(on_callback))

    # contacts first
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))

    # text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    log.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
