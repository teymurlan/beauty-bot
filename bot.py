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

# -------------------------
# Logging
# -------------------------
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

    def is_slot_blocked(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?",
                            (book_date, book_time)).fetchone()
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
        """Атомарно создаём запись и защищаем от гонок."""
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")

            if conn.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=? LIMIT 1",
                            (book_date, book_time)).fetchone():
                conn.rollback()
                return False, None, "Это время заблокировано."

            if conn.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
                LIMIT 1
            """, (book_date, book_time)).fetchone():
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
    "Подберу форму и покрытие под ваш стиль — чтобы носилось красиво и долго."
).strip()

# фото: file_id или URL
LOGO_PHOTO = os.getenv("LOGO_PHOTO", "").strip()
MASTER_PHOTO = os.getenv("MASTER_PHOTO", "").strip()
WORKS_PHOTOS = [x.strip() for x in os.getenv("WORKS_PHOTOS", "").split(",") if x.strip()]

WORK_START = os.getenv("WORK_START", "08:00").strip()
WORK_END = os.getenv("WORK_END", "23:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")

tz = ZoneInfo(TZ_NAME)
store = Storage(DB_PATH)


# =========================
# Services + FAQ
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

FAQ = [
    ("Сколько держится покрытие?", "Обычно 2–4 недели — зависит от ногтей и нагрузки."),
    ("Стерилизация инструмента есть?", "Да. Полная обработка + стерилизация."),
    ("Сколько по времени маникюр с покрытием?", "В среднем 1,5–2 часа, с дизайном — дольше."),
    ("Можно прийти с дизайном из Pinterest?", "Да 🙂 Пришлите фото — подберём вариант."),
    ("Можно записаться на сегодня?", "Если есть свободные слоты — бот покажет доступное время."),
    ("Делаете укрепление?", "Да, подбираем материал под ваши ногти."),
    ("Если сломался ноготь?", "Напишите — подскажу ремонт и ближайшее окошко."),
    ("Нужно что-то брать с собой?", "Нет 🙂 Всё есть на месте."),
    ("Можно перенести запись?", "Да. Отмените в профиле и запишитесь заново."),
    ("Как оплатить?", "Наличными или переводом — можно указать в комментарии при записи."),
]

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


# =========================
# Clean chat: keep only 2 bot messages
# =========================
async def safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=int(message_id))
    except Exception:
        pass

def remember_bot_msg(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list):
        ids = []
    ids.append(int(message_id))
    if len(ids) > 200:
        ids = ids[-200:]
    context.user_data["bot_msg_ids"] = ids

async def ensure_only_two(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    keep = set()
    if context.user_data.get("menu_msg_id"):
        keep.add(int(context.user_data["menu_msg_id"]))
    if context.user_data.get("flow_msg_id"):
        keep.add(int(context.user_data["flow_msg_id"]))

    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list):
        ids = []

    for mid in ids:
        if int(mid) not in keep:
            await safe_delete(context, chat_id, mid)

    context.user_data["bot_msg_ids"] = [mid for mid in ids if int(mid) in keep]

async def delete_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not AUTO_DELETE_USER_INPUT or not update.message:
        return
    await safe_delete(context, update.effective_chat.id, update.message.message_id)

async def show_menu_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup):
    chat_id = update.effective_chat.id
    old = context.user_data.get("menu_msg_id")
    if old:
        await safe_delete(context, chat_id, old)
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )
    context.user_data["menu_msg_id"] = msg.message_id
    remember_bot_msg(context, msg.message_id)
    await ensure_only_two(context, chat_id)

async def flow_show_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, markup=None):
    chat_id = update.effective_chat.id
    flow_id = context.user_data.get("flow_msg_id")
    flow_type = context.user_data.get("flow_type", "text")

    if flow_id and flow_type == "text":
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(flow_id),
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
            )
            await ensure_only_two(context, chat_id)
            return
        except Exception:
            pass

    if flow_id:
        await safe_delete(context, chat_id, flow_id)

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=markup,
        disable_web_page_preview=True,
    )
    context.user_data["flow_msg_id"] = msg.message_id
    context.user_data["flow_type"] = "text"
    remember_bot_msg(context, msg.message_id)
    await ensure_only_two(context, chat_id)

async def flow_show_photo(update: Update, context: ContextTypes.DEFAULT_TYPE, photo: str, caption: str, markup=None):
    chat_id = update.effective_chat.id
    flow_id = context.user_data.get("flow_msg_id")
    flow_type = context.user_data.get("flow_type", "text")

    if flow_id and flow_type == "photo":
        try:
            await context.bot.edit_message_media(
                chat_id=chat_id,
                message_id=int(flow_id),
                media=InputMediaPhoto(media=photo, caption=caption, parse_mode="HTML"),
                reply_markup=markup,
            )
            await ensure_only_two(context, chat_id)
            return
        except Exception:
            pass

    if flow_id:
        await safe_delete(context, chat_id, flow_id)

    msg = await context.bot.send_photo(
        chat_id=chat_id,
        photo=photo,
        caption=caption,
        parse_mode="HTML",
        reply_markup=markup,
    )
    context.user_data["flow_msg_id"] = msg.message_id
    context.user_data["flow_type"] = "photo"
    remember_bot_msg(context, msg.message_id)
    await ensure_only_two(context, chat_id)


# =========================
# UI (Reply)
# =========================
def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩‍🎨 Обо мне", "📍 Контакты"],
        ["👤 Профиль", "📩 Вопрос мастеру"],
        ["❓ Вопросы и ответы", "🏠 Меню"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def profile_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["❌ Отменить запись"],
        ["🏠 Меню"],
    ]
    if is_admin(user_id):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def about_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["📸 Галерея работ", "⬅️ Назад в меню"]], resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# =========================
# UI (Texts / Inline)
# =========================
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
    def line(k: str) -> str:
        t, p = SERVICES[k]
        return f"• {h(t)} — <b>{p} ₽</b>"
    return (
        "💳 <b>Прайс-лист</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "✨ <b>Маникюр</b>\n"
        f"{line('mn_no')}\n{line('mn_cov')}\n{line('mn_cov_design')}\n\n"
        "🦶 <b>Педикюр</b>\n"
        f"{line('pd_no')}\n{line('pd_cov')}\n{line('pd_toes')}\n{line('pd_heels')}\n\n"
        "🌟 <b>Дополнительно</b>\n"
        f"{line('ext')}\n{line('corr')}\n{line('design')}\n\n"
        "ℹ️ Точная стоимость зависит от длины/сложности. Можно уточнить в комментарии при записи 🙂"
    )

def about_text() -> str:
    return (
        f"👩‍🎨 <b>{h(MASTER_NAME)}</b>\n"
        f"🏆 <b>{h(MASTER_EXPERIENCE)}</b>\n\n"
        "✨ <b>Почему выбирают меня</b>\n"
        "• аккуратная техника и ровное покрытие\n"
        "• стерильность и чистота инструментов\n"
        "• помогу с подбором формы и оттенка\n\n"
        f"{h(MASTER_TEXT)}\n\n"
        "📸 Нажмите <b>«Галерея работ»</b>, чтобы посмотреть примеры."
    )

def faq_text() -> str:
    lines = ["❓ <b>Вопросы и ответы</b>\n"]
    for i, (q, a) in enumerate(FAQ, 1):
        lines.append(f"<b>{i}. {h(q)}</b>\n{h(a)}\n")
    lines.append("Если остались вопросы — нажмите <b>📩 Вопрос мастеру</b> 🙂")
    return "\n".join(lines)

def kb_about_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📸 Галерея работ", callback_data="GALLERY:0")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="MENU")],
    ])

def kb_ask_cancel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Отменить вопрос", callback_data="ASK_CANCEL")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])


# =========================
# Booking keyboards
# =========================
def kb_start_for_new_user() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Регистрация", callback_data="REG_START")],
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
        if ok:
            row.append(InlineKeyboardButton(f"✅ {t}", callback_data=f"TIME:{did}:{t}"))
        else:
            row.append(InlineKeyboardButton(f"⛔ {t}", callback_data="NOOP"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

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


# =========================
# Admin panel
# =========================
def kb_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Ближайшие (25)", callback_data="ADM_NEXT")],
        [InlineKeyboardButton("📅 Сегодня", callback_data="ADM_TODAY"),
         InlineKeyboardButton("📆 7 дней", callback_data="ADM_7D")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_adm_confirm_cancel(booking_id: int, client_user_id: int, client_url: str) -> InlineKeyboardMarkup:
    direct_chat = f"tg://user?id={client_user_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_CONFIRM:{booking_id}"),
         InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_CANCEL:{booking_id}")],
        [InlineKeyboardButton("💬 Написать клиенту", url=client_url)],
        [InlineKeyboardButton("✍️ Написать клиенту в боте", url=direct_chat)],
    ])


# =========================
# Cards
# =========================
def booking_card_for_admin(u: User, title: str, price: int, d_iso: str, hhmm: str, comment: str, booking_id: int) -> str:
    tg = user_link(u.tg_id, u.username)
    return (
        "🆕 <b>Новая запись</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        f"👤 <b>{h(u.full_name)}</b>\n"
        f"📞 <b>{h(u.phone)}</b>\n"
        f"🔗 {h(tg)}\n\n"
        f"🧾 <b>{h(title)}</b>\n"
        f"💰 <b>{price} ₽</b>\n"
        f"🗓 <b>{fmt_dt_ru(d_iso, hhmm)}</b>\n"
        f"📝 <b>{h(comment or '—')}</b>\n\n"
        f"🆔 ID: <code>{booking_id}</code>"
    )

def booking_card_user_sent(title: str, d_iso: str, hhmm: str) -> str:
    return (
        "✅ <b>Заявка отправлена мастеру!</b>\n\n"
        f"🗓 {fmt_dt_ru(d_iso, hhmm)}\n"
        f"🧾 {h(title)}\n\n"
        "⏳ <b>Ожидайте подтверждения от мастера.</b>\n"
        "Как только подтвердит — я вам напишу 🙂"
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
        f"🗓 {fmt_dt_ru(b.book_date, b.book_time)}\n"
        f"🧾 {h(b.service_title)}\n\n"
        f"{contacts_text()}\n"
        "Если планы изменились — зайдите в <b>👤 Профиль</b> → <b>❌ Отменить запись</b>."
    )
    try:
        await context.bot.send_message(b.user_id, text, parse_mode="HTML", reply_markup=contacts_inline())
        store.mark_reminder_sent(b.id)
    except Exception:
        pass

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
        return
    now = now_local()
    for b in store.list_for_reminders():
        start_dt = booking_start_dt(b.book_date, b.book_time)
        if start_dt > now:
            schedule_reminder(app, b.id, b.book_date, b.book_time)


# =========================
# Normalization for reply buttons (важно для “не реагирует”)
# =========================
def normalize_button_text(s: str) -> str:
    s = (s or "").strip()
    # убираем variation selector (часто ломает сравнения на iOS/Android)
    s = s.replace("\ufe0f", "")
    # схлопываем пробелы
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_commands(s: str) -> str:
    t = (s or "").strip().lower()
    if t in ("/book", "/записаться"):
        return "💅 Записаться"
    if t in ("/admin",):
        return "🛠 Админ-панель"
    if t in ("/menu",):
        return "🏠 Меню"
    if t in ("/prices",):
        return "💳 Цены"
    if t in ("/faq",):
        return "❓ Вопросы и ответы"
    if t in ("/profile",):
        return "👤 Профиль"
    if t in ("/about",):
        return "👩‍🎨 Обо мне"
    return s


# =========================
# Commands
# =========================
async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_draft(context)
    context.user_data["stage"] = STAGE_NONE
    await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(update.effective_user.id))
    await flow_show_text(update, context, "✅ Отменено. Выберите действие в меню 👇")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    clear_draft(context)
    context.user_data["stage"] = STAGE_NONE

    await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(uid))

    u = store.get_user(uid)
    intro = (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Я помогу записаться в пару кликов:\n"
        "1) выбрать услугу\n"
        "2) выбрать дату и время\n"
        "3) подтвердить ✅\n\n"
        "Отмена любого действия: /cancel"
    )

    if LOGO_PHOTO:
        await flow_show_photo(update, context, LOGO_PHOTO, intro, kb_start_for_new_user() if not u else None)
    else:
        await flow_show_text(update, context, intro, kb_start_for_new_user() if not u else None)

    if u:
        await flow_show_text(update, context, "Можно сразу нажать <b>💅 Записаться</b> 🙂")

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_draft(context)
    context.user_data["stage"] = STAGE_NONE
    await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(update.effective_user.id))
    await flow_show_text(update, context, "Выберите действие в меню 👇")

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    clear_draft(context)
    context.user_data["stage"] = STAGE_NONE
    await show_menu_message(update, context, "🛠 Админ-панель", main_menu_kb(uid))
    await flow_show_text(update, context, "🛠 <b>Админ-панель</b>", kb_admin_panel())

async def cmd_prices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await flow_show_text(update, context, prices_text())

async def cmd_faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await flow_show_text(update, context, faq_text())

async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_profile(update, context)

async def cmd_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_booking(update, context)


# =========================
# Flows
# =========================
async def start_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    # убираем клавиатуру на время регистрации
    await show_menu_message(update, context, "📝 Регистрация", ReplyKeyboardRemove())
    await flow_show_text(
        update, context,
        "📝 <b>Регистрация (1 раз)</b>\n\n"
        "Введите <b>имя</b> (фамилия — по желанию).\n"
        "Пример: <code>Анна</code> или <code>Анна Иванова</code>\n\n"
        "Отмена: /cancel"
    )

async def start_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show_text(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
        return
    did = set_draft(context)
    await flow_show_text(update, context, "Выберите категорию услуги:", kb_service_cats(did))

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show_text(update, context, "Профиля нет. Нажмите «📝 Регистрация».", kb_start_for_new_user())
        return
    link = user_link(u.tg_id, u.username)
    await show_menu_message(update, context, "👤 Профиль", profile_kb(uid))
    await flow_show_text(
        update, context,
        "👤 <b>Профиль</b>\n\n"
        f"👤 Имя: <b>{h(u.full_name)}</b>\n"
        f"📞 Телефон: <b>{h(u.phone)}</b>\n"
        f"🔗 Telegram: {h(link)}\n\n"
        "Чтобы отменить запись — нажмите <b>❌ Отменить запись</b>."
    )

async def cancel_booking_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show_text(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
        return

    items = store.list_user_upcoming(uid)
    if not items:
        await flow_show_text(update, context, "У вас нет активных записей 🙂")
        return

    did = set_draft(context)
    rows = []
    for b in items:
        rows.append([InlineKeyboardButton(
            f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
            callback_data=f"UCANCEL:{did}:{b.id}"
        )])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    await flow_show_text(update, context, "Выберите запись для отмены:", InlineKeyboardMarkup(rows))

async def show_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # В ЭТОМ разделе: отдельные reply кнопки + inline
    await show_menu_message(update, context, "👩‍🎨 Обо мне", about_reply_kb())

    if MASTER_PHOTO:
        await flow_show_photo(update, context, MASTER_PHOTO, about_text(), kb_about_inline())
    else:
        await flow_show_text(update, context, about_text(), kb_about_inline())

async def start_ask_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show_text(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user())
        return
    context.user_data["stage"] = STAGE_ASK_MASTER
    await flow_show_text(
        update, context,
        "📩 <b>Вопрос мастеру</b>\n\n"
        "Напишите вопрос одним сообщением.\n"
        "Если нажали случайно — /cancel или кнопка ниже.",
        kb_ask_cancel_inline()
    )

async def show_gallery(update: Update, context: ContextTypes.DEFAULT_TYPE, idx: int):
    if not WORKS_PHOTOS:
        await flow_show_text(update, context, "📸 Галерея пока пустая. Добавьте переменную WORKS_PHOTOS.", kb_about_inline())
        return
    idx = max(0, min(idx, len(WORKS_PHOTOS) - 1))
    context.user_data["gallery_idx"] = idx

    prev_i = max(0, idx - 1)
    next_i = min(len(WORKS_PHOTOS) - 1, idx + 1)

    nav = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀️", callback_data=f"GALLERY:{prev_i}"),
            InlineKeyboardButton(f"{idx+1}/{len(WORKS_PHOTOS)}", callback_data="NOOP"),
            InlineKeyboardButton("▶️", callback_data=f"GALLERY:{next_i}"),
        ],
        [InlineKeyboardButton("⬅️ Назад (Обо мне)", callback_data="ABOUT_BACK")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

    caption = "📸 <b>Мои работы</b>\n\nЛистайте кнопками ◀️ ▶️"
    await flow_show_photo(update, context, WORKS_PHOTOS[idx], caption, nav)


# =========================
# Message handlers
# =========================
async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return

    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await flow_show_text(update, context, "Номер не распознан 😕 Нажмите «📱 Отправить номер».")
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = STAGE_NONE

    await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(uid))
    await flow_show_text(update, context, f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅")
    await delete_user_input(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await delete_user_input(update, context)

    uid = update.effective_user.id
    raw = (update.message.text or "").strip()
    raw = normalize_commands(raw)
    txt = normalize_button_text(raw)
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # /cancel текстом тоже
    if raw.lower() in ("/cancel", "отмена", "cancel"):
        await cmd_cancel(update, context)
        return

    # -------- Registration stages (запрет отвечать кнопками) --------
    if stage == STAGE_REG_NAME:
        # если человек жмёт кнопки/меню — не принимаем как имя
        if txt in ("💅 Записаться", "💳 Цены", "👩‍🎨 Обо мне", "📍 Контакты", "👤 Профиль",
                   "📩 Вопрос мастеру", "❓ Вопросы и ответы", "🏠 Меню", "🛠 Админ-панель",
                   "📸 Галерея работ", "⬅️ Назад в меню", "❌ Отменить запись"):
            await flow_show_text(update, context, "✍️ Во время регистрации нужно ввести <b>имя текстом</b>.\nОтмена: /cancel")
            return

        if len(raw) < 2 or not re.fullmatch(r"[A-Za-zА-Яа-яЁё\s\-]{2,60}", raw):
            await flow_show_text(update, context, "Имя лучше написать буквами 🙂\nПример: <code>Анна</code> или <code>Анна Иванова</code>\nОтмена: /cancel")
            return

        context.user_data["reg_name"] = raw.strip()
        context.user_data["stage"] = STAGE_REG_PHONE
        await show_menu_message(update, context, "📱 Отправьте номер 👇", phone_request_kb())
        await flow_show_text(update, context, "📱 Отправьте номер кнопкой ниже (или напишите +79991234567).\nОтмена: /cancel")
        return

    if stage == STAGE_REG_PHONE:
        if txt in ("💅 Записаться", "💳 Цены", "👩‍🎨 Обо мне", "📍 Контакты", "👤 Профиль",
                   "📩 Вопрос мастеру", "❓ Вопросы и ответы", "🏠 Меню", "🛠 Админ-панель",
                   "📸 Галерея работ", "⬅️ Назад в меню", "❌ Отменить запись"):
            await flow_show_text(update, context, "📱 Нужен номер телефона. Нажмите кнопку или напишите номер.\nОтмена: /cancel")
            return

        phone = parse_phone(raw)
        if len(re.sub(r"\D", "", phone)) < 10:
            await flow_show_text(update, context, "Номер не распознан 😕\nФормат: <code>+79991234567</code>\nОтмена: /cancel")
            return

        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE

        await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(uid))
        await flow_show_text(update, context, f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 💅")
        return

    # -------- Ask master --------
    if stage == STAGE_ASK_MASTER:
        if txt in ("🏠 Меню",):
            await cmd_menu(update, context)
            return
        if txt in ("💅 Записаться", "💳 Цены", "👩‍🎨 Обо мне", "📍 Контакты", "👤 Профиль",
                   "❓ Вопросы и ответы", "🛠 Админ-панель", "📸 Галерея работ", "⬅️ Назад в меню", "❌ Отменить запись"):
            await flow_show_text(update, context, "Вы сейчас пишете вопрос мастеру.\nОтмена: /cancel")
            return

        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n🔗 {h(link)}\n\n{h(raw)}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML")
        except Exception:
            pass

        context.user_data["stage"] = STAGE_NONE
        await show_menu_message(update, context, "🏠 Меню 👇", main_menu_kb(uid))
        await flow_show_text(update, context, "✅ Отправил мастеру. Скоро ответим 🙂")
        return

    # -------- Normal routes (Reply buttons) --------
    if txt == "🏠 Меню":
        await cmd_menu(update, context); return

    if txt == "💅 Записаться":
        await start_booking(update, context); return

    if txt == "💳 Цены":
        await cmd_prices(update, context); return

    if txt == "📍 Контакты":
        await flow_show_text(update, context, contacts_text(), contacts_inline()); return

    if txt == "👩‍🎨 Обо мне":
        await show_about(update, context); return

    if txt == "📸 Галерея работ":
        await show_gallery(update, context, int(context.user_data.get("gallery_idx", 0) or 0)); return

    if txt == "⬅️ Назад в меню":
        await cmd_menu(update, context); return

    if txt == "👤 Профиль":
        await show_profile(update, context); return

    if txt == "❌ Отменить запись":
        await cancel_booking_flow(update, context); return

    if txt == "📩 Вопрос мастеру":
        await start_ask_master(update, context); return

    if txt == "❓ Вопросы и ответы":
        await cmd_faq(update, context); return

    if txt == "🛠 Админ-панель":
        await cmd_admin(update, context); return

    # fallback
    await flow_show_text(update, context, "Выберите действие в меню 👇")


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

    if data == "MENU":
        await cmd_menu(update, context); return
    if data == "NOOP":
        return

    if data == "REG_START":
        await start_registration(update, context); return

    if data == "ASK_CANCEL":
        context.user_data["stage"] = STAGE_NONE
        await cmd_menu(update, context)
        await flow_show_text(update, context, "✅ Ок, отменил вопрос. Меню 👇")
        return

    if data == "ABOUT_BACK":
        await show_about(update, context)
        return

    if data.startswith("GALLERY:"):
        try:
            idx = int(data.split(":", 1)[1])
        except Exception:
            idx = 0
        await show_gallery(update, context, idx)
        return

    # Back buttons
    if data.startswith("BACK_CATS:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        await flow_show_text(update, context, "Выберите категорию услуги:", kb_service_cats(did)); return

    if data.startswith("BACK_SVC:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        cat = context.user_data.get("book_cat")
        if not cat:
            await flow_show_text(update, context, "Выберите категорию услуги:", kb_service_cats(did)); return
        await flow_show_text(update, context, "Выберите услугу:", kb_services(did, cat)); return

    if data.startswith("BACK_DAYS:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        await flow_show_text(update, context, "Выберите дату:", kb_days(did)); return

    if data.startswith("BACK_TIMES:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show_text(update, context, "Выберите дату:", kb_days(did)); return
        await flow_show_text(update, context, "Выберите время:", kb_times(did, day_iso)); return

    # Category
    p = split3("CAT")
    if p:
        did, cat = p
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        context.user_data["book_cat"] = cat
        await flow_show_text(update, context, "Выберите услугу:", kb_services(did, cat))
        return

    # Service
    p = split3("SVC")
    if p:
        did, key = p
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        if key not in SERVICES:
            return
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)
        context.user_data.pop("book_date", None)
        context.user_data.pop("book_time", None)
        context.user_data.pop("comment", None)
        await flow_show_text(update, context, f"✅ Вы выбрали:\n<b>{h(title)}</b> — <b>{price} ₽</b>\n\nВыберите дату:", kb_days(did))
        return

    # Day
    p = split3("DAY")
    if p:
        did, day_iso = p
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        context.user_data["book_date"] = day_iso
        context.user_data.pop("book_time", None)
        await flow_show_text(update, context, f"📅 Дата: <b>{fmt_date_ru(day_iso)}</b>\nВыберите время:", kb_times(did, day_iso))
        return

    # Time
    p = split3("TIME")
    if p:
        did, hhmm = p
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await flow_show_text(update, context, "Выберите дату:", kb_days(did)); return
        ok, res = is_time_allowed_for_booking(day_iso, hhmm)
        if not ok:
            await flow_show_text(update, context, f"😕 {res}\nВыберите другое время:", kb_times(did, day_iso)); return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show_text(update, context, "📝 Добавьте комментарий (необязательно) или нажмите «Без комментария».", kb_comment(did))
        return

    if data.startswith("MANUAL_TIME:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        if not context.user_data.get("book_date"):
            await flow_show_text(update, context, "Сначала выберите дату:", kb_days(did)); return
        context.user_data["stage"] = STAGE_MANUAL_TIME
        await flow_show_text(update, context, "✍️ Введите время вручную (например 17:00).")
        return

    if data.startswith("COMMENT:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, payload = parts[1], parts[2]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return
        context.user_data["comment"] = "" if payload == "-" else payload
        context.user_data["stage"] = STAGE_NONE

        title = context.user_data.get("service_title")
        price = context.user_data.get("service_price")
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment") or "—"

        await flow_show_text(
            update, context,
            "✅ <b>Проверьте, всё верно</b>\n\n"
            f"🧾 Услуга: <b>{h(title)}</b>\n"
            f"💰 Цена: <b>{price} ₽</b>\n"
            f"🗓 Дата/время: <b>{fmt_dt_ru(d_iso, t)}</b>\n"
            f"📝 Комментарий: <b>{h(comment)}</b>\n\n"
            "Нажмите <b>✅ Подтвердить запись</b> — и заявка уйдёт мастеру.",
            kb_confirm(did)
        )
        return

    if data.startswith("CONFIRM:"):
        did = data.split(":", 1)[1]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Нажмите 💅 Записаться заново 🙂"); return

        u = store.get_user(uid)
        if not u:
            await flow_show_text(update, context, "Сначала регистрация (1 раз) 🙂", kb_start_for_new_user()); return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0) or 0)
        d_iso = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        ok, checked = is_time_allowed_for_booking(d_iso, t)
        if not ok:
            await flow_show_text(update, context, f"😕 {checked}\nВыберите другое время:", kb_times(did, d_iso)); return

        await flow_show_text(update, context, "⏳ Создаю заявку...")

        done, booking_id, msg = store.create_booking_safe(
            user_id=u.tg_id,
            service_key=key,
            service_title=title,
            price=price,
            book_date=d_iso,
            book_time=checked,
            comment=comment,
        )
        if not done or not booking_id:
            await flow_show_text(update, context, f"😕 {h(msg)}\nПопробуйте выбрать другое время.", kb_times(did, d_iso)); return

        try:
            schedule_reminder(context.application, booking_id, d_iso, checked)
        except Exception:
            pass

        await flow_show_text(update, context, booking_card_user_sent(title, d_iso, checked))

        client_url = user_link(u.tg_id, u.username)
        admin_text = booking_card_for_admin(u, title, price, d_iso, checked, comment, booking_id)
        try:
            await context.bot.send_message(
                ADMIN_ID,
                admin_text,
                parse_mode="HTML",
                reply_markup=kb_adm_confirm_cancel(booking_id, u.tg_id, client_url),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        clear_draft(context)
        return

    # Cancel by user (from profile)
    if data.startswith("UCANCEL:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        did, bid_s = parts[1], parts[2]
        if not must_draft(context, did):
            await flow_show_text(update, context, "Кнопки устарели. Откройте отмену в профиле заново 🙂"); return
        b = store.get_booking(int(bid_s))
        if not b or b.user_id != uid:
            await flow_show_text(update, context, "Запись не найдена."); return
        store.set_booking_status(b.id, "cancelled")
        await flow_show_text(update, context, "✅ Запись отменена.")
        return

    # Admin lists
    if data == "ADM_NEXT" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await flow_show_text(update, context, "Ближайших записей нет 🙂"); return
        lines = ["📌 <b>Ближайшие записи</b>\n"]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await flow_show_text(update, context, "\n".join(lines))
        return

    if data == "ADM_TODAY" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await flow_show_text(update, context, "На сегодня записей нет 🙂"); return
        lines = [f"📅 <b>Сегодня</b> ({fmt_date_ru(today_iso)})\n"]
        for b in items:
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await flow_show_text(update, context, "\n".join(lines))
        return

    if data == "ADM_7D" and is_admin(uid):
        today = now_local().date()
        day_from = today.isoformat()
        day_to = (today + timedelta(days=6)).isoformat()
        items = store.list_range(day_from, day_to)
        if not items:
            await flow_show_text(update, context, "На ближайшие 7 дней записей нет 🙂"); return
        lines = [f"📆 <b>Записи на 7 дней</b> ({fmt_date_ru(day_from)} — {fmt_date_ru(day_to)})\n"]
        cur = ""
        for b in items:
            if b.book_date != cur:
                cur = b.book_date
                lines.append(f"\n<b>{fmt_date_ru(cur)}</b>")
            uu = store.get_user(b.user_id)
            who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
            lines.append(f"• <b>{b.book_time}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
        await flow_show_text(update, context, "\n".join(lines))
        return

    # Admin confirm/cancel
    if data.startswith("ADM_CONFIRM:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            return
        store.set_booking_status(booking_id, "confirmed")
        try:
            await context.bot.send_message(
                b.user_id,
                "✅ <b>Запись подтверждена!</b>\n\n"
                f"🗓 {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"🧾 {h(b.service_title)}\n\n"
                f"{contacts_text()}",
                parse_mode="HTML",
                reply_markup=contacts_inline(),
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("ADM_CANCEL:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            return
        store.set_booking_status(booking_id, "cancelled")
        try:
            await context.bot.send_message(
                b.user_id,
                "❌ <b>Запись отменена мастером.</b>\n\n"
                f"🗓 {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"🧾 {h(b.service_title)}\n\n"
                "Хотите — подберём другое время 🙂",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return


# =========================
# Startup / app
# =========================
async def on_startup(app: Application):
    # быстрые команды (чтобы при вводе "/" показывало подсказки)
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Запуск"),
            BotCommand("menu", "Меню"),
            BotCommand("book", "Записаться"),
            BotCommand("prices", "Цены"),
            BotCommand("profile", "Профиль"),
            BotCommand("faq", "Вопросы и ответы"),
            BotCommand("admin", "Админ-панель"),
            BotCommand("cancel", "Отменить действие"),
        ])
    except Exception:
        pass

    try:
        reschedule_all_reminders(app)
    except Exception as e:
        log.warning("Startup reminders failed: %s", e)

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("book", cmd_book))
    app.add_handler(CommandHandler("prices", cmd_prices))
    app.add_handler(CommandHandler("faq", cmd_faq))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    return app

def main():
    app = build_app()
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
