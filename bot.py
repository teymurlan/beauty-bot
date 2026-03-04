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
            if not row: return None
            return User(tg_id=row["tg_id"], username=row["username"] or "", full_name=row["full_name"], phone=row["phone"], created_at=row["created_at"])

    def delete_user(self, tg_id: int) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
            c.execute("DELETE FROM bookings WHERE user_id=?", (tg_id,))

    def block_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("INSERT OR IGNORE INTO blocked_slots(book_date, book_time) VALUES(?,?)", (book_date, book_time))

    def unblock_slot(self, book_date: str, book_time: str) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM blocked_slots WHERE book_date=? AND book_time=?", (book_date, book_time))

    def is_slot_blocked(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?", (book_date, book_time)).fetchone()
            return row is not None

    def is_slot_taken_exact(self, book_date: str, book_time: str) -> bool:
        with self._conn() as c:
            row = c.execute("SELECT 1 FROM bookings WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed') LIMIT 1", (book_date, book_time)).fetchone()
            return row is not None

    def create_booking_safe(self, user_id: int, service_key: str, service_title: str, price: int, book_date: str, book_time: str, comment: str) -> Tuple[bool, Optional[int], str]:
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=? LIMIT 1", (book_date, book_time)).fetchone():
                conn.rollback()
                return False, None, "Это время заблокировано."
            if conn.execute("SELECT 1 FROM bookings WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed') LIMIT 1", (book_date, book_time)).fetchone():
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
            return False, None, "Ошибка БД."
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
            rows = c.execute("SELECT * FROM bookings WHERE book_date=? AND status IN ('pending','confirmed') ORDER BY book_time", (day,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_range(self, day_from: str, day_to: str) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM bookings WHERE book_date>=? AND book_date<=? AND status IN ('pending','confirmed') ORDER BY book_date, book_time", (day_from, day_to)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM bookings WHERE status IN ('pending','confirmed') ORDER BY book_date, book_time LIMIT ?", (int(limit),)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_user_upcoming(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM bookings WHERE user_id=? AND status IN ('pending','confirmed') ORDER BY book_date, book_time", (user_id,)).fetchall()
            return [self._row_to_booking(r) for r in rows]

    def list_for_reminders(self) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM bookings WHERE status IN ('pending','confirmed') AND reminder_sent=0 ORDER BY book_date, book_time").fetchall()
            return [self._row_to_booking(r) for r in rows]

    def _row_to_booking(self, row: sqlite3.Row) -> Booking:
        return Booking(
            id=row["id"], user_id=row["user_id"], service_key=row["service_key"], service_title=row["service_title"],
            price=row["price"], book_date=row["book_date"], book_time=row["book_time"], comment=row["comment"] or "",
            status=row["status"], created_at=row["created_at"], reminder_sent=int(row["reminder_sent"])
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
MASTER_TEXT = os.getenv("MASTER_TEXT", "Аккуратно, стерильно и с любовью к деталям ✨").strip()
MASTER_PHOTO = os.getenv("MASTER_PHOTO", "").strip()

WORK_START = os.getenv("WORK_START", "08:00").strip()
WORK_END = os.getenv("WORK_END", "23:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"
AUTO_DELETE_BOT_MESSAGES = os.getenv("AUTO_DELETE_BOT_MESSAGES", "1").strip() == "1"
KEEP_LAST_BOT_MESSAGES = int(os.getenv("KEEP_LAST_BOT_MESSAGES", "2").strip() or "2")

if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID: raise RuntimeError("ADMIN_ID is required")

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
def h(s: str) -> str: return html.escape(str(s or ""))
def is_admin(user_id: int) -> bool: return user_id == ADMIN_ID
def now_local() -> datetime: return datetime.now(tz)
def fmt_date_ru(iso_date: str) -> str: return date.fromisoformat(iso_date).strftime("%d.%m.%Y")
def fmt_dt_ru(iso_date: str, hhmm: str) -> str: return f"{fmt_date_ru(iso_date)} {hhmm}"
def parse_phone(text: str) -> str:
    t = re.sub(r"[^\d+]", "", text or "")
    if t.startswith("8") and len(t) >= 11: t = "+7" + t[1:]
    if t.startswith("7") and len(t) == 11: t = "+" + t
    return t
def user_link(user_id: int, username: str) -> str: return f"https://t.me/{username}" if username else f"tg://user?id={user_id}"
def parse_hhmm(text: str) -> Optional[str]:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", text or "")
    if not m: return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59): return None
    return f"{hh:02d}:{mm:02d}"

def work_bounds_for_day(day_iso: str) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(day_iso)
    ws = datetime.strptime(WORK_START, "%H:%M").time()
    we = datetime.strptime(WORK_END, "%H:%M").time()
    return datetime.combine(d, ws).replace(tzinfo=tz), datetime.combine(d, we).replace(tzinfo=tz)

def is_time_allowed_for_booking(day_iso: str, hhmm: str) -> Tuple[bool, str]:
    parsed = parse_hhmm(hhmm)
    if not parsed: return False, "Формат HH:MM."
    start_dt, end_dt = work_bounds_for_day(day_iso)
    hh, mm = map(int, parsed.split(":"))
    slot_dt = datetime.combine(date.fromisoformat(day_iso), dtime(hh, mm)).replace(tzinfo=tz)
    if slot_dt < start_dt or slot_dt > end_dt: return False, f"Вне {WORK_START}–{WORK_END}."
    if day_iso == now_local().date().isoformat() and slot_dt <= now_local(): return False, "Прошло."
    if store.is_slot_blocked(day_iso, parsed): return False, "Заблокировано."
    if store.is_slot_taken_exact(day_iso, parsed): return False, "Занято."
    return True, parsed

def new_draft_id() -> str: return secrets.token_hex(3)
def set_draft(context: ContextTypes.DEFAULT_TYPE) -> str:
    did = new_draft_id()
    context.user_data["draft_id"] = did
    return did
def get_draft(context: ContextTypes.DEFAULT_TYPE) -> str: return str(context.user_data.get("draft_id", ""))
def must_draft(context: ContextTypes.DEFAULT_TYPE, did: str) -> bool: return did and did == get_draft(context)
def clear_draft(context: ContextTypes.DEFAULT_TYPE):
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat", "draft_id"]: context.user_data.pop(k, None)
    context.user_data["stage"] = STAGE_NONE

def remember_bot_msg(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    ids = context.user_data.get("bot_msg_ids", [])
    if not isinstance(ids, list): ids = []
    ids.append(int(message_id))
    context.user_data["bot_msg_ids"] = ids[-50:]

async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try: await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except: pass

async def delete_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if AUTO_DELETE_USER_INPUT and update.message: await safe_delete_message(context, update.effective_chat.id, update.message.message_id)

async def cleanup_bot_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int, keep_flow: bool = True):
    if not AUTO_DELETE_BOT_MESSAGES: return
    ids = context.user_data.get("bot_msg_ids", [])
    if not ids: return
    flow_id = context.user_data.get("flow_msg_id")
    keep_set = {int(flow_id)} if keep_flow and flow_id else set()
    tail = ids[-KEEP_LAST_BOT_MESSAGES:]
    keep_set.update(tail)
    for mid in ids:
        if mid not in keep_set: await safe_delete_message(context, chat_id, mid)
    context.user_data["bot_msg_ids"] = [mid for mid in ids if mid in keep_set]

async def send_clean(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, clean_before=True):
    chat_id = update.effective_chat.id
    if clean_before: await cleanup_bot_messages(context, chat_id)
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=reply_markup or ReplyKeyboardRemove(), disable_web_page_preview=True)
    remember_bot_msg(context, msg.message_id)
    return msg

# =========================
# UI
# =========================
def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [["💅 Записаться", "💳 Цены"], ["👩🎨 Обо мне", "📍 Контакты"], ["👤 Профиль", "📩 Вопрос мастеру"], ["🏠 Меню"]]
    if is_admin(user_id): kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup: return ReplyKeyboardMarkup([[KeyboardButton("📱 Отправить номер", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)

def contacts_inline() -> Optional[InlineKeyboardMarkup]:
    rows = []
    if ADMIN_CONTACT: rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if YANDEX_MAP_URL: rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None

def contacts_text() -> str: return f"📍 <b>Как нас найти</b>\n\n• {h(ADDRESS)}\n• {h(HOW_TO_FIND)}\n\n🕘 Время работы: <b>{h(WORK_START)}–{h(WORK_END)}</b>\n"
def prices_text() -> str:
    lines = ["💳 <b>Цены</b>\n"]
    for k, (t, p) in SERVICES.items(): lines.append(f"• {h(t)} — <b>{p} ₽</b>")
    return "\n".join(lines)

def kb_start_for_new_user() -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("📝 Регистрация", callback_data="REG_START")], [InlineKeyboardButton("💅 Записаться", callback_data="BOOK_START")], [InlineKeyboardButton("🏠 В меню", callback_data="MENU")]])
def kb_service_cats(did: str) -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("✨ Маникюр", callback_data=f"CAT:{did}:mn")], [InlineKeyboardButton("🦶 Педикюр", callback_data=f"CAT:{did}:pd")], [InlineKeyboardButton("🌟 Дополнительно", callback_data=f"CAT:{did}:extra")], [InlineKeyboardButton("🏠 В меню", callback_data="MENU")]])
def kb_services(did: str, cat: str) -> InlineKeyboardMarkup:
    if cat == "mn": keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd": keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else: keys = ["ext", "corr", "design"]
    rows = [[InlineKeyboardButton(f"{SERVICES[k][0]} — {SERVICES[k][1]} ₽", callback_data=f"SVC:{did}:{k}")] for k in keys]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_CATS:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_days(did: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for iso in days_from_today(31):
        row.append(InlineKeyboardButton(fmt_date_ru(iso), callback_data=f"DAY:{did}:{iso}"))
        if len(row) == 3: rows.append(row); row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_SVC:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_times(did: str, day_iso: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)
    rows, row = [], []
    for t in slots:
        ok, _ = is_time_allowed_for_booking(day_iso, t)
        if ok:
            row.append(InlineKeyboardButton(t, callback_data=f"TIME:{did}:{t}"))
            if len(row) == 4: rows.append(row); row = []
    if row: rows.append(row)
    if not rows: rows = [[InlineKeyboardButton("😕 Нет слотов", callback_data="NOOP")]]
    rows.append([InlineKeyboardButton("✍️ Вручную", callback_data=f"MANUAL_TIME:{did}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_DAYS:{did}")])
    rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_comment(did: str) -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("Без комментария", callback_data=f"COMMENT:{did}:-")], [InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_TIMES:{did}")], [InlineKeyboardButton("🏠 В меню", callback_data="MENU")]])
def kb_confirm(did: str) -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Подтвердить", callback_data=f"CONFIRM:{did}")], [InlineKeyboardButton("⬅️ Назад", callback_data=f"BACK_TIMES:{did}")], [InlineKeyboardButton("🏠 В меню", callback_data="MENU")]])
def kb_admin_panel() -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("📌 Ближайшие", callback_data="ADM_NEXT")], [InlineKeyboardButton("📅 Сегодня", callback_data="ADM_TODAY"), InlineKeyboardButton("📆 7 дней", callback_data="ADM_7D")], [InlineKeyboardButton("🏠 В меню", callback_data="MENU")]])
def kb_adm_confirm_cancel(bid: int, url: str, uid: int) -> InlineKeyboardMarkup: return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Да", callback_data=f"ADM_CONFIRM:{bid}"), InlineKeyboardButton("❌ Нет", callback_data=f"ADM_CANCEL:{bid}")], [InlineKeyboardButton("💬 Написать", url=url)]])

async def flow_show(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, markup=None):
    chat_id = update.effective_chat.id
    flow_msg_id = context.user_data.get("flow_msg_id")
    if flow_msg_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=int(flow_msg_id), text=text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
            return
        except: pass
    await cleanup_bot_messages(context, chat_id, keep_flow=False)
    msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=markup, disable_web_page_preview=True)
    context.user_data["flow_msg_id"] = msg.message_id
    remember_bot_msg(context, msg.message_id)

def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title, price, d_iso, t, comment = context.user_data.get("service_title"), context.user_data.get("service_price"), context.user_data.get("book_date"), context.user_data.get("book_time"), context.user_data.get("comment") or "—"
    if not title or not d_iso or not t: return "⚠️ Ошибка."
    return f"Проверьте:\n• Услуга: <b>{h(title)}</b>\n• Цена: <b>{price} ₽</b>\n• Дата: <b>{fmt_dt_ru(d_iso, t)}</b>\n• Коммент: <b>{h(comment)}</b>"

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    bid = int(context.job.data.get("booking_id"))
    b = store.get_booking(bid)
    if not b or b.reminder_sent == 1 or b.status not in ("pending", "confirmed"): return
    text = f"⏰ <b>Напоминание</b>\n• {fmt_dt_ru(b.book_date, b.book_time)}\n• {h(b.service_title)}"
    try:
        await context.bot.send_message(b.user_id, text, parse_mode="HTML", reply_markup=contacts_inline())
        store.mark_reminder_sent(b.id)
    except: pass

def schedule_reminder(app: Application, bid: int, iso: str, hhmm: str):
    if not app.job_queue: return
    remind_at = datetime.combine(date.fromisoformat(iso), dtime(*map(int, hhmm.split(":")))).replace(tzinfo=tz) - timedelta(hours=24)
    if remind_at <= now_local(): return
    app.job_queue.run_once(reminder_job, when=(remind_at - now_local()).total_seconds(), data={"booking_id": bid}, name=f"rem_{bid}")

# =========================
# Flows
# =========================
def _normalize_btn(text: str) -> str:
    t = (text or "").strip()
    t_clean = re.sub(r'[^\w\s]', '', t).lower().strip()
    if "записаться" in t_clean or "запись" in t_clean: return "💅 Записаться"
    if "цены" in t_clean or "прайс" in t_clean: return "💳 Цены"
    if "обо мне" in t_clean or "мастер" in t_clean: return "👩🎨 Обо мне"
    if "контакты" in t_clean or "адрес" in t_clean: return "📍 Контакты"
    if "профиль" in t_clean: return "👤 Профиль"
    if "вопрос" in t_clean: return "📩 Вопрос мастеру"
    if "меню" in t_clean or "главная" in t_clean: return "🏠 Меню"
    if "админ" in t_clean: return "🛠 Админ-панель"
    return t

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    clear_draft(context)
    await send_clean(update, context, f"✨ <b>{h(SALON_NAME)}</b>\nМеню ниже 👇", reply_markup=main_menu_kb(uid))
    if not store.get_user(uid): await flow_show(update, context, "Нужна регистрация.", kb_start_for_new_user())

async def profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await flow_show(update, context, "Нет профиля.", kb_start_for_new_user())
        return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отменить запись", callback_data="CANCEL_LIST")], [InlineKeyboardButton("🏠 Меню", callback_data="MENU")]])
    await send_clean(update, context, f"👤 <b>Профиль</b>\n• Имя: {h(u.full_name)}\n• Тел: {h(u.phone)}", reply_markup=kb)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt_raw = (update.message.text or "").strip()
    txt = _normalize_btn(txt_raw)
    
    # Check menu buttons first to reset stage
    menu_btns = ["💅 Записаться", "💳 Цены", "👩🎨 Обо мне", "📍 Контакты", "👤 Профиль", "📩 Вопрос мастеру", "🏠 Меню", "🛠 Админ-панель"]
    if txt in menu_btns: context.user_data["stage"] = STAGE_NONE

    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    if stage == STAGE_REG_NAME:
        context.user_data["reg_name"] = txt_raw
        context.user_data["stage"] = STAGE_REG_PHONE
        await send_clean(update, context, "Номер телефона:", reply_markup=phone_request_kb())
        await delete_user_input(update, context); return
    if stage == STAGE_REG_PHONE:
        phone = parse_phone(txt_raw)
        store.upsert_user(uid, update.effective_user.username or "", context.user_data.get("reg_name", "Клиент"), phone)
        context.user_data["stage"] = STAGE_NONE
        await send_clean(update, context, "Готово!", reply_markup=main_menu_kb(uid))
        await delete_user_input(update, context); return
    if stage == STAGE_MANUAL_TIME:
        ok, res = is_time_allowed_for_booking(context.user_data.get("book_date"), txt_raw)
        if not ok: await send_clean(update, context, f"😕 {res}"); return
        context.user_data["book_time"] = res
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_show(update, context, "Комментарий:", kb_comment(get_draft(context)))
        await delete_user_input(update, context); return
    if stage == STAGE_BOOK_COMMENT:
        context.user_data["comment"] = txt_raw
        context.user_data["stage"] = STAGE_NONE
        await flow_show(update, context, build_confirm_text(context), kb_confirm(get_draft(context)))
        await delete_user_input(update, context); return
    if stage == STAGE_ASK_MASTER:
        who = f"{u.full_name} ({u.phone})" if u else "Клиент"
        await context.bot.send_message(ADMIN_ID, f"📩 <b>Вопрос</b>\nОт: {h(who)}\n\n{h(txt_raw)}", parse_mode="HTML")
        await send_clean(update, context, "Отправлено!", reply_markup=main_menu_kb(uid))
        context.user_data["stage"] = STAGE_NONE
        await delete_user_input(update, context); return

    if txt == "💅 Записаться":
        if not u: await flow_show(update, context, "Регистрация!", kb_start_for_new_user())
        else: await flow_show(update, context, "Категория:", kb_service_cats(set_draft(context)))
    elif txt == "💳 Цены": await send_clean(update, context, prices_text(), reply_markup=main_menu_kb(uid))
    elif txt == "📍 Контакты": await send_clean(update, context, contacts_text(), reply_markup=contacts_inline() or main_menu_kb(uid))
    elif txt == "👤 Профиль": await profile_cmd(update, context)
    elif txt == "📩 Вопрос мастеру":
        context.user_data["stage"] = STAGE_ASK_MASTER
        await send_clean(update, context, "Ваш вопрос:", reply_markup=ReplyKeyboardRemove())
    elif txt == "🏠 Меню": await start(update, context)
    elif txt == "🛠 Админ-панель" and is_admin(uid): await flow_show(update, context, "Админ", kb_admin_panel())
    else: await send_clean(update, context, "Меню 👇", reply_markup=main_menu_kb(uid))
    await delete_user_input(update, context)

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer(); uid, data = q.from_user.id, q.data or ""
    if data == "MENU": clear_draft(context); await start(update, context); return
    if data == "REG_START": context.user_data["stage"] = STAGE_REG_NAME; await flow_show(update, context, "Имя:"); return
    if data == "BOOK_START": await flow_show(update, context, "Категория:", kb_service_cats(set_draft(context))); return
    if data == "CANCEL_LIST":
        items = store.list_user_upcoming(uid)
        if not items: await flow_show(update, context, "Нет записей."); return
        did = set_draft(context)
        rows = [[InlineKeyboardButton(f"❌ {fmt_dt_ru(b.book_date, b.book_time)}", callback_data=f"UCANCEL:{did}:{b.id}")] for b in items]
        await flow_show(update, context, "Отмена:", InlineKeyboardMarkup(rows)); return
    if data.startswith("CAT:"):
        did, cat = data.split(":")[1:]
        if must_draft(context, did): context.user_data["book_cat"] = cat; await flow_show(update, context, "Услуга:", kb_services(did, cat))
    if data.startswith("SVC:"):
        did, key = data.split(":")[1:]
        if must_draft(context, did):
            t, p = SERVICES[key]
            context.user_data.update({"service_key": key, "service_title": t, "service_price": p})
            await flow_show(update, context, f"{t} - {p}₽\nДата:", kb_days(did))
    if data.startswith("DAY:"):
        did, iso = data.split(":")[1:]
        if must_draft(context, did): context.user_data["book_date"] = iso; await flow_show(update, context, f"{iso}\nВремя:", kb_times(did, iso))
    if data.startswith("TIME:"):
        did, t = data.split(":")[1:]
        if must_draft(context, did):
            ok, res = is_time_allowed_for_booking(context.user_data.get("book_date"), t)
            if ok: context.user_data["book_time"] = res; context.user_data["stage"] = STAGE_BOOK_COMMENT; await flow_show(update, context, "Коммент:", kb_comment(did))
    if data.startswith("CONFIRM:"):
        did = data.split(":")[1]
        if must_draft(context, did):
            u = store.get_user(uid)
            done, bid, msg = store.create_booking_safe(uid, context.user_data["service_key"], context.user_data["service_title"], context.user_data["service_price"], context.user_data["book_date"], context.user_data["book_time"], context.user_data.get("comment", ""))
            if done:
                schedule_reminder(context.application, bid, context.user_data["book_date"], context.user_data["book_time"])
                await flow_show(update, context, "Записано! Ждите подтверждения."); await context.bot.send_message(ADMIN_ID, f"Новая запись: {bid}", reply_markup=kb_adm_confirm_cancel(bid, user_link(uid, q.from_user.username), uid))
                clear_draft(context)
    if data.startswith("UCANCEL:"):
        did, bid = data.split(":")[1:]
        if must_draft(context, did): store.set_booking_status(int(bid), "cancelled"); await flow_show(update, context, "Отменено."); clear_draft(context)

async def on_startup(app: Application):
    await app.bot.set_my_commands([BotCommand("start", "Запуск"), BotCommand("menu", "Меню"), BotCommand("book", "Запись"), BotCommand("profile", "Профиль")])
    reschedule_all_reminders(app)

def main():
    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", start))
    app.add_handler(CommandHandler("profile", profile_cmd))
    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.CONTACT, handle_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()

if __name__ == "__main__": main()
