# bot.py
# python-telegram-bot >= 21

import os
import re
import html
import sqlite3
import logging
import secrets
import calendar as cal
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
log = logging.getLogger("booking-bot")

# =========================
# ENV (под твой код)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int((os.getenv("ADMIN_ID", "0") or "0").strip())
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

SALON_NAME = os.getenv("SALON_NAME", "Запись к мастеру").strip()

ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()
YANDEX_MAP_URL = os.getenv("YANDEX_MAP_URL", "").strip()

ADDRESS = os.getenv("ADDRESS", "Адрес: (впишите адрес)").strip()
HOW_TO_FIND = os.getenv("HOW_TO_FIND", "Как нас найти: (ориентиры)").strip()
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
SLOT_MINUTES = int((os.getenv("SLOT_MINUTES", "60") or "60").strip())

AUTO_DELETE_USER_INPUT = (os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")

tz = ZoneInfo(TZ_NAME)

# =========================
# Services (оставил твой список — можешь менять)
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

# =========================
# Storage
# =========================
class Storage:
    def __init__(self, path: str):
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
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_dt ON bookings(book_date, book_time);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id);")

    # users
    def upsert_user(self, tg_id: int, username: str, full_name: str, phone: str):
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
            r = c.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()
            if not r:
                return None
            return User(r["tg_id"], r["username"] or "", r["full_name"], r["phone"], r["created_at"])

    # bookings
    def is_slot_taken(self, day_iso: str, hhmm: str) -> bool:
        with self._conn() as c:
            r = c.execute("""
                SELECT 1 FROM bookings
                WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
                LIMIT 1
            """, (day_iso, hhmm)).fetchone()
            return r is not None

    def create_booking(self, user_id: int, service_key: str, title: str, price: int, day_iso: str, hhmm: str, comment: str) -> Tuple[bool, Optional[int], str]:
        if self.is_slot_taken(day_iso, hhmm):
            return False, None, "Это время уже занято."
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self._conn() as c:
            cur = c.execute("""
                INSERT INTO bookings(user_id, service_key, service_title, price, book_date, book_time, comment, status, created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (user_id, service_key, title, int(price), day_iso, hhmm, comment or "", "pending", now))
            return True, int(cur.lastrowid), "ok"

    def get_booking(self, bid: int) -> Optional[Booking]:
        with self._conn() as c:
            r = c.execute("SELECT * FROM bookings WHERE id=?", (bid,)).fetchone()
            if not r:
                return None
            return Booking(
                id=r["id"], user_id=r["user_id"], service_key=r["service_key"],
                service_title=r["service_title"], price=r["price"],
                book_date=r["book_date"], book_time=r["book_time"],
                comment=r["comment"] or "", status=r["status"], created_at=r["created_at"]
            )

    def set_status(self, bid: int, status: str):
        with self._conn() as c:
            c.execute("UPDATE bookings SET status=? WHERE id=?", (status, bid))

    def list_user_active(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
                SELECT * FROM bookings
                WHERE user_id=? AND status IN ('pending','confirmed')
                ORDER BY book_date, book_time
            """, (user_id,)).fetchall()
            return [self.get_booking(int(r["id"])) for r in rows if r]

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
                SELECT * FROM bookings
                WHERE status IN ('pending','confirmed')
                ORDER BY book_date, book_time
                LIMIT ?
            """, (int(limit),)).fetchall()
            out = []
            for r in rows:
                out.append(self.get_booking(int(r["id"])))
            return [b for b in out if b]

store = Storage(DB_PATH)

# =========================
# Stages / user_data keys
# =========================
ST_NONE = "none"
ST_REG_NAME = "reg_name"
ST_REG_PHONE = "reg_phone"
ST_BOOK_COMMENT = "book_comment"
ST_ASK = "ask"

def h(s: str) -> str:
    return html.escape(str(s or ""))

def now_local() -> datetime:
    return datetime.now(tz)

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def parse_phone(text: str) -> str:
    t = re.sub(r"[^\d+]", "", text or "")
    if t.startswith("8") and len(t) >= 11:
        t = "+7" + t[1:]
    if t.startswith("7") and len(t) == 11:
        t = "+" + t
    return t

def parse_hhmm(text: str) -> Optional[str]:
    m = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", text or "")
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return f"{hh:02d}:{mm:02d}"

def fmt_date_ru(d: date) -> str:
    return d.strftime("%d.%m.%Y")

def fmt_dt_ru(day_iso: str, hhmm: str) -> str:
    return f"{date.fromisoformat(day_iso).strftime('%d.%m.%Y')} {hhmm}"

def user_link(uid: int, username: str) -> str:
    return f"https://t.me/{username}" if username else f"tg://user?id={uid}"

def gen_flow_id() -> str:
    return secrets.token_hex(3)

# =========================
# Keyboards (как на скрине)
# =========================
def reply_menu(uid: int) -> ReplyKeyboardMarkup:
    kb = [
        ["📅 Записаться"],
        ["📋 Мои записи", "💅 Услуги"],
        ["👩‍🎨 Обо мне", "❓ Возник вопрос"],
    ]
    if is_admin(uid):
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def kb_back_to_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="MENU")]])

def kb_phone() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# Услуги: пагинация “1 из N”
SERVICES_LIST = list(SERVICES.items())
PAGE_SIZE = 5

def kb_services_page(page: int) -> InlineKeyboardMarkup:
    total = (len(SERVICES_LIST) + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, total - 1))
    start = page * PAGE_SIZE
    chunk = SERVICES_LIST[start:start + PAGE_SIZE]

    rows = []
    for key, (title, price) in chunk:
        rows.append([InlineKeyboardButton(f"{title} - {price}₽", callback_data=f"SVC:{key}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"SVC_PAGE:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1} из {total}", callback_data="NOOP"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"SVC_PAGE:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

# Календарь: месяц сеткой как на скрине
def kb_calendar(year: int, month: int) -> InlineKeyboardMarkup:
    c = cal.Calendar(firstweekday=0)  # Пн=0
    weeks = c.monthdayscalendar(year, month)

    rows = []
    # шапка дней недели
    rows.append([
        InlineKeyboardButton("Пн", callback_data="NOOP"),
        InlineKeyboardButton("Вт", callback_data="NOOP"),
        InlineKeyboardButton("Ср", callback_data="NOOP"),
        InlineKeyboardButton("Чт", callback_data="NOOP"),
        InlineKeyboardButton("Пт", callback_data="NOOP"),
        InlineKeyboardButton("Сб", callback_data="NOOP"),
        InlineKeyboardButton("Вс", callback_data="NOOP"),
    ])

    today = now_local().date()
    for w in weeks:
        row = []
        for d in w:
            if d == 0:
                row.append(InlineKeyboardButton(" ", callback_data="NOOP"))
            else:
                ddate = date(year, month, d)
                # прошедшие даты блокируем
                if ddate < today:
                    row.append(InlineKeyboardButton(str(d), callback_data="NOOP"))
                else:
                    row.append(InlineKeyboardButton(str(d), callback_data=f"DAY:{ddate.isoformat()}"))
        rows.append(row)

    # навигация: ◀️ Месяц ▶️
    prev_y, prev_m = (year, month - 1) if month > 1 else (year - 1, 12)
    next_y, next_m = (year, month + 1) if month < 12 else (year + 1, 1)
    rows.append([
        InlineKeyboardButton("◀️", callback_data=f"CAL:{prev_y}:{prev_m}"),
        InlineKeyboardButton(f"{cal.month_name[month]} {year}", callback_data="NOOP"),
        InlineKeyboardButton("▶️", callback_data=f"CAL:{next_y}:{next_m}"),
    ])
    rows.append([InlineKeyboardButton("Сегодня", callback_data="CAL_TODAY")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="BOOK")])
    return InlineKeyboardMarkup(rows)

def work_bounds_for_day(day_iso: str) -> Tuple[datetime, datetime]:
    d = date.fromisoformat(day_iso)
    ws = datetime.strptime(WORK_START, "%H:%M").time()
    we = datetime.strptime(WORK_END, "%H:%M").time()
    start_dt = datetime.combine(d, ws).replace(tzinfo=tz)
    end_dt = datetime.combine(d, we).replace(tzinfo=tz)
    return start_dt, end_dt

def build_time_slots(day_iso: str) -> List[str]:
    start_dt, end_dt = work_bounds_for_day(day_iso)
    slots = []
    cur = start_dt
    while cur <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots

def time_allowed(day_iso: str, hhmm: str) -> bool:
    parsed = parse_hhmm(hhmm)
    if not parsed:
        return False
    d = date.fromisoformat(day_iso)
    hh, mm = map(int, parsed.split(":"))
    slot_dt = datetime.combine(d, dtime(hh, mm)).replace(tzinfo=tz)
    if d == now_local().date() and slot_dt <= now_local():
        return False
    if store.is_slot_taken(day_iso, parsed):
        return False
    start_dt, end_dt = work_bounds_for_day(day_iso)
    if slot_dt < start_dt or slot_dt > end_dt:
        return False
    return True

def kb_times(day_iso: str) -> InlineKeyboardMarkup:
    slots = build_time_slots(day_iso)
    rows, row = [], []
    for t in slots:
        if not time_allowed(day_iso, t):
            continue
        row.append(InlineKeyboardButton(t, callback_data=f"TIME:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободного времени", callback_data="NOOP")]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="BOOK")])
    return InlineKeyboardMarkup(rows)

def kb_my_bookings(bookings: List[Booking]) -> InlineKeyboardMarkup:
    rows = []
    for b in bookings[:12]:
        rows.append([InlineKeyboardButton(
            f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
            callback_data=f"CANCEL:{b.id}"
        )])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)

def kb_admin_booking(bid: int, client_url: str, client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_OK:{bid}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_NO:{bid}")
        ],
        [InlineKeyboardButton("💬 Профиль клиента", url=client_url)],
        [InlineKeyboardButton("✍️ Написать клиенту в боте", url=f"tg://user?id={client_id}")],
    ])

# =========================
# Rendering “экранов” (редактируем одно сообщение)
# =========================
async def show_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, markup: Optional[InlineKeyboardMarkup]):
    chat_id = update.effective_chat.id
    mid = context.user_data.get("screen_mid")

    if mid:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(mid),
                text=text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True
            )
            return
        except Exception:
            pass

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=markup,
        disable_web_page_preview=True
    )
    context.user_data["screen_mid"] = msg.message_id

async def delete_user_message_later(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not AUTO_DELETE_USER_INPUT:
        return
    if not update.message:
        return
    try:
        await context.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except Exception:
        pass

# =========================
# Texts
# =========================
def text_menu() -> str:
    return (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Выберите раздел снизу 👇\n"
    )

def text_about() -> str:
    return (
        f"👩‍🎨 <b>{h(MASTER_NAME)}</b>\n"
        f"<b>{h(MASTER_EXPERIENCE)}</b>\n\n"
        f"{h(MASTER_TEXT)}"
    )

def text_contacts() -> str:
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

# =========================
# Commands
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data.setdefault("stage", ST_NONE)
    context.user_data.pop("draft", None)
    context.user_data.pop("ask_text", None)

    await update.effective_chat.send_action("typing")
    await update.message.reply_text(text_menu(), parse_mode="HTML", reply_markup=reply_menu(uid))

    u = store.get_user(uid)
    if not u:
        context.user_data["stage"] = ST_REG_NAME
        await show_screen(
            update, context,
            "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)",
            None
        )
    else:
        await show_screen(update, context, "Готово 🙂 Нажмите <b>📅 Записаться</b> или <b>💅 Услуги</b>.", kb_back_to_menu())

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data["stage"] = ST_NONE
    context.user_data.pop("draft", None)
    await update.message.reply_text("🏠 Меню", reply_markup=reply_menu(uid))
    await show_screen(update, context, text_menu(), None)

# =========================
# Reply button routing
# =========================
def norm_btn(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t

async def open_book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        context.user_data["stage"] = ST_REG_NAME
        await show_screen(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None)
        return

    # draft booking
    context.user_data["draft"] = {
        "flow": gen_flow_id(),
        "service_key": None,
        "service_title": None,
        "price": None,
        "day": None,
        "time": None,
        "comment": "",
    }

    today = now_local().date()
    await show_screen(
        update, context,
        "📅 <b>Запись</b>\n\n1) Выберите услугу в разделе <b>💅 Услуги</b>\n2) Затем выберите дату в календаре 👇",
        kb_calendar(today.year, today.month)
    )

async def open_services(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    await show_screen(update, context, "💅 <b>Наши услуги</b>", kb_services_page(page))

async def open_my_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        context.user_data["stage"] = ST_REG_NAME
        await show_screen(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None)
        return
    items = store.list_user_active(uid)
    if not items:
        await show_screen(update, context, "📋 <b>Мои записи</b>\n\nУ вас пока нет активных записей 🙂", kb_back_to_menu())
        return

    lines = ["📋 <b>Мои записи</b>\n"]
    for b in items[:10]:
        st = "⏳ ожидает" if b.status == "pending" else "✅ подтверждена"
        lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — <b>{b.price}₽</b> ({st})")
    await show_screen(update, context, "\n".join(lines), kb_my_bookings(items))

async def open_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_screen(update, context, text_about(), InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Как нас найти", callback_data="CONTACTS")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="MENU")]
    ]))

async def open_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        context.user_data["stage"] = ST_REG_NAME
        await show_screen(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None)
        return
    context.user_data["stage"] = ST_ASK
    await show_screen(update, context, "❓ <b>Возник вопрос</b>\n\nНапишите ваш вопрос одним сообщением — я отправлю мастеру.", kb_back_to_menu())

async def open_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await show_screen(update, context, "⛔️ Доступ только для администратора.", kb_back_to_menu())
        return

    items = store.list_next(25)
    if not items:
        await show_screen(update, context, "🛠 <b>Админ-панель</b>\n\nЗаписей пока нет.", kb_back_to_menu())
        return

    lines = ["🛠 <b>Ближайшие записи</b>\n"]
    for b in items:
        lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {b.price}₽ (user {b.user_id})")
    await show_screen(update, context, "\n".join(lines), kb_back_to_menu())

# =========================
# Callback handler (INLINE)
# =========================
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    data = q.data or ""
    await q.answer()

    uid = update.effective_user.id

    if data == "NOOP":
        return

    if data == "MENU":
        context.user_data["stage"] = ST_NONE
        context.user_data.pop("draft", None)
        await show_screen(update, context, text_menu(), None)
        return

    if data == "BOOK":
        await open_book(update, context)
        return

    if data == "CONTACTS":
        rows = []
        if ADMIN_CONTACT:
            rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
        if YANDEX_MAP_URL:
            rows.append([InlineKeyboardButton("🗺 Яндекс.Карты", url=YANDEX_MAP_URL)])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="MENU")])
        await show_screen(update, context, text_contacts(), InlineKeyboardMarkup(rows))
        return

    # services pagination
    if data.startswith("SVC_PAGE:"):
        page = int(data.split(":", 1)[1])
        await show_screen(update, context, "💅 <b>Наши услуги</b>", kb_services_page(page))
        return

    # pick service
    if data.startswith("SVC:"):
        key = data.split(":", 1)[1]
        if key not in SERVICES:
            await show_screen(update, context, "⚠️ Услуга не найдена.", kb_back_to_menu())
            return

        u = store.get_user(uid)
        if not u:
            context.user_data["stage"] = ST_REG_NAME
            await show_screen(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None)
            return

        title, price = SERVICES[key]
        draft = context.user_data.get("draft")
        if not draft:
            await open_book(update, context)
            draft = context.user_data.get("draft")

        draft["service_key"] = key
        draft["service_title"] = title
        draft["price"] = int(price)
        context.user_data["draft"] = draft

        today = now_local().date()
        await show_screen(
            update, context,
            f"✅ Услуга выбрана:\n<b>{h(title)}</b> — <b>{price}₽</b>\n\nТеперь выберите дату в календаре 👇",
            kb_calendar(today.year, today.month)
        )
        return

    # calendar month navigation
    if data == "CAL_TODAY":
        t = now_local().date()
        await show_screen(update, context, "📅 Выберите дату:", kb_calendar(t.year, t.month))
        return

    if data.startswith("CAL:"):
        _, y, m = data.split(":")
        y, m = int(y), int(m)
        await show_screen(update, context, "📅 Выберите дату:", kb_calendar(y, m))
        return

    # pick day
    if data.startswith("DAY:"):
        day_iso = data.split(":", 1)[1]
        draft = context.user_data.get("draft")
        if not draft or not draft.get("service_key"):
            await show_screen(update, context, "Сначала выберите услугу в разделе <b>💅 Услуги</b>.", kb_back_to_menu())
            return

        draft["day"] = day_iso
        context.user_data["draft"] = draft
        await show_screen(update, context, f"🕒 Выберите время на <b>{fmt_date_ru(date.fromisoformat(day_iso))}</b>:", kb_times(day_iso))
        return

    # pick time
    if data.startswith("TIME:"):
        hhmm = data.split(":", 1)[1]
        draft = context.user_data.get("draft")
        if not draft or not draft.get("day") or not draft.get("service_key"):
            await show_screen(update, context, "Сценарий устарел. Нажмите <b>📅 Записаться</b> заново.", kb_back_to_menu())
            return
        if not time_allowed(draft["day"], hhmm):
            await show_screen(update, context, "Это время уже недоступно. Выберите другое:", kb_times(draft["day"]))
            return

        draft["time"] = hhmm
        context.user_data["draft"] = draft
        context.user_data["stage"] = ST_BOOK_COMMENT

        await show_screen(
            update, context,
            "📝 Комментарий (необязательно)\n\nНапишите пожелания одним сообщением или отправьте «-» без комментария.",
            InlineKeyboardMarkup([[InlineKeyboardButton("Без комментария", callback_data="NO_COMMENT")],
                                 [InlineKeyboardButton("⬅️ Назад", callback_data="BOOK")]])
        )
        return

    if data == "NO_COMMENT":
        draft = context.user_data.get("draft") or {}
        draft["comment"] = ""
        context.user_data["draft"] = draft
        context.user_data["stage"] = ST_NONE

        if not draft.get("service_title") or not draft.get("day") or not draft.get("time"):
            await show_screen(update, context, "Сценарий устарел. Нажмите <b>📅 Записаться</b> заново.", kb_back_to_menu())
            return

        confirm_text = (
            "✅ <b>Проверьте запись</b>\n\n"
            f"• Услуга: <b>{h(draft['service_title'])}</b>\n"
            f"• Цена: <b>{draft['price']}₽</b>\n"
            f"• Дата/время: <b>{fmt_dt_ru(draft['day'], draft['time'])}</b>\n"
            f"• Комментарий: <b>—</b>\n\n"
            "Нажмите «✅ Подтвердить» — и заявка уйдёт мастеру."
        )
        await show_screen(update, context, confirm_text, InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data="CONFIRM")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="BOOK")],
        ]))
        return

    if data == "CONFIRM":
        u = store.get_user(uid)
        if not u:
            context.user_data["stage"] = ST_REG_NAME
            await show_screen(update, context, "📝 <b>Регистрация</b>\n\nКак к вам обращаться? (имя)", None)
            return

        draft = context.user_data.get("draft") or {}
        if not draft.get("service_key") or not draft.get("day") or not draft.get("time"):
            await show_screen(update, context, "Сценарий устарел. Нажмите <b>📅 Записаться</b> заново.", kb_back_to_menu())
            return

        ok, bid, msg = store.create_booking(
            uid,
            draft["service_key"],
            draft["service_title"],
            int(draft["price"]),
            draft["day"],
            draft["time"],
            draft.get("comment", "")
        )
        if not ok:
            await show_screen(update, context, f"😕 {h(msg)}\nВыберите другое время:", kb_times(draft["day"]))
            return

        # клиент
        await show_screen(
            update, context,
            "🎉 <b>Заявка отправлена!</b>\n\n"
            f"• {fmt_dt_ru(draft['day'], draft['time'])}\n"
            f"• {h(draft['service_title'])} — <b>{draft['price']}₽</b>\n\n"
            "⏳ <b>Ожидайте подтверждения от мастера</b>.\n"
            "Как только подтвердим — я напишу 🙂",
            kb_back_to_menu()
        )

        # админ
        try:
            client_url = user_link(u.tg_id, u.username)
            admin_text = (
                "🆕 <b>Новая запись</b>\n\n"
                f"👤 Клиент: <b>{h(u.full_name)}</b>\n"
                f"📞 Телефон: <b>{h(u.phone)}</b>\n"
                f"🔗 Telegram: {h(client_url)}\n\n"
                f"💅 Услуга: <b>{h(draft['service_title'])}</b>\n"
                f"💳 Цена: <b>{draft['price']}₽</b>\n"
                f"🗓 Дата/время: <b>{fmt_dt_ru(draft['day'], draft['time'])}</b>\n"
                f"📝 Комментарий: <b>{h(draft.get('comment') or '—')}</b>\n"
            )
            await context.bot.send_message(
                ADMIN_ID,
                admin_text,
                parse_mode="HTML",
                reply_markup=kb_admin_booking(int(bid), client_url, u.tg_id),
                disable_web_page_preview=True
            )
        except Exception as e:
            log.exception("Send to admin failed: %s", e)

        context.user_data.pop("draft", None)
        context.user_data["stage"] = ST_NONE
        return

    # отмена из "Мои записи"
    if data.startswith("CANCEL:"):
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b or b.user_id != uid or b.status not in ("pending", "confirmed"):
            await show_screen(update, context, "Эта запись уже недоступна.", kb_back_to_menu())
            return
        store.set_status(bid, "canceled")
        await show_screen(update, context, "✅ Запись отменена.", kb_back_to_menu())
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"❌ <b>Клиент отменил запись</b>\n• {fmt_dt_ru(b.book_date, b.book_time)}\n• {h(b.service_title)}\n• user_id: {b.user_id}",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    # админ confirm/cancel
    if data.startswith("ADM_OK:"):
        if not is_admin(uid):
            return
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b:
            await q.answer("Запись не найдена", show_alert=True)
            return
        store.set_status(bid, "confirmed")
        try:
            await context.bot.send_message(
                b.user_id,
                "✅ <b>Запись подтверждена!</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\nДо встречи 🙂",
                parse_mode="HTML"
            )
        except Exception:
            pass
        await q.answer("Подтверждено ✅")
        return

    if data.startswith("ADM_NO:"):
        if not is_admin(uid):
            return
        bid = int(data.split(":", 1)[1])
        b = store.get_booking(bid)
        if not b:
            await q.answer("Запись не найдена", show_alert=True)
            return
        store.set_status(bid, "canceled")
        try:
            await context.bot.send_message(
                b.user_id,
                "❌ <b>Запись отменена мастером</b>\n\n"
                f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                f"• {h(b.service_title)}\n\nНапишите, если хотите подобрать другое время 🙂",
                parse_mode="HTML"
            )
        except Exception:
            pass
        await q.answer("Отменено ❌")
        return

# =========================
# Contact handler (кнопка поделиться номером)
# =========================
async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != ST_REG_PHONE:
        return
    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await show_screen(update, context, "Номер не распознан 😕 Введите вручную или нажмите кнопку.", None)
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = ST_NONE

    await show_screen(update, context, f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 📅", kb_back_to_menu())
    await delete_user_message_later(update, context)

# =========================
# Text handler (reply + этапы)
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt = (update.message.text or "").strip()
    stage = context.user_data.get("stage", ST_NONE)

    # этап регистрации: имя
    if stage == ST_REG_NAME:
        if len(txt) < 2:
            await show_screen(update, context, "Напишите имя чуть понятнее 🙂", None)
            return
        context.user_data["reg_name"] = txt
        context.user_data["stage"] = ST_REG_PHONE
        # важно: можно и вручную, и кнопкой
        await update.message.reply_text(
            "📞 <b>Ваш номер телефона</b>\n\n"
            "Можно:\n• ввести вручную (например +79991234567)\n• или нажать кнопку «📱 Поделиться номером» 👇",
            parse_mode="HTML",
            reply_markup=kb_phone()
        )
        await show_screen(update, context, "Жду номер телефона 🙂", None)
        await delete_user_message_later(update, context)
        return

    # этап регистрации: телефон вручную
    if stage == ST_REG_PHONE:
        phone = parse_phone(txt)
        if len(re.sub(r"\D", "", phone)) < 10:
            await show_screen(update, context, "Номер не распознан 😕 Введите вручную или нажмите кнопку «📱 Поделиться номером».", None)
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = ST_NONE
        await show_screen(update, context, f"✅ Готово, <b>{h(name)}</b>! Теперь можно записаться 📅", kb_back_to_menu())
        # вернуть обычное меню (убрать keyboard request)
        await update.message.reply_text("Меню обновлено 👇", reply_markup=reply_menu(uid))
        await delete_user_message_later(update, context)
        return

    # этап комментария
    if stage == ST_BOOK_COMMENT:
        draft = context.user_data.get("draft") or {}
        if txt.strip() == "-":
            draft["comment"] = ""
        else:
            draft["comment"] = txt.strip()
        context.user_data["draft"] = draft
        context.user_data["stage"] = ST_NONE

        if not draft.get("service_title") or not draft.get("day") or not draft.get("time"):
            await show_screen(update, context, "Сценарий устарел. Нажмите <b>📅 Записаться</b> заново.", kb_back_to_menu())
            return

        comment = draft.get("comment") or "—"
        confirm_text = (
            "✅ <b>Проверьте запись</b>\n\n"
            f"• Услуга: <b>{h(draft['service_title'])}</b>\n"
            f"• Цена: <b>{draft['price']}₽</b>\n"
            f"• Дата/время: <b>{fmt_dt_ru(draft['day'], draft['time'])}</b>\n"
            f"• Комментарий: <b>{h(comment)}</b>\n\n"
            "Нажмите «✅ Подтвердить» — и заявка уйдёт мастеру."
        )
        await show_screen(update, context, confirm_text, InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data="CONFIRM")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="BOOK")],
        ]))
        await delete_user_message_later(update, context)
        return

    # этап вопроса мастеру
    if stage == ST_ASK:
        u = store.get_user(uid)
        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        msg = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n{h(link)}\n\n{h(txt)}"
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="HTML", disable_web_page_preview=True)
            await show_screen(update, context, "✅ Отправил мастеру. Мы ответим вам скоро 🙂", kb_back_to_menu())
        except Exception as e:
            log.exception("Ask send failed: %s", e)
            await show_screen(update, context, "⚠️ Не удалось отправить. Проверь ADMIN_ID и что мастер нажал /start.", kb_back_to_menu())
        context.user_data["stage"] = ST_NONE
        await delete_user_message_later(update, context)
        return

    # reply buttons
    t = norm_btn(txt)

    if "запис" in t:
        await open_book(update, context)
        await delete_user_message_later(update, context)
        return

    if "услуг" in t:
        await open_services(update, context, 0)
        await delete_user_message_later(update, context)
        return

    if "мои" in t and "запис" in t:
        await open_my_bookings(update, context)
        await delete_user_message_later(update, context)
        return

    if "обо" in t or "мастер" in t:
        await open_about(update, context)
        await delete_user_message_later(update, context)
        return

    if "вопрос" in t:
        await open_ask(update, context)
        await delete_user_message_later(update, context)
        return

    if "админ" in t:
        await open_admin(update, context)
        await delete_user_message_later(update, context)
        return

    # fallback
    await show_screen(update, context, "Нажмите кнопку внизу 👇", None)
    await delete_user_message_later(update, context)

# =========================
# Post init
# =========================
async def post_init(app: Application):
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Запуск бота"),
            BotCommand("menu", "Меню"),
        ])
    except Exception:
        pass

# =========================
# Main
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))

    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    log.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
