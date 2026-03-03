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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("beauty-bot")

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

SALON_NAME = os.getenv("SALON_NAME", "Beauty Lounge").strip()
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
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")  # шаг слотов
AUTO_DELETE_USER_INPUT = os.getenv("AUTO_DELETE_USER_INPUT", "1").strip() == "1"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is required")

tz = ZoneInfo(TZ_NAME)

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
        c = sqlite3.connect(self.path, timeout=20)
        c.row_factory = sqlite3.Row
        return c

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
                status TEXT NOT NULL, -- pending / confirmed / cancelled
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
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_day_time ON bookings(book_date, book_time)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(user_id)")

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

    def delete_user(self, tg_id: int):
        with self._conn() as c:
            c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))
            c.execute("DELETE FROM bookings WHERE user_id=?", (tg_id,))

    # slots
    def is_slot_blocked(self, d: str, t: str) -> bool:
        with self._conn() as c:
            r = c.execute("SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=?", (d, t)).fetchone()
            return r is not None

    def block_slot(self, d: str, t: str):
        with self._conn() as c:
            c.execute("INSERT OR IGNORE INTO blocked_slots(book_date, book_time) VALUES(?,?)", (d, t))

    def unblock_slot(self, d: str, t: str):
        with self._conn() as c:
            c.execute("DELETE FROM blocked_slots WHERE book_date=? AND book_time=?", (d, t))

    # bookings
    def is_slot_taken_exact(self, d: str, t: str) -> bool:
        with self._conn() as c:
            r = c.execute("""
            SELECT 1 FROM bookings
            WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
            LIMIT 1
            """, (d, t)).fetchone()
            return r is not None

    def create_booking_atomic(
        self,
        user_id: int,
        service_key: str,
        service_title: str,
        price: int,
        book_date: str,
        book_time: str,
        comment: str,
    ) -> Tuple[bool, Optional[int], str]:
        """
        Атомарно:
        - проверяем блок
        - проверяем занято
        - вставляем
        """
        conn = self._conn()
        try:
            conn.execute("BEGIN IMMEDIATE")

            rb = conn.execute(
                "SELECT 1 FROM blocked_slots WHERE book_date=? AND book_time=? LIMIT 1",
                (book_date, book_time)
            ).fetchone()
            if rb:
                conn.rollback()
                return False, None, "Это время заблокировано."

            rt = conn.execute("""
            SELECT 1 FROM bookings
            WHERE book_date=? AND book_time=? AND status IN ('pending','confirmed')
            LIMIT 1
            """, (book_date, book_time)).fetchone()
            if rt:
                conn.rollback()
                return False, None, "Это время уже занято."

            now = datetime.utcnow().isoformat(timespec="seconds")
            cur = conn.execute("""
            INSERT INTO bookings(user_id, service_key, service_title, price, book_date, book_time, comment, status, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """, (user_id, service_key, service_title, int(price), book_date, book_time, comment or "", "pending", now))
            bid = int(cur.lastrowid)
            conn.commit()
            return True, bid, "ok"
        except Exception as e:
            conn.rollback()
            log.exception("create_booking_atomic error: %s", e)
            return False, None, "Ошибка базы данных"
        finally:
            conn.close()

    def get_booking(self, booking_id: int) -> Optional[Booking]:
        with self._conn() as c:
            r = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
            if not r:
                return None
            return Booking(
                id=r["id"], user_id=r["user_id"], service_key=r["service_key"], service_title=r["service_title"],
                price=r["price"], book_date=r["book_date"], book_time=r["book_time"], comment=r["comment"] or "",
                status=r["status"], created_at=r["created_at"]
            )

    def set_booking_status(self, booking_id: int, status: str):
        with self._conn() as c:
            c.execute("UPDATE bookings SET status=? WHERE id=?", (status, booking_id))

    def list_user_upcoming(self, user_id: int) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE user_id=? AND status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            """, (user_id,)).fetchall()
            return [Booking(
                id=r["id"], user_id=r["user_id"], service_key=r["service_key"], service_title=r["service_title"],
                price=r["price"], book_date=r["book_date"], book_time=r["book_time"], comment=r["comment"] or "",
                status=r["status"], created_at=r["created_at"]
            ) for r in rows]

    def list_next(self, limit: int = 25) -> List[Booking]:
        with self._conn() as c:
            rows = c.execute("""
            SELECT * FROM bookings
            WHERE status IN ('pending','confirmed')
            ORDER BY book_date, book_time
            LIMIT ?
            """, (int(limit),)).fetchall()
            return [Booking(
                id=r["id"], user_id=r["user_id"], service_key=r["service_key"], service_title=r["service_title"],
                price=r["price"], book_date=r["book_date"], book_time=r["book_time"], comment=r["comment"] or "",
                status=r["status"], created_at=r["created_at"]
            ) for r in rows]

store = Storage(DB_PATH)

# =========================
# Business
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
    return (
        datetime.combine(d, ws).replace(tzinfo=tz),
        datetime.combine(d, we).replace(tzinfo=tz),
    )

def is_time_allowed(day_iso: str, hhmm: str) -> Tuple[bool, str]:
    t = parse_hhmm(hhmm)
    if not t:
        return False, "Введите время в формате HH:MM"
    slot_dt = datetime.combine(date.fromisoformat(day_iso), dtime(*map(int, t.split(":")))).replace(tzinfo=tz)
    start_dt, end_dt = work_bounds_for_day(day_iso)

    if slot_dt < start_dt or slot_dt > end_dt:
        return False, f"Время в пределах {WORK_START}–{WORK_END}"
    if day_iso == now_local().date().isoformat() and slot_dt <= now_local():
        return False, "Это время уже прошло"
    if store.is_slot_blocked(day_iso, t):
        return False, "Это время заблокировано"
    if store.is_slot_taken_exact(day_iso, t):
        return False, "Это время уже занято"
    return True, t

def days_from_today(n: int = 31) -> List[str]:
    d = now_local().date()
    return [(d + timedelta(days=i)).isoformat() for i in range(n)]

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
    for k in [
        "draft_id", "book_cat", "service_key", "service_title", "service_price",
        "book_date", "book_time", "comment", "stage"
    ]:
        context.user_data.pop(k, None)

# =========================
# UI
# =========================
def main_menu_kb(uid: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👩‍🎨 Обо мне", "📍 Контакты"],
        ["👤 Профиль", "📩 Вопрос мастеру"],
        ["❌ Отменить запись", "🏠 Меню"],
    ]
    if is_admin(uid):
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
        f"\n🕘 Время работы: <b>{h(WORK_START)}–{h(WORK_END)}</b>"
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

def about_text() -> str:
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
    arr = []
    cur = start_dt
    while cur <= end_dt:
        arr.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return arr

def kb_times(did: str, day_iso: str) -> InlineKeyboardMarkup:
    rows, row = [], []
    for t in generate_time_slots(day_iso):
        ok, _ = is_time_allowed(day_iso, t)
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
        [InlineKeyboardButton("📌 Ближайшие записи", callback_data="ADM_NEXT")],
        [InlineKeyboardButton("🏠 В меню", callback_data="MENU")],
    ])

def kb_adm_confirm_cancel(booking_id: int, client_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"ADM_CONFIRM:{booking_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"ADM_CANCEL:{booking_id}")
        ],
        [InlineKeyboardButton("💬 Написать клиенту", url=client_url)]
    ])

def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    c = context.user_data.get("comment") or "—"
    return (
        "Проверьте, всё верно:\n\n"
        f"• Услуга: <b>{h(title)}</b>\n"
        f"• Цена: <b>{price} ₽</b>\n"
        f"• Дата/время: <b>{fmt_dt_ru(d, t)}</b>\n"
        f"• Комментарий: <b>{h(c)}</b>\n\n"
        "Нажмите <b>Подтвердить запись</b>"
    )

# =========================
# Anti-mess
# =========================
async def safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
        pass

async def clean_user_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if AUTO_DELETE_USER_INPUT and update.message:
        await safe_delete(context, update.effective_chat.id, update.message.message_id)

async def flow_send(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str, markup=None):
    """
    Один основной flow-message: редактируем, если можно.
    """
    flow_id = context.user_data.get("flow_msg_id")
    if flow_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=flow_id,
                text=text,
                parse_mode="HTML",
                reply_markup=markup
            )
            return
        except Exception:
            pass
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=markup
    )
    context.user_data["flow_msg_id"] = msg.message_id

# =========================
# Handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data["stage"] = STAGE_NONE

    intro = (
        f"✨ <b>{h(SALON_NAME)}</b>\n\n"
        "Я помогу записаться:\n"
        "1) услуга\n2) дата и время\n3) подтверждение ✅"
    )
    await update.message.reply_text(intro, parse_mode="HTML", reply_markup=main_menu_kb(uid))

    u = store.get_user(uid)
    if not u:
        await update.message.reply_text("Чтобы записаться, нужна регистрация (1 раз).", reply_markup=kb_start_for_new_user())
    else:
        await update.message.reply_text("Профиль найден. Можно сразу нажать 💅 Записаться")

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_draft(context)
    await update.message.reply_text("Меню 👇", reply_markup=main_menu_kb(update.effective_user.id))

async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await flow_send(update.effective_chat.id, context, "📝 Регистрация\n\nКак к вам обращаться? (имя)")
    await update.effective_chat.send_message(" ", reply_markup=ReplyKeyboardRemove())

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("stage") != STAGE_REG_PHONE:
        return
    phone = parse_phone(update.message.contact.phone_number if update.message.contact else "")
    if len(re.sub(r"\D", "", phone)) < 10:
        await update.message.reply_text("Номер не распознан, нажмите кнопку ещё раз.", reply_markup=phone_request_kb())
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)
    context.user_data["stage"] = STAGE_NONE
    await update.message.reply_text(
        f"✅ Готово, <b>{h(name)}</b>! Профиль сохранён.",
        parse_mode="HTML",
        reply_markup=main_menu_kb(uid)
    )
    await clean_user_input(update, context)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    txt = (update.message.text or "").strip()
    stage = context.user_data.get("stage", STAGE_NONE)
    u = store.get_user(uid)

    # ===== stages =====
    if stage == STAGE_REG_NAME:
        if len(txt) < 2:
            await update.message.reply_text("Введите имя корректно 🙂")
            return
        context.user_data["reg_name"] = txt
        context.user_data["stage"] = STAGE_REG_PHONE
        await update.message.reply_text("Отправьте номер кнопкой ниже:", reply_markup=phone_request_kb())
        await clean_user_input(update, context)
        return

    if stage == STAGE_REG_PHONE:
        phone = parse_phone(txt)
        if len(re.sub(r"\D", "", phone)) < 10:
            await update.message.reply_text("Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await update.message.reply_text(
            f"✅ Готово, <b>{h(name)}</b>! Профиль сохранён.",
            parse_mode="HTML",
            reply_markup=main_menu_kb(uid)
        )
        await clean_user_input(update, context)
        return

    if stage == STAGE_MANUAL_TIME:
        did = get_draft(context)
        d = context.user_data.get("book_date")
        if not did or not d:
            clear_draft(context)
            await update.message.reply_text("Сценарий устарел. Нажмите 💅 Записаться.")
            return
        ok, t = is_time_allowed(d, txt)
        if not ok:
            await update.message.reply_text(f"😕 {t}. Введите ещё раз.")
            return
        context.user_data["book_time"] = t
        context.user_data["stage"] = STAGE_BOOK_COMMENT
        await flow_send(update.effective_chat.id, context, "Комментарий? (или нажмите «Без комментария»)", kb_comment(did))
        await clean_user_input(update, context)
        return

    if stage == STAGE_BOOK_COMMENT:
        did = get_draft(context)
        if not did:
            clear_draft(context)
            await update.message.reply_text("Сценарий устарел. Нажмите 💅 Записаться.")
            return
        context.user_data["comment"] = txt
        context.user_data["stage"] = STAGE_NONE
        await flow_send(update.effective_chat.id, context, build_confirm_text(context), kb_confirm(did))
        await clean_user_input(update, context)
        return

    if stage == STAGE_ASK_MASTER:
        who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
        link = user_link(uid, update.effective_user.username or "")
        text = f"📩 <b>Вопрос мастеру</b>\nОт: <b>{h(who)}</b>\n{h(link)}\n\n{h(txt)}"
        try:
            await context.bot.send_message(ADMIN_ID, text, parse_mode="HTML")
            await update.message.reply_text("✅ Отправил мастеру.", reply_markup=main_menu_kb(uid))
        except Exception as e:
            log.exception("ask master send failed: %s", e)
            await update.message.reply_text("⚠️ Не удалось отправить мастеру.", reply_markup=main_menu_kb(uid))
        context.user_data["stage"] = STAGE_NONE
        await clean_user_input(update, context)
        return

    # ===== menu =====
    if txt in ("🏠 Меню",):
        clear_draft(context)
        await update.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))
        await clean_user_input(update, context)
        return

    if txt == "💅 Записаться":
        if not u:
            await update.message.reply_text("Сначала регистрация (1 раз).", reply_markup=kb_start_for_new_user())
            return
        did = set_draft(context)
        await update.message.reply_text(" ", reply_markup=ReplyKeyboardRemove())
        await flow_send(update.effective_chat.id, context, "Выберите категорию:", kb_service_cats(did))
        await clean_user_input(update, context)
        return

    if txt == "💳 Цены":
        await update.message.reply_text(prices_text(), parse_mode="HTML", reply_markup=main_menu_kb(uid))
        return

    if txt == "👩‍🎨 Обо мне":
        if MASTER_PHOTO:
            await update.message.reply_photo(MASTER_PHOTO, caption=about_text(), parse_mode="HTML", reply_markup=main_menu_kb(uid))
        else:
            await update.message.reply_text(about_text(), parse_mode="HTML", reply_markup=main_menu_kb(uid))
        return

    if txt == "📍 Контакты":
        await update.message.reply_text(contacts_text(), parse_mode="HTML", reply_markup=contacts_inline() or main_menu_kb(uid))
        return

    if txt == "👤 Профиль":
        if not u:
            await update.message.reply_text("Профиль не найден. Нажмите регистрацию.", reply_markup=kb_start_for_new_user())
            return
        await update.message.reply_text(
            "👤 <b>Профиль</b>\n\n"
            f"• Имя: <b>{h(u.full_name)}</b>\n"
            f"• Телефон: <b>{h(u.phone)}</b>\n\n"
            "Чтобы сбросить: <code>Сброс профиля</code>",
            parse_mode="HTML",
            reply_markup=main_menu_kb(uid)
        )
        return

    if txt.lower() in ("сброс профиля", "сброс", "сбросить профиль"):
        store.delete_user(uid)
        clear_draft(context)
        await update.message.reply_text("✅ Профиль удалён. Нажмите /start", reply_markup=main_menu_kb(uid))
        return

    if txt == "📩 Вопрос мастеру":
        if not u:
            await update.message.reply_text("Сначала регистрация.", reply_markup=kb_start_for_new_user())
            return
        context.user_data["stage"] = STAGE_ASK_MASTER
        await update.message.reply_text("Напишите вопрос одним сообщением.", reply_markup=ReplyKeyboardRemove())
        return

    if txt == "❌ Отменить запись":
        if not u:
            await update.message.reply_text("Сначала регистрация.", reply_markup=kb_start_for_new_user())
            return
        items = store.list_user_upcoming(uid)
        if not items:
            await update.message.reply_text("У вас нет активных записей.", reply_markup=main_menu_kb(uid))
            return
        did = set_draft(context)
        rows = [[InlineKeyboardButton(
            f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
            callback_data=f"UCANCEL:{did}:{b.id}"
        )] for b in items]
        rows.append([InlineKeyboardButton("🏠 В меню", callback_data="MENU")])
        await flow_send(update.effective_chat.id, context, "Выберите запись для отмены:", InlineKeyboardMarkup(rows))
        return

    if txt == "🛠 Админ-панель" and is_admin(uid):
        await flow_send(update.effective_chat.id, context, "🛠 Админ-панель", kb_admin_panel())
        return

    await update.message.reply_text("Выберите действие в меню 👇", reply_markup=main_menu_kb(uid))

# =========================
# Callbacks
# =========================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data or ""

    try:
        def split3(prefix: str):
            if not data.startswith(prefix + ":"):
                return None
            a = data.split(":", 2)
            return (a[1], a[2]) if len(a) == 3 else None

        if data == "NOOP":
            return

        if data == "MENU":
            clear_draft(context)
            await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))
            return

        if data == "REG_START":
            await reg_start(update, context)
            return

        if data == "BOOK_START":
            u = store.get_user(uid)
            if not u:
                await q.message.reply_text("Сначала регистрация.")
                await reg_start(update, context)
                return
            did = set_draft(context)
            await flow_send(q.message.chat_id, context, "Выберите категорию:", kb_service_cats(did))
            return

        if data.startswith("BACK_CATS:"):
            did = data.split(":", 1)[1]
            if must_draft(context, did):
                await flow_send(q.message.chat_id, context, "Выберите категорию:", kb_service_cats(did))
            return

        if data.startswith("BACK_SVC:"):
            did = data.split(":", 1)[1]
            if not must_draft(context, did):
                return
            cat = context.user_data.get("book_cat")
            if not cat:
                await flow_send(q.message.chat_id, context, "Выберите категорию:", kb_service_cats(did))
                return
            await flow_send(q.message.chat_id, context, "Выберите услугу:", kb_services(did, cat))
            return

        if data.startswith("BACK_DAYS:"):
            did = data.split(":", 1)[1]
            if must_draft(context, did):
                await flow_send(q.message.chat_id, context, "Выберите дату:", kb_days(did))
            return

        if data.startswith("BACK_TIMES:"):
            did = data.split(":", 1)[1]
            if not must_draft(context, did):
                return
            d = context.user_data.get("book_date")
            if d:
                await flow_send(q.message.chat_id, context, "Выберите время:", kb_times(did, d))
            return

        p = split3("CAT")
        if p:
            did, cat = p
            if not must_draft(context, did):
                return
            context.user_data["book_cat"] = cat
            await flow_send(q.message.chat_id, context, "Выберите услугу:", kb_services(did, cat))
            return

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

            await flow_send(
                q.message.chat_id,
                context,
                f"Вы выбрали:\n<b>{h(title)}</b> — <b>{price} ₽</b>\n\nВыберите дату:",
                kb_days(did)
            )
            return

        p = split3("DAY")
        if p:
            did, d = p
            if not must_draft(context, did):
                return
            context.user_data["book_date"] = d
            context.user_data.pop("book_time", None)
            await flow_send(q.message.chat_id, context, f"Дата: <b>{fmt_date_ru(d)}</b>\nВыберите время:", kb_times(did, d))
            return

        p = split3("TIME")
        if p:
            did, t = p
            if not must_draft(context, did):
                return
            d = context.user_data.get("book_date")
            if not d:
                await flow_send(q.message.chat_id, context, "Сначала выберите дату:", kb_days(did))
                return
            ok, msg = is_time_allowed(d, t)
            if not ok:
                await flow_send(q.message.chat_id, context, f"😕 {msg}\nВыберите другое время:", kb_times(did, d))
                return
            context.user_data["book_time"] = msg
            context.user_data["stage"] = STAGE_BOOK_COMMENT
            await flow_send(q.message.chat_id, context, "Комментарий? (или «Без комментария»)", kb_comment(did))
            return

        if data.startswith("MANUAL_TIME:"):
            did = data.split(":", 1)[1]
            if not must_draft(context, did):
                return
            if not context.user_data.get("book_date"):
                await flow_send(q.message.chat_id, context, "Сначала выберите дату:", kb_days(did))
                return
            context.user_data["stage"] = STAGE_MANUAL_TIME
            await flow_send(q.message.chat_id, context, "Введите время вручную (пример: 17:00)")
            return

        if data.startswith("COMMENT:"):
            parts = data.split(":", 2)
            if len(parts) != 3:
                return
            did, payload = parts[1], parts[2]
            if not must_draft(context, did):
                return
            context.user_data["comment"] = "" if payload == "-" else payload
            context.user_data["stage"] = STAGE_NONE
            await flow_send(q.message.chat_id, context, build_confirm_text(context), kb_confirm(did))
            return

        # ===== client confirm booking =====
        if data.startswith("CONFIRM:"):
            did = data.split(":", 1)[1]
            if not must_draft(context, did):
                await q.message.reply_text("Кнопки устарели, начните заново.")
                return

            u = store.get_user(uid)
            if not u:
                await q.message.reply_text("Сначала регистрация.", reply_markup=kb_start_for_new_user())
                return

            key = context.user_data.get("service_key")
            title = context.user_data.get("service_title")
            price = int(context.user_data.get("service_price", 0) or 0)
            d = context.user_data.get("book_date")
            t = context.user_data.get("book_time")
            comment = context.user_data.get("comment", "")

            if not (key and title and d and t and price > 0):
                await q.message.reply_text("Сценарий устарел, нажмите 💅 Записаться.")
                return

            # Мгновенно показываем действие пользователю
            await flow_send(q.message.chat_id, context, "⏳ Создаю запись...")

            # Финальная проверка
            ok, msg = is_time_allowed(d, t)
            if not ok:
                await flow_send(q.message.chat_id, context, f"😕 {msg}\nВыберите другое время:", kb_times(did, d))
                return

            created, booking_id, err = store.create_booking_atomic(
                user_id=u.tg_id,
                service_key=key,
                service_title=title,
                price=price,
                book_date=d,
                book_time=t,
                comment=comment
            )
            if not created or not booking_id:
                await flow_send(q.message.chat_id, context, f"😕 {h(err)}\nВыберите другое время:", kb_times(did, d))
                return

            await flow_send(
                q.message.chat_id,
                context,
                "✅ <b>Запись создана!</b>\n\n"
                f"• {fmt_dt_ru(d, t)}\n"
                f"• {h(title)}\n\n"
                "Ожидайте подтверждение мастера 🙂"
            )

            # уведомление админу
            client_url = user_link(u.tg_id, u.username)
            admin_text = (
                "🆕 <b>Новая запись</b>\n\n"
                f"• Клиент: <b>{h(u.full_name)}</b>\n"
                f"• Телефон: <b>{h(u.phone)}</b>\n"
                f"• TG: {h(client_url)}\n"
                f"• Услуга: <b>{h(title)}</b> — <b>{price} ₽</b>\n"
                f"• Дата/время: <b>{fmt_dt_ru(d, t)}</b>\n"
                f"• Комментарий: <b>{h(comment or '—')}</b>\n\n"
                f"ID: <code>{booking_id}</code>"
            )

            try:
                await context.bot.send_message(
                    ADMIN_ID,
                    admin_text,
                    parse_mode="HTML",
                    reply_markup=kb_adm_confirm_cancel(booking_id, client_url)
                )
            except Exception as e:
                log.exception("send admin notification failed: %s", e)
                await q.message.reply_text("⚠️ Запись сохранена, но админу не отправилось уведомление.")

            clear_draft(context)
            await q.message.reply_text("🏠 Меню", reply_markup=main_menu_kb(uid))
            return

        # ===== user cancel own booking =====
        if data.startswith("UCANCEL:"):
            parts = data.split(":", 2)
            if len(parts) != 3:
                return
            did, bid_s = parts[1], parts[2]
            if not must_draft(context, did):
                return
            b = store.get_booking(int(bid_s))
            if not b or b.user_id != uid:
                await q.message.reply_text("Запись не найдена.")
                return
            store.set_booking_status(b.id, "cancelled")
            await flow_send(q.message.chat_id, context, "✅ Запись отменена.")
            await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))
            return

        # ===== admin =====
        if data == "ADM_NEXT" and is_admin(uid):
            items = store.list_next(25)
            if not items:
                await flow_send(q.message.chat_id, context, "Ближайших записей нет.")
                return
            lines = ["📌 <b>Ближайшие записи</b>", ""]
            for b in items:
                uu = store.get_user(b.user_id)
                who = f"{uu.full_name} ({uu.phone})" if uu else str(b.user_id)
                lines.append(f"• <b>{fmt_dt_ru(b.book_date, b.book_time)}</b> — {h(b.service_title)} — {h(who)} (ID <code>{b.id}</code>)")
            await flow_send(q.message.chat_id, context, "\n".join(lines))
            return

        if data.startswith("ADM_CONFIRM:") and is_admin(uid):
            booking_id = int(data.split(":", 1)[1])
            b = store.get_booking(booking_id)
            if not b:
                await q.message.reply_text("Запись не найдена.")
                return
            store.set_booking_status(booking_id, "confirmed")

            # сообщение клиенту о подтверждении
            try:
                await context.bot.send_message(
                    b.user_id,
                    "✅ <b>Ваша запись подтверждена!</b>\n\n"
                    f"• Дата/время: <b>{fmt_dt_ru(b.book_date, b.book_time)}</b>\n"
                    f"• Услуга: <b>{h(b.service_title)}</b>\n\n"
                    "Ждём вас 💅",
                    parse_mode="HTML",
                    reply_markup=main_menu_kb(b.user_id)
                )
            except Exception as e:
                log.warning("notify client confirm failed: %s", e)

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
                    "❌ <b>К сожалению, запись отменена мастером.</b>\n"
                    "Напишите снова, подберём другое время 🙂",
                    parse_mode="HTML",
                    reply_markup=main_menu_kb(b.user_id)
                )
            except Exception:
                pass
            await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
            return

    except Exception as e:
        log.exception("callback error: %s", e)
        await q.message.reply_text("⚠️ Произошла ошибка. Нажмите /start")


# =========================
# Error / app
# =========================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
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
