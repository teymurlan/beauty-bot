import os
import re
import logging
from datetime import datetime, timedelta, date
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

from storage import Storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("beauty-bot")

# ===== ENV =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
SALON_NAME = os.getenv("SALON_NAME", "Beauty Lounge").strip()
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()  # https://t.me/username
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

ADDRESS = os.getenv("ADDRESS", "Адрес: (впишите адрес)").strip()
HOW_TO_FIND = os.getenv("HOW_TO_FIND", "Как нас найти: (впишите ориентиры/этаж/домофон)").strip()
MAP_URL = os.getenv("MAP_URL", "").strip()  # ссылка на карты (не обязательно)

# Рабочее время (можешь менять)
WORK_START = os.getenv("WORK_START", "10:00").strip()
WORK_END = os.getenv("WORK_END", "20:00").strip()
SLOT_MINUTES = int(os.getenv("SLOT_MINUTES", "60").strip() or "60")

if not BOT_TOKEN:
    raise RuntimeError("ENV BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ENV ADMIN_ID is required")

store = Storage(DB_PATH)
tz = ZoneInfo(TZ_NAME)

# ===== Services =====
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

# ===== user flow states in context.user_data =====
STAGE_NONE = "none"
STAGE_REG_NAME = "reg_name"
STAGE_REG_PHONE = "reg_phone"
STAGE_BOOK_COMMENT = "book_comment"

# ===== helpers =====
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

def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["🔥 Акции", "⭐ Отзывы"],
        ["🖼 Фотогалерея", "📍 Контакты"],
        ["👤 Профиль", "❌ Отменить запись"],
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

def about_text() -> str:
    # улучшенная версия “что может делать этот бот” из твоих скринов
    return (
        f"✨ *{SALON_NAME}*\n\n"
        "Это чат-бот для записи на услуги маникюра и педикюра 💅🦶\n\n"
        "Что я умею:\n"
        "• Показать цены\n"
        "• Записать на удобную дату и свободное время\n"
        "• Отправить заявку администратору\n"
        "• Напомнить о визите за 24 часа\n"
        "• Дать контакты и как нас найти\n\n"
        "Нажмите *💅 Записаться* — и выберите услугу 🙂"
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
    t = "📍 *Контакты*\n\n"
    t += f"• {ADDRESS}\n"
    t += f"• {HOW_TO_FIND}\n"
    t += "• Время работы: 10:00–20:00\n"
    return t

def contacts_inline() -> InlineKeyboardMarkup | None:
    rows = []
    if ADMIN_CONTACT:
        rows.append([InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)])
    if MAP_URL:
        rows.append([InlineKeyboardButton("🗺 Открыть карту", url=MAP_URL)])
    return InlineKeyboardMarkup(rows) if rows else None

# ===== calendar: today -> end of month =====
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
    d = date.fromisoformat(day_iso)
    start_dt = datetime.combine(d, datetime.strptime(WORK_START, "%H:%M").time(), tzinfo=tz)
    end_dt = datetime.combine(d, datetime.strptime(WORK_END, "%H:%M").time(), tzinfo=tz)
    slots = []
    cur = start_dt
    while cur + timedelta(minutes=SLOT_MINUTES) <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots

def build_times_kb(day_iso: str, mode: str) -> InlineKeyboardMarkup:
    # mode: client / adm_block / adm_unblock
    slots = generate_time_slots(day_iso)

    if mode == "adm_unblock":
        blocked = set(store.list_blocked_for_day(day_iso))
        shown = [t for t in slots if t in blocked]
        if not shown:
            return InlineKeyboardMarkup([
                [InlineKeyboardButton("😕 Нет заблокированных слотов", callback_data="noop")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")],
            ])
        rows, row = [], []
        for t in shown:
            row.append(InlineKeyboardButton(t, callback_data=f"adm_unblock_time:{day_iso}|{t}"))
            if len(row) == 4:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")])
        return InlineKeyboardMarkup(rows)

    rows, row = [], []
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

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data="noop")]]

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

# ===== reminders =====
def booking_start_dt(iso_date: str, hhmm: str) -> datetime:
    d = date.fromisoformat(iso_date)
    t = datetime.strptime(hhmm, "%H:%M").time()
    return datetime.combine(d, t, tzinfo=tz)

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    booking_id = context.job.data.get("booking_id")
    b = store.get_booking(int(booking_id))
    if not b or b.reminder_sent == 1 or b.status not in ("pending", "confirmed"):
        return

    # send reminder
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
        # если меньше 24ч — не спамим, но можно отправить за 2ч (не просили)
        return

    delay = (remind_at - now).total_seconds()
    name = f"rem_{booking_id}"
    # убрать возможный дубль
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

# ====== start / registration ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    context.user_data.setdefault("stage", STAGE_NONE)

    await update.message.reply_text(
        f"Приветствуем в *{SALON_NAME}* 😊\n\n"
        "Запись на маникюр и педикюр — быстро и удобно.\n"
        "Нажмите *💅 Записаться* и выберите услугу.",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(uid)
    )

    u = store.get_user(uid)
    if not u:
        context.user_data["stage"] = STAGE_REG_NAME
        await update.message.reply_text("Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")
        return

    # зарегистрирован
    await update.message.reply_text("Хотите записаться прямо сейчас? 👇", reply_markup=after_reg_inline())

async def begin_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["stage"] = STAGE_REG_NAME
    await update.message.reply_text("Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")

# ===== text / contact router (без багов) =====
async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stage = context.user_data.get("stage", STAGE_NONE)
    if stage != STAGE_REG_PHONE:
        return

    phone = update.message.contact.phone_number if update.message.contact else ""
    phone = parse_phone(phone)
    if len(re.sub(r"\D", "", phone)) < 10:
        await update.message.reply_text("Не вижу номер 😕 Нажмите «📱 Отправить номер».", reply_markup=phone_request_kb())
        return

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
    store.upsert_user(uid, update.effective_user.username or "", name, phone)

    context.user_data["stage"] = STAGE_NONE
    await update.message.reply_text(f"✅ Отлично, *{name}*!\n\nТеперь можно записаться 👇", parse_mode="Markdown", reply_markup=main_menu_kb(uid))
    await update.message.reply_text("Нажмите кнопку:", reply_markup=after_reg_inline())

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    stage = context.user_data.get("stage", STAGE_NONE)

    # 1) REG NAME
    if stage == STAGE_REG_NAME:
        if len(text) < 2:
            await update.message.reply_text("Напишите имя чуть понятнее 🙂")
            return
        context.user_data["reg_name"] = text
        context.user_data["stage"] = STAGE_REG_PHONE
        await update.message.reply_text("Отправьте номер телефона кнопкой ниже:", reply_markup=phone_request_kb())
        return

    # 2) REG PHONE (если вдруг не контакт, а текст)
    if stage == STAGE_REG_PHONE:
        phone = parse_phone(text)
        if len(re.sub(r"\D", "", phone)) < 10:
            await update.message.reply_text("Нажмите «📱 Отправить номер» (так без ошибок).", reply_markup=phone_request_kb())
            return
        name = context.user_data.get("reg_name", update.effective_user.full_name or "Клиент")
        store.upsert_user(uid, update.effective_user.username or "", name, phone)
        context.user_data["stage"] = STAGE_NONE
        await update.message.reply_text(f"✅ Отлично, *{name}*!\n\nТеперь можно записаться 👇", parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        await update.message.reply_text("Нажмите кнопку:", reply_markup=after_reg_inline())
        return

    # 3) BOOK COMMENT (если клиент пишет комментарий)
    if stage == STAGE_BOOK_COMMENT:
        context.user_data["comment"] = "" if text == "-" else text
        context.user_data["stage"] = STAGE_NONE
        # покажем подтверждение
        await update.message.reply_text(
            build_confirm_text(context),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
                 InlineKeyboardButton("В меню", callback_data="back_to_menu")]
            ])
        )
        return

    # ----- MENU BUTTONS -----
    # Если не зарегистрирован и нажал что-то кроме /start — мягко отправим на регистрацию
    u = store.get_user(uid)

    if text == "💅 Записаться":
        if not u:
            await begin_registration(update, context)
            return
        await update.message.reply_text("Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    if text == "💳 Цены":
        await update.message.reply_text(prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        return

    if text == "🔥 Акции":
        await update.message.reply_text(
            "🔥 *Акции*\n\n"
            "• Уточняйте актуальные предложения у администратора\n"
            "• Можно записаться и в комментарии указать пожелания 🙂",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(uid)
        )
        return

    if text == "⭐ Отзывы":
        await update.message.reply_text(
            "⭐ *Отзывы*\n\n"
            "Хотите — пришлём подборку отзывов.\n"
            "Напишите одним сообщением: «хочу отзывы».",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(uid)
        )
        return

    if text == "🖼 Фотогалерея":
        await update.message.reply_text(
            "🖼 *Фотогалерея*\n\n"
            "Сюда можно добавить ссылку на альбом/Instagram.\n"
            "Напишите: «хочу фото работ» — и админ пришлёт подборку.",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(uid)
        )
        return

    if text == "📍 Контакты":
        kb = contacts_inline()
        await update.message.reply_text(contacts_text(), parse_mode="Markdown", reply_markup=kb or main_menu_kb(uid))
        return

    if text == "👤 Профиль":
        if not u:
            await begin_registration(update, context)
            return
        link = user_link(u.tg_id, u.username)
        await update.message.reply_text(
            "👤 *Профиль*\n\n"
            f"• Имя: *{u.full_name}*\n"
            f"• Телефон: *{u.phone}*\n"
            f"• Telegram: {link}\n\n"
            "Если хотите изменить данные — напишите: `Сменить профиль`",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(uid)
        )
        return

    if text.lower() in ["сменить профиль", "сброс", "сбросить профиль"]:
        store.delete_user(uid)
        await update.message.reply_text("✅ Профиль сброшен. Нажмите /start для новой регистрации.", reply_markup=main_menu_kb(uid))
        return

    if text == "❌ Отменить запись":
        if not u:
            await begin_registration(update, context)
            return
        upcoming = store.list_user_upcoming(uid)
        if not upcoming:
            await update.message.reply_text("У вас нет активных записей 🙂", reply_markup=main_menu_kb(uid))
            return
        rows = []
        for b in upcoming:
            rows.append([InlineKeyboardButton(f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}", callback_data=f"ucancel:{b.id}")])
        rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
        await update.message.reply_text("Выберите запись для отмены:", reply_markup=InlineKeyboardMarkup(rows))
        return

    if text == "🛠 Админ-панель" and is_admin(uid):
        await update.message.reply_text("🛠 *Админ-панель*", parse_mode="Markdown", reply_markup=admin_panel_inline())
        return

    # ----- OTHER TEXT = question to admin -----
    # (теперь это НЕ сработает во время регистрации — мы это уже исправили)
    who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
    link = user_link(uid, update.effective_user.username or "")
    msg = f"❓ *Вопрос из бота*\nОт: *{who}*\n{link}\n\n{text}"
    try:
        await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except Exception:
        pass
    await update.message.reply_text("Спасибо! Я передал ваше сообщение администратору ✅", reply_markup=main_menu_kb(uid))

# ===== booking confirm text =====
def build_confirm_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"
    return (
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{fmt_dt_ru(d, t)}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и заявка уйдёт администратору ✅"
    )

# ===== callbacks =====
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data

    if data == "noop":
        return

    if data == "back_to_menu":
        await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))
        return

    if data == "go_prices":
        await q.message.reply_text(prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(uid))
        return

    if data == "go_book":
        u = store.get_user(uid)
        if not u:
            # если по инлайн нажали без регистрации
            context.user_data["stage"] = STAGE_REG_NAME
            await q.message.reply_text("Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")
            return
        await q.message.reply_text("Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    # booking flow
    if data == "back_to_service_cats":
        await q.edit_message_text("Выберите категорию услуги:", reply_markup=service_cats_kb())
        return

    if data.startswith("cat:"):
        cat = data.split(":", 1)[1]
        context.user_data["book_cat"] = cat
        await q.edit_message_text("Выберите услугу:", reply_markup=services_list_kb(cat))
        return

    if data.startswith("svc:"):
        key = data.split(":", 1)[1]
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)

        await q.edit_message_text(
            f"Вы выбрали:\n*{title}* — *{price} ₽*\n\nВыберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(prefix="day")
        )
        return

    if data == "back_to_days":
        await q.edit_message_text("Выберите дату:", reply_markup=build_days_kb(prefix="day"))
        return

    if data.startswith("day:"):
        day_iso = data.split(":", 1)[1]
        context.user_data["book_date"] = day_iso
        await q.edit_message_text(
            f"Дата: *{fmt_date_ru(day_iso)}*\nВыберите время (свободные слоты):",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="client")
        )
        return

    if data == "back_to_times":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.message.reply_text("Выберите дату заново 🙂", reply_markup=build_days_kb(prefix="day"))
            return
        await q.edit_message_text("Выберите время:", reply_markup=build_times_kb(day_iso, mode="client"))
        return

    if data.startswith("time:"):
        t = data.split(":", 1)[1]
        context.user_data["book_time"] = t
        context.user_data["stage"] = STAGE_BOOK_COMMENT

        await q.edit_message_text(
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
        await q.edit_message_text(
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
            context.user_data["stage"] = STAGE_REG_NAME
            await q.message.reply_text("Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")
            return

        key = context.user_data.get("service_key")
        title = context.user_data.get("service_title")
        price = int(context.user_data.get("service_price", 0))
        d = context.user_data.get("book_date")
        t = context.user_data.get("book_time")
        comment = context.user_data.get("comment", "")

        if not all([key, title, d, t]) or price <= 0:
            await q.message.reply_text("Ошибка данных. Нажмите 💅 Записаться ещё раз.", reply_markup=main_menu_kb(uid))
            return

        # final slot check
        if store.is_slot_blocked(d, t) or store.is_slot_taken(d, t):
            await q.message.reply_text("Этот слот уже занят 😕 Выберите другое время.", reply_markup=main_menu_kb(uid))
            return

        booking_id = store.create_booking(u.tg_id, key, title, price, d, t, comment)

        # schedule reminder 24h
        schedule_reminder(context.application, booking_id, d, t)

        # client
        await q.edit_message_text(
            "✅ *Заявка отправлена администратору!*\n\n"
            f"• {fmt_dt_ru(d, t)}\n"
            f"• {title}\n\n"
            "Мы подтвердим запись и пришлём уведомление 🙂",
            parse_mode="Markdown"
        )
        await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(uid))

        # admin notify
        link = user_link(u.tg_id, u.username)
        admin_text = (
            "🆕 *Новая запись*\n\n"
            f"• Клиент: *{u.full_name}*\n"
            f"• Телефон: *{u.phone}*\n"
            f"• TG: {link}\n"
            f"• Услуга: *{title}* — *{price} ₽*\n"
            f"• Дата/время: *{fmt_dt_ru(d, t)}*\n"
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

        # clear temp
        for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat"]:
            context.user_data.pop(k, None)

        return

    # user cancel
    if data.startswith("ucancel:"):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b or b.user_id != uid:
            await q.message.reply_text("Запись не найдена.", reply_markup=main_menu_kb(uid))
            return

        store.set_booking_status(booking_id, "cancelled")
        await q.edit_message_text("✅ Запись отменена. Если нужно — запишитесь заново через меню.")

        u = store.get_user(uid)
        if u:
            link = user_link(u.tg_id, u.username)
            msg = (
                "⚠️ *Клиент отменил запись*\n\n"
                f"• Клиент: *{u.full_name}* ({u.phone})\n"
                f"• Запись: *{fmt_dt_ru(b.book_date, b.book_time)}*\n"
                f"• Услуга: *{b.service_title}*\n"
                f"ID: `{b.id}`"
            )
            try:
                await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown",
                                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать клиенту", url=link)]]))
            except Exception:
                pass
        return

    # admin panel
    if data == "adm_today" and is_admin(uid):
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await q.message.reply_text("На сегодня записей нет 🙂")
            return
        lines = [f"📅 *Записи на сегодня* ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm_next" and is_admin(uid):
        items = store.list_next(25)
        if not items:
            await q.message.reply_text("Ближайших записей нет 🙂")
            return
        lines = ["⏭ *Ближайшие записи*", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{fmt_dt_ru(b.book_date, b.book_time)}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm_block" and is_admin(uid):
        context.user_data["adm_mode"] = "block"
        await q.message.reply_text("⛔ *Блокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                   reply_markup=build_days_kb(prefix="adm_block_day"))
        return

    if data == "adm_unblock" and is_admin(uid):
        context.user_data["adm_mode"] = "unblock"
        await q.message.reply_text("✅ *Разблокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                   reply_markup=build_days_kb(prefix="adm_unblock_day"))
        return

    if data == "adm_back_days" and is_admin(uid):
        mode = context.user_data.get("adm_mode", "block")
        if mode == "block":
            await q.message.reply_text("⛔ *Блокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                       reply_markup=build_days_kb(prefix="adm_block_day"))
        else:
            await q.message.reply_text("✅ *Разблокировка слота*\nВыберите дату:", parse_mode="Markdown",
                                       reply_markup=build_days_kb(prefix="adm_unblock_day"))
        return

    if data.startswith("adm_block_day:") and is_admin(uid):
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "block"
        await q.message.reply_text(
            f"⛔ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для блокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_block")
        )
        return

    if data.startswith("adm_block_time:") and is_admin(uid):
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)
        if store.is_slot_taken(day_iso, t):
            await q.message.reply_text("Этот слот уже занят записью. Нельзя заблокировать.")
            return
        store.block_slot(day_iso, t)
        await q.message.reply_text(f"✅ Заблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return

    if data.startswith("adm_unblock_day:") and is_admin(uid):
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "unblock"
        await q.message.reply_text(
            f"✅ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для разблокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_unblock")
        )
        return

    if data.startswith("adm_unblock_time:") and is_admin(uid):
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)
        store.unblock_slot(day_iso, t)
        await q.message.reply_text(f"✅ Разблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return

    if data.startswith("adm_confirm:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
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
                reply_markup=contacts_inline() or main_menu_kb(b.user_id)
            )
        except Exception:
            pass
        await q.message.reply_text(f"✅ Подтверждено (ID {booking_id})")
        return

    if data.startswith("adm_cancel:") and is_admin(uid):
        booking_id = int(data.split(":", 1)[1])
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
                reply_markup=contacts_inline() or main_menu_kb(b.user_id)
            )
        except Exception:
            pass
        await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
        return

# ===== startup hook =====
async def on_startup(app: Application):
    # восстановим напоминания после рестарта Railway
    reschedule_all_reminders(app)
    log.info("Reminders rescheduled.")

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
