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
    ConversationHandler,
    ContextTypes,
    filters,
)

from storage import Storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("beauty-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
SALON_NAME = os.getenv("SALON_NAME", "Beauty Lounge").strip()
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()  # https://t.me/username
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()

if not BOT_TOKEN:
    raise RuntimeError("ENV BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ENV ADMIN_ID is required")

store = Storage(DB_PATH)
tz = ZoneInfo(TZ_NAME)

# ====== Услуги/цены (из твоих скринов) ======
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

# ====== Стейты ======
REG_NAME, REG_PHONE, BOOK_FLOW, BOOK_COMMENT = range(4)

# ========= helpers =========
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

def main_menu_kb(for_admin: bool) -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["👤 Обо мне", "📍 Контакты"],
        ["❌ Отменить запись"],
    ]
    if for_admin:
        kb.append(["🛠 Админ-панель"])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def about_text() -> str:
    return (
        f"✨ *{SALON_NAME}*\n\n"
        "Добро пожаловать в наш уютный салон 💅🦶\n"
        "Здесь вы можете быстро записаться на:\n"
        "• маникюр (классика / покрытие / дизайн)\n"
        "• педикюр (разные варианты)\n"
        "• наращивание / коррекцию / дизайн\n\n"
        "Я помогу вам выбрать удобное время и оформить запись в пару кликов.\n"
        "Нажмите *💅 Записаться* — и увидите свободные окна ✅"
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

# ====== календарь: сегодня → конец текущего месяца ======
def month_days_from_today() -> list[str]:
    today = now_local().date()
    # последний день месяца:
    first_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    last_day = first_next_month - timedelta(days=1)

    days = []
    d = today
    while d <= last_day:
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days

def build_days_kb(prefix: str = "day") -> InlineKeyboardMarkup:
    days = month_days_from_today()
    rows = []
    row = []
    for iso in days:
        label = fmt_date_ru(iso)
        row.append(InlineKeyboardButton(label, callback_data=f"{prefix}:{iso}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)

# ====== рабочие часы / интервалы (можешь поменять тут) ======
WORK_START = "10:00"
WORK_END = "20:00"
SLOT_MINUTES = 60

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

def build_times_kb(day_iso: str, mode: str = "client") -> InlineKeyboardMarkup:
    # mode: client / adm_block / adm_unblock
    slots = generate_time_slots(day_iso)

    rows = []
    row = []

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
        rows.append([
            InlineKeyboardButton("⬅️ Назад", callback_data="back_to_days"),
            InlineKeyboardButton("В меню", callback_data="back_to_menu"),
        ])
    else:
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm_back_days")])

    return InlineKeyboardMarkup(rows)

# ====== услуги UI ======
def service_cats_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Маникюр", callback_data="cat:mn")],
        [InlineKeyboardButton("🦶 Педикюр", callback_data="cat:pd")],
        [InlineKeyboardButton("🌟 Дополнительно", callback_data="cat:extra")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")],
    ])

def services_by_cat(cat: str) -> list[tuple[str, str, int]]:
    if cat == "mn":
        keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd":
        keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else:
        keys = ["ext", "corr", "design"]
    return [(k, SERVICES[k][0], SERVICES[k][1]) for k in keys]

def services_list_kb(cat: str) -> InlineKeyboardMarkup:
    rows = []
    for k, title, price in services_by_cat(cat):
        rows.append([InlineKeyboardButton(f"{title} — {price} ₽", callback_data=f"svc:{k}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_service_cats")])
    rows.append([InlineKeyboardButton("В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)

def after_reg_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💅 Записаться", callback_data="go_book")],
        [InlineKeyboardButton("💳 Посмотреть цены", callback_data="go_prices")],
    ])

# ====== Admin UI ======
def admin_panel_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записи на сегодня", callback_data="adm_today"),
         InlineKeyboardButton("⏭ Ближайшие", callback_data="adm_next")],
        [InlineKeyboardButton("⛔ Заблокировать слот", callback_data="adm_block"),
         InlineKeyboardButton("✅ Разблокировать слот", callback_data="adm_unblock")],
    ])

# ================== START / REG ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    u = store.get_user(user.id)

    await update.message.reply_text(
        f"Приветствуем в *{SALON_NAME}* 😊\n\n"
        "Я — бот для удобной записи на маникюр/педикюр.\n"
        "Выберите услугу и свободное время — и заявка уйдёт администратору ✅",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(is_admin(user.id))
    )

    if not u:
        await update.message.reply_text(
            "Как я могу к вам обращаться? (имя)",
            reply_markup=ReplyKeyboardMarkup([["⬅️ В меню"]], resize_keyboard=True, one_time_keyboard=True)
        )
        return REG_NAME

    # если уже зарегистрирован — сразу даём быстрый старт
    await update.message.reply_text(
        "Хотите записаться прямо сейчас? 👇",
        reply_markup=after_reg_inline()
    )
    return ConversationHandler.END

async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip() == "⬅️ В меню":
        await update.message.reply_text("Ок 🙂", reply_markup=main_menu_kb(is_admin(update.effective_user.id)))
        return ConversationHandler.END

    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("Напишите имя чуть понятнее 🙂")
        return REG_NAME

    context.user_data["reg_name"] = name
    await update.message.reply_text(
        "Отправьте номер телефона кнопкой ниже:",
        reply_markup=phone_request_kb()
    )
    return REG_PHONE

async def reg_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = ""
    if update.message.contact and update.message.contact.phone_number:
        phone = update.message.contact.phone_number
    else:
        phone = update.message.text or ""

    phone = parse_phone(phone)
    if len(re.sub(r"\D", "", phone)) < 10:
        await update.message.reply_text("Не вижу номер 😕 Отправьте ещё раз кнопкой «📱 Отправить номер».", reply_markup=phone_request_kb())
        return REG_PHONE

    user = update.effective_user
    full_name = context.user_data.get("reg_name", user.full_name or "Клиент")
    store.upsert_user(user.id, user.username or "", full_name, phone)

    await update.message.reply_text(
        f"✅ Отлично, *{full_name}*!\n\n"
        "Теперь можно записаться на услугу 👇",
        parse_mode="Markdown",
        reply_markup=main_menu_kb(is_admin(user.id))
    )
    await update.message.reply_text(
        "Нажмите кнопку ниже:",
        reply_markup=after_reg_inline()
    )
    return ConversationHandler.END

# ================== MENU (reply кнопки) ==================
async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    admin = is_admin(uid)

    if text == "💅 Записаться":
        return await open_booking(update, context)

    if text == "💳 Цены":
        await update.message.reply_text(prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(admin))
        return ConversationHandler.END

    if text == "👤 Обо мне":
        await update.message.reply_text(about_text(), parse_mode="Markdown", reply_markup=main_menu_kb(admin))
        return ConversationHandler.END

    if text == "📍 Контакты":
        kb = None
        if ADMIN_CONTACT:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)]])
        await update.message.reply_text(
            "📍 *Контакты*\n\n"
            "• Адрес: (впишите адрес)\n"
            "• Время работы: 10:00–20:00\n"
            "• Запись: через *💅 Записаться*",
            parse_mode="Markdown",
            reply_markup=kb or main_menu_kb(admin)
        )
        return ConversationHandler.END

    if text == "❌ Отменить запись":
        u = store.get_user(uid)
        if not u:
            await update.message.reply_text("Сначала /start 🙂", reply_markup=main_menu_kb(admin))
            return ConversationHandler.END
        upcoming = store.list_user_upcoming(uid)
        if not upcoming:
            await update.message.reply_text("У вас нет активных записей 🙂", reply_markup=main_menu_kb(admin))
            return ConversationHandler.END

        rows = []
        for b in upcoming:
            rows.append([InlineKeyboardButton(
                f"❌ {fmt_dt_ru(b.book_date, b.book_time)} — {b.service_title}",
                callback_data=f"ucancel:{b.id}"
            )])
        rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
        await update.message.reply_text("Выберите запись для отмены:", reply_markup=InlineKeyboardMarkup(rows))
        return ConversationHandler.END

    if text == "🛠 Админ-панель" and admin:
        await update.message.reply_text("🛠 *Админ-панель*", parse_mode="Markdown", reply_markup=admin_panel_inline())
        return ConversationHandler.END

    # Любой другой текст — как вопрос админу
    u = store.get_user(uid)
    who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
    link = user_link(uid, update.effective_user.username or "")
    msg = f"❓ *Вопрос из бота*\nОт: *{who}*\n{link}\n\n{update.message.text}"

    try:
        await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except Exception:
        pass

    await update.message.reply_text("Спасибо! Я передал ваш вопрос администратору ✅", reply_markup=main_menu_kb(admin))
    return ConversationHandler.END

# ================== BOOKING OPEN ==================
async def open_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = store.get_user(uid)
    if not u:
        await update.message.reply_text("Сначала регистрация 🙂\nКак к вам обращаться? (имя)")
        return REG_NAME

    await update.message.reply_text(
        "Выберите категорию услуги:",
        reply_markup=service_cats_kb()
    )
    return BOOK_FLOW

# ================== CALLBACKS ==================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    uid = q.from_user.id
    admin = is_admin(uid)

    # общие
    if data == "noop":
        return

    if data == "back_to_menu":
        await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(admin))
        return ConversationHandler.END

    if data == "go_prices":
        await q.message.reply_text(prices_text(), parse_mode="Markdown", reply_markup=main_menu_kb(admin))
        return ConversationHandler.END

    if data == "go_book":
        # старт записи инлайном после регистрации
        await q.message.reply_text("Выберите категорию услуги:", reply_markup=service_cats_kb())
        return BOOK_FLOW

    # ===== booking flow =====
    if data == "back_to_service_cats":
        await q.edit_message_text("Выберите категорию услуги:", reply_markup=service_cats_kb())
        return BOOK_FLOW

    if data.startswith("cat:"):
        cat = data.split(":", 1)[1]
        context.user_data["book_cat"] = cat
        await q.edit_message_text("Выберите услугу:", reply_markup=services_list_kb(cat))
        return BOOK_FLOW

    if data.startswith("svc:"):
        key = data.split(":", 1)[1]
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = int(price)

        await q.edit_message_text(
            f"Вы выбрали:\n*{title}* — *{price} ₽*\n\n"
            "Теперь выберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(prefix="day")
        )
        return BOOK_FLOW

    if data == "back_to_days":
        await q.edit_message_text("Выберите дату:", reply_markup=build_days_kb(prefix="day"))
        return BOOK_FLOW

    if data.startswith("day:"):
        day_iso = data.split(":", 1)[1]
        context.user_data["book_date"] = day_iso
        await q.edit_message_text(
            f"Дата: *{fmt_date_ru(day_iso)}*\nВыберите время (только свободные слоты):",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="client")
        )
        return BOOK_FLOW

    if data.startswith("time:"):
        t = data.split(":", 1)[1]
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.message.reply_text("Выберите дату заново 🙂", reply_markup=build_days_kb(prefix="day"))
            return BOOK_FLOW

        context.user_data["book_time"] = t

        await q.edit_message_text(
            "Добавьте комментарий (необязательно).\n\n"
            "Например: «снятие», «укрепление», «френч», «пожелания по форме».\n\n"
            "Можно нажать «Без комментария».",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Без комментария", callback_data="comment:-")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")],
            ])
        )
        return BOOK_COMMENT

    if data == "back_to_times":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.edit_message_text("Выберите дату:", reply_markup=build_days_kb(prefix="day"))
            return BOOK_FLOW
        await q.edit_message_text("Выберите время:", reply_markup=build_times_kb(day_iso, mode="client"))
        return BOOK_FLOW

    if data.startswith("comment:"):
        comment = data.split(":", 1)[1]
        context.user_data["comment"] = "" if comment == "-" else comment
        return await show_confirm(q, context)

    if data == "confirm_booking":
        return await confirm_booking(update, context)

    # ===== user cancel =====
    if data.startswith("ucancel:"):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.", reply_markup=main_menu_kb(admin))
            return ConversationHandler.END
        if b.user_id != uid:
            await q.message.reply_text("Нельзя отменить чужую запись.", reply_markup=main_menu_kb(admin))
            return ConversationHandler.END

        store.set_booking_status(booking_id, "cancelled")
        await q.edit_message_text("✅ Запись отменена. Если нужно — запишитесь заново через меню.")

        u = store.get_user(b.user_id)
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
                await context.bot.send_message(
                    ADMIN_ID,
                    msg,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать клиенту", url=link)]])
                )
            except Exception:
                pass

        return ConversationHandler.END

    # ===== admin panel =====
    if data == "adm_today" and admin:
        today_iso = now_local().date().isoformat()
        items = store.list_day(today_iso)
        if not items:
            await q.message.reply_text("На сегодня записей нет 🙂")
            return ConversationHandler.END

        lines = [f"📅 *Записи на сегодня* ({fmt_date_ru(today_iso)})", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return ConversationHandler.END

    if data == "adm_next" and admin:
        items = store.list_next(25)
        if not items:
            await q.message.reply_text("Ближайших записей нет 🙂")
            return ConversationHandler.END

        lines = ["⏭ *Ближайшие записи*", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{fmt_dt_ru(b.book_date, b.book_time)}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return ConversationHandler.END

    if data == "adm_block" and admin:
        await q.message.reply_text(
            "⛔ *Блокировка слота*\nВыберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(prefix="adm_block_day")
        )
        return ConversationHandler.END

    if data == "adm_unblock" and admin:
        await q.message.reply_text(
            "✅ *Разблокировка слота*\nВыберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb(prefix="adm_unblock_day")
        )
        return ConversationHandler.END

    if data == "adm_back_days" and admin:
        # возвращаем туда, откуда пришли (смотрим флаг)
        mode = context.user_data.get("adm_mode", "block")
        prefix = "adm_block_day" if mode == "block" else "adm_unblock_day"
        title = "⛔ *Блокировка слота*\nВыберите дату:" if mode == "block" else "✅ *Разблокировка слота*\nВыберите дату:"
        await q.message.reply_text(title, parse_mode="Markdown", reply_markup=build_days_kb(prefix=prefix))
        return ConversationHandler.END

    if data.startswith("adm_block_day:") and admin:
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "block"
        await q.message.reply_text(
            f"⛔ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для блокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_block")
        )
        return ConversationHandler.END

    if data.startswith("adm_block_time:") and admin:
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)

        if store.is_slot_taken(day_iso, t):
            await q.message.reply_text("Этот слот уже занят записью. Нельзя заблокировать.")
            return ConversationHandler.END

        store.block_slot(day_iso, t)
        await q.message.reply_text(f"✅ Заблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return ConversationHandler.END

    if data.startswith("adm_unblock_day:") and admin:
        day_iso = data.split(":", 1)[1]
        context.user_data["adm_mode"] = "unblock"
        await q.message.reply_text(
            f"✅ Дата: *{fmt_date_ru(day_iso)}*\nВыберите время для разблокировки:",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso, mode="adm_unblock")
        )
        return ConversationHandler.END

    if data.startswith("adm_unblock_time:") and admin:
        payload = data.split(":", 1)[1]
        day_iso, t = payload.split("|", 1)
        store.unblock_slot(day_iso, t)
        await q.message.reply_text(f"✅ Разблокировано: *{fmt_dt_ru(day_iso, t)}*", parse_mode="Markdown")
        return ConversationHandler.END

    # подтверждение/отмена админом (из уведомлений)
    if data.startswith("adm_confirm:") and admin:
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return ConversationHandler.END

        store.set_booking_status(booking_id, "confirmed")
        u = store.get_user(b.user_id)
        if u:
            try:
                await context.bot.send_message(
                    u.tg_id,
                    "✅ *Запись подтверждена!*\n\n"
                    f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                    f"• {b.service_title}\n\n"
                    "Если планы изменятся — нажмите *❌ Отменить запись* в меню.",
                    parse_mode="Markdown",
                    reply_markup=main_menu_kb(False)
                )
            except Exception:
                pass

        await q.message.reply_text(f"✅ Подтверждено (ID {booking_id})")
        return ConversationHandler.END

    if data.startswith("adm_cancel:") and admin:
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return ConversationHandler.END

        store.set_booking_status(booking_id, "cancelled")
        u = store.get_user(b.user_id)
        if u:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать администратору", url=ADMIN_CONTACT)]]) if ADMIN_CONTACT else None
            try:
                await context.bot.send_message(
                    u.tg_id,
                    "❌ *Запись отменена администратором.*\n\n"
                    f"• {fmt_dt_ru(b.book_date, b.book_time)}\n"
                    f"• {b.service_title}\n\n"
                    "Хотите — подберём другое время 🙂",
                    parse_mode="Markdown",
                    reply_markup=kb or main_menu_kb(False)
                )
            except Exception:
                pass

        await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
        return ConversationHandler.END


async def booking_comment_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # если клиент напечатал комментарий текстом
    text = (update.message.text or "").strip()
    context.user_data["comment"] = "" if text == "-" else text
    # отправим подтверждение отдельным сообщением (чтобы не ломать inline)
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
         InlineKeyboardButton("В меню", callback_data="back_to_menu")],
    ])

    await update.message.reply_text(
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{fmt_dt_ru(d, t)}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и заявка уйдёт администратору ✅",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return ConversationHandler.END


async def show_confirm(q, context: ContextTypes.DEFAULT_TYPE):
    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
         InlineKeyboardButton("В меню", callback_data="back_to_menu")],
    ])

    await q.edit_message_text(
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{fmt_dt_ru(d, t)}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и заявка уйдёт администратору ✅",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return ConversationHandler.END


async def confirm_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    u = store.get_user(uid)
    if not u:
        await q.message.reply_text("Сначала /start 🙂", reply_markup=main_menu_kb(is_admin(uid)))
        return ConversationHandler.END

    key = context.user_data.get("service_key")
    title = context.user_data.get("service_title")
    price = int(context.user_data.get("service_price", 0))
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment", "")

    if not all([key, title, d, t]) or price <= 0:
        await q.message.reply_text("Что-то пошло не так. Нажмите 💅 Записаться ещё раз.", reply_markup=main_menu_kb(is_admin(uid)))
        return ConversationHandler.END

    # проверка слота ещё раз
    if store.is_slot_blocked(d, t) or store.is_slot_taken(d, t):
        await q.message.reply_text("Этот слот уже занят 😕 Выберите другое время.", reply_markup=main_menu_kb(is_admin(uid)))
        return ConversationHandler.END

    booking_id = store.create_booking(u.tg_id, key, title, price, d, t, comment)

    await q.edit_message_text(
        "✅ *Заявка отправлена администратору!*\n\n"
        f"• {fmt_dt_ru(d, t)}\n"
        f"• {title}\n\n"
        "Как только подтвердим — пришлю уведомление 🙂",
        parse_mode="Markdown"
    )

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
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"adm_confirm:{booking_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"adm_cancel:{booking_id}"),
        ],
        [
            InlineKeyboardButton("💬 Написать клиенту", url=link),
        ],
    ])

    try:
        await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown", reply_markup=admin_kb)
    except Exception as e:
        log.exception("Failed to notify admin: %s", e)

    # очистка временных данных
    for k in ["service_key", "service_title", "service_price", "book_date", "book_time", "comment", "book_cat"]:
        context.user_data.pop(k, None)

    await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb(is_admin(uid)))
    return ConversationHandler.END

# ================== ERRORS ==================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)

# ================== APP ==================
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_PHONE: [MessageHandler((filters.CONTACT | (filters.TEXT & ~filters.COMMAND)), reg_phone)],
            BOOK_FLOW: [CallbackQueryHandler(callbacks)],
            BOOK_COMMENT: [
                CallbackQueryHandler(callbacks),
                MessageHandler(filters.TEXT & ~filters.COMMAND, booking_comment_text),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    # callback handler (на всякий случай, если вне ConversationHandler)
    app.add_handler(CallbackQueryHandler(callbacks))

    # меню reply
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    return app

def main():
    log.info("Starting bot...")
    app = build_app()
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
