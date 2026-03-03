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
    ConversationHandler,
    filters,
)

from storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("beauty-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")  # твой TG user id
SALON_NAME = os.getenv("SALON_NAME", "Beauty Lounge").strip()
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "").strip()  # например: "https://t.me/username" или пусто
DB_PATH = os.getenv("DB_PATH", "data.sqlite3").strip()

if not BOT_TOKEN:
    raise RuntimeError("ENV BOT_TOKEN is required")
if not ADMIN_ID:
    raise RuntimeError("ENV ADMIN_ID is required (your telegram user id)")

store = Storage(DB_PATH)

# --------- Услуги и цены (взял из скрина + аккуратно оформил) ----------
SERVICES = {
    # key: (title, price)
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

# --------- Conversation states ----------
REG_NAME, REG_PHONE, BOOK_SERVICE, BOOK_DATE, BOOK_TIME, BOOK_COMMENT, BOOK_CONFIRM = range(7)

# --------- UI ----------
def main_menu_kb() -> ReplyKeyboardMarkup:
    kb = [
        ["💅 Записаться", "💳 Цены"],
        ["🔥 Акции", "⭐ Отзывы"],
        ["🖼 Фотогалерея", "📍 Контакты"],
        ["👤 Профиль", "❌ Отменить запись"],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def phone_request_kb() -> ReplyKeyboardMarkup:
    kb = [[KeyboardButton("📱 Отправить номер", request_contact=True)]]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, one_time_keyboard=True)

def back_inline(btn_text="⬅️ Назад", cb="back_to_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(btn_text, callback_data=cb)]])

def admin_only(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    return uid == ADMIN_ID

def fmt_prices() -> str:
    lines = [
        "💳 *Прайс-лист*",
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

def tz() -> ZoneInfo:
    return ZoneInfo(store.get_setting("tz", "Europe/Moscow"))

def now_local() -> datetime:
    return datetime.now(tz())

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

def build_days_kb() -> InlineKeyboardMarkup:
    lead_days = int(store.get_setting("lead_days", "14"))
    start = now_local().date()
    rows = []
    row = []
    for i in range(lead_days):
        d = start + timedelta(days=i)
        label = d.strftime("%d.%m (%a)").replace("Mon", "Пн").replace("Tue", "Вт").replace("Wed", "Ср")\
                                       .replace("Thu", "Чт").replace("Fri", "Пт").replace("Sat", "Сб")\
                                       .replace("Sun", "Вс")
        row.append(InlineKeyboardButton(label, callback_data=f"day:{d.isoformat()}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)

def generate_time_slots(day_iso: str) -> list[str]:
    # slots by settings: work_start, work_end, slot_minutes
    ws = store.get_setting("work_start", "10:00")
    we = store.get_setting("work_end", "20:00")
    sm = int(store.get_setting("slot_minutes", "60"))

    d = date.fromisoformat(day_iso)
    start_dt = datetime.combine(d, datetime.strptime(ws, "%H:%M").time(), tzinfo=tz())
    end_dt = datetime.combine(d, datetime.strptime(we, "%H:%M").time(), tzinfo=tz())

    slots = []
    cur = start_dt
    while cur + timedelta(minutes=sm) <= end_dt:
        slots.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=sm)
    return slots

def build_times_kb(day_iso: str) -> InlineKeyboardMarkup:
    slots = generate_time_slots(day_iso)
    rows = []
    row = []
    for t in slots:
        if store.is_slot_blocked(day_iso, t):
            continue
        if store.is_slot_taken(day_iso, t):
            continue
        row.append(InlineKeyboardButton(t, callback_data=f"time:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if not rows:
        rows = [[InlineKeyboardButton("😕 Нет свободных слотов", callback_data="noop")]]

    rows.append([
        InlineKeyboardButton("⬅️ Назад", callback_data="back_to_days"),
        InlineKeyboardButton("В меню", callback_data="back_to_menu"),
    ])
    return InlineKeyboardMarkup(rows)

def service_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✨ Маникюр", callback_data="cat:mn")],
        [InlineKeyboardButton("🦶 Педикюр", callback_data="cat:pd")],
        [InlineKeyboardButton("🌟 Дополнительно", callback_data="cat:extra")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")],
    ]
    return InlineKeyboardMarkup(rows)

def services_by_cat(cat: str) -> list[tuple[str, str, int]]:
    items = []
    if cat == "mn":
        keys = ["mn_no", "mn_cov", "mn_cov_design"]
    elif cat == "pd":
        keys = ["pd_no", "pd_cov", "pd_toes", "pd_heels"]
    else:
        keys = ["ext", "corr", "design"]
    for k in keys:
        title, price = SERVICES[k]
        items.append((k, title, price))
    return items

def services_list_kb(cat: str) -> InlineKeyboardMarkup:
    rows = []
    for k, title, price in services_by_cat(cat):
        rows.append([InlineKeyboardButton(f"{title} — {price} ₽", callback_data=f"svc:{k}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_service_cats")])
    rows.append([InlineKeyboardButton("В меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(rows)

def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Записи на сегодня", callback_data="adm:today"),
         InlineKeyboardButton("⏭ Ближайшие", callback_data="adm:next")],
        [InlineKeyboardButton("⛔ Заблокировать слот", callback_data="adm:block_help"),
         InlineKeyboardButton("✅ Разблокировать слот", callback_data="adm:unblock_help")],
        [InlineKeyboardButton("⚙️ Настройки времени", callback_data="adm:time_settings"),
         InlineKeyboardButton("👥 Статистика", callback_data="adm:stats")],
    ])

# --------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    u = store.get_user(user.id)

    text = (
        f"✨ *{SALON_NAME}*\n"
        "Запись на маникюр/педикюр в пару кликов.\n\n"
        "📌 Что умею:\n"
        "• Быстрая запись по датам и свободным слотам\n"
        "• Подтверждение/отмена через админ-панель\n"
        "• Вы можете отменить запись сами (админ получит уведомление)\n\n"
        "Нажмите *💅 Записаться* — начнём 🙂"
    )

    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_kb())

    if not u:
        # мягкая регистрация (имя + телефон), чтобы админ видел контакты
        await update.message.reply_text(
            "Для записи нужен контакт.\n\nКак к вам обращаться? (имя)",
            reply_markup=ReplyKeyboardMarkup([["⬅️ В меню"]], resize_keyboard=True, one_time_keyboard=True)
        )
        return REG_NAME
    return ConversationHandler.END

async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "⬅️ В меню":
        await update.message.reply_text("Ок 🙂", reply_markup=main_menu_kb())
        return ConversationHandler.END

    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("Напишите имя чуть понятнее 🙂")
        return REG_NAME

    context.user_data["reg_name"] = name
    await update.message.reply_text(
        "Отправьте номер телефона кнопкой ниже (так быстрее и без ошибок):",
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
        f"Готово, *{full_name}* ✅\nТеперь можно записаться.",
        parse_mode="Markdown",
        reply_markup=main_menu_kb()
    )
    return ConversationHandler.END

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "💅 Записаться":
        u = store.get_user(update.effective_user.id)
        if not u:
            await update.message.reply_text("Сначала короткая регистрация 🙂\nКак к вам обращаться? (имя)")
            return REG_NAME

        await update.message.reply_text(
            "Выберите категорию услуги:",
            reply_markup=InlineKeyboardMarkup(service_kb().inline_keyboard)
        )
        return BOOK_SERVICE

    if text == "💳 Цены":
        await update.message.reply_text(fmt_prices(), parse_mode="Markdown", reply_markup=main_menu_kb())
        return ConversationHandler.END

    if text == "🔥 Акции":
        await update.message.reply_text(
            "🔥 *Акции*\n\n"
            "• Снятие/выравнивание уточняем перед визитом\n"
            "• Скидка на комплекс (маникюр+педикюр) — по наличию окон\n\n"
            "Напишите, что хотите — подберём лучшее 🙂",
            parse_mode="Markdown",
            reply_markup=main_menu_kb()
        )
        return ConversationHandler.END

    if text == "⭐ Отзывы":
        await update.message.reply_text(
            "⭐ *Отзывы*\n\n"
            "Можете прислать ссылку на ваши отзывы (2GIS/Яндекс/Инстаграм) — я вставлю сюда красиво.\n"
            "Пока можно просто написать: «Хочу посмотреть отзывы» — и админ пришлёт подборку.",
            parse_mode="Markdown",
            reply_markup=main_menu_kb()
        )
        return ConversationHandler.END

    if text == "🖼 Фотогалерея":
        await update.message.reply_text(
            "🖼 *Фотогалерея*\n\n"
            "Сюда удобно добавить альбом/канал/инст. Скиньте ссылку — вставим кнопкой.\n"
            "Пока напишите, какой стиль хотите (нюд/яркий/френч/минимализм).",
            parse_mode="Markdown",
            reply_markup=main_menu_kb()
        )
        return ConversationHandler.END

    if text == "📍 Контакты":
        contact_btns = []
        if ADMIN_CONTACT:
            contact_btns.append([InlineKeyboardButton("💬 Написать мастеру", url=ADMIN_CONTACT)])
        await update.message.reply_text(
            "📍 *Контакты*\n\n"
            "• Адрес: (впишите адрес)\n"
            "• Время работы: 10:00–20:00\n"
            "• Запись: через кнопку *💅 Записаться*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(contact_btns) if contact_btns else main_menu_kb()
        )
        return ConversationHandler.END

    if text == "👤 Профиль":
        u = store.get_user(update.effective_user.id)
        if not u:
            await update.message.reply_text("Профиль ещё не создан. Нажмите /start")
            return ConversationHandler.END
        link = user_link(u.tg_id, u.username)
        await update.message.reply_text(
            "👤 *Профиль*\n\n"
            f"• Имя: *{u.full_name}*\n"
            f"• Телефон: *{u.phone}*\n"
            f"• Telegram: {link}\n\n"
            "Если нужно изменить данные — напишите «Сменить профиль».",
            parse_mode="Markdown",
            reply_markup=main_menu_kb()
        )
        return ConversationHandler.END

    if text == "❌ Отменить запись":
        u = store.get_user(update.effective_user.id)
        if not u:
            await update.message.reply_text("Сначала /start 🙂")
            return ConversationHandler.END
        upcoming = store.list_user_upcoming(u.tg_id)
        if not upcoming:
            await update.message.reply_text("У вас нет активных записей 🙂", reply_markup=main_menu_kb())
            return ConversationHandler.END

        rows = []
        for b in upcoming:
            rows.append([InlineKeyboardButton(
                f"❌ {b.book_date} {b.book_time} — {b.service_title}",
                callback_data=f"ucancel:{b.id}"
            )])
        rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu")])
        await update.message.reply_text("Выберите запись для отмены:", reply_markup=InlineKeyboardMarkup(rows))
        return ConversationHandler.END

    if text.lower() in ["сменить профиль", "сбросить профиль", "сброс"]:
        store.delete_user(update.effective_user.id)
        await update.message.reply_text("Ок ✅ Профиль сброшен. Нажмите /start для новой регистрации.", reply_markup=main_menu_kb())
        return ConversationHandler.END

    # Любой другой текст — отправим админу как вопрос
    u = store.get_user(update.effective_user.id)
    who = f"{u.full_name} ({u.phone})" if u else (update.effective_user.full_name or "Клиент")
    link = user_link(update.effective_user.id, update.effective_user.username or "")
    msg = f"❓ *Вопрос из бота*\nОт: *{who}*\n{link}\n\n{update.message.text}"
    try:
        await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
    except Exception:
        pass

    await update.message.reply_text("Спасибо! Я передал ваш вопрос мастеру ✅\nСкоро ответим 🙂", reply_markup=main_menu_kb())
    return ConversationHandler.END

# ----- Booking flow callbacks -----
async def booking_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "noop":
        return

    if data == "back_to_menu":
        await q.message.reply_text("Вы в меню 🙂", reply_markup=main_menu_kb())
        return ConversationHandler.END

    if data == "back_to_service_cats":
        await q.edit_message_text("Выберите категорию услуги:", reply_markup=service_kb())
        return BOOK_SERVICE

    if data.startswith("cat:"):
        cat = data.split(":", 1)[1]
        context.user_data["book_cat"] = cat
        await q.edit_message_text("Выберите услугу:", reply_markup=services_list_kb(cat))
        return BOOK_SERVICE

    if data.startswith("svc:"):
        key = data.split(":", 1)[1]
        title, price = SERVICES[key]
        context.user_data["service_key"] = key
        context.user_data["service_title"] = title
        context.user_data["service_price"] = price
        await q.edit_message_text(
            f"Вы выбрали: *{title}* — *{price} ₽*\n\nТеперь выберите дату:",
            parse_mode="Markdown",
            reply_markup=build_days_kb()
        )
        return BOOK_DATE

    if data == "back_to_days":
        await q.edit_message_text("Выберите дату:", reply_markup=build_days_kb())
        return BOOK_DATE

    if data.startswith("day:"):
        day_iso = data.split(":", 1)[1]
        context.user_data["book_date"] = day_iso
        await q.edit_message_text(
            f"Дата: *{day_iso}*\nВыберите время (показываю только свободные слоты):",
            parse_mode="Markdown",
            reply_markup=build_times_kb(day_iso)
        )
        return BOOK_TIME

    if data.startswith("time:"):
        t = data.split(":", 1)[1]
        context.user_data["book_time"] = t
        await q.edit_message_text(
            "Добавьте комментарий (необязательно).\n\n"
            "Например: «снятие», «укрепление», «френч», «пожелания по форме». \n\n"
            "Можно написать `-` если без комментария.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Без комментария", callback_data="comment:-")],
                                              [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times")]])
        )
        return BOOK_COMMENT

    if data == "back_to_times":
        day_iso = context.user_data.get("book_date")
        if not day_iso:
            await q.edit_message_text("Выберите дату:", reply_markup=build_days_kb())
            return BOOK_DATE
        await q.edit_message_text("Выберите время:", reply_markup=build_times_kb(day_iso))
        return BOOK_TIME

    if data.startswith("comment:"):
        comment = data.split(":", 1)[1]
        context.user_data["comment"] = "" if comment == "-" else comment
        return await _show_confirm(q, context)

async def booking_comment_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == "-":
        text = ""
    context.user_data["comment"] = text
    # fake callback object not available, just send confirm message
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
         InlineKeyboardButton("В меню", callback_data="back_to_menu")],
    ])

    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    await update.message.reply_text(
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{d} {t}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и я отправлю заявку мастеру ✅",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return BOOK_CONFIRM

async def _show_confirm(q, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить запись", callback_data="confirm_booking")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_times"),
         InlineKeyboardButton("В меню", callback_data="back_to_menu")],
    ])

    title = context.user_data.get("service_title")
    price = context.user_data.get("service_price")
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment") or "—"

    await q.edit_message_text(
        "Проверьте, всё верно:\n\n"
        f"• Услуга: *{title}*\n"
        f"• Цена: *{price} ₽*\n"
        f"• Дата/время: *{d} {t}*\n"
        f"• Комментарий: *{comment}*\n\n"
        "Нажмите *Подтвердить* — и я отправлю заявку мастеру ✅",
        parse_mode="Markdown",
        reply_markup=kb
    )
    return BOOK_CONFIRM

async def confirm_booking_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user = update.effective_user
    u = store.get_user(user.id)
    if not u:
        await q.message.reply_text("Сначала /start 🙂", reply_markup=main_menu_kb())
        return ConversationHandler.END

    key = context.user_data.get("service_key")
    title = context.user_data.get("service_title")
    price = int(context.user_data.get("service_price", 0))
    d = context.user_data.get("book_date")
    t = context.user_data.get("book_time")
    comment = context.user_data.get("comment", "")

    if not all([key, title, d, t]) or price <= 0:
        await q.message.reply_text("Что-то пошло не так. Нажмите 💅 Записаться ещё раз.", reply_markup=main_menu_kb())
        return ConversationHandler.END

    # защита от гонок: проверим слот ещё раз
    if store.is_slot_blocked(d, t) or store.is_slot_taken(d, t):
        await q.message.reply_text("Этот слот уже занят 😕 Выберите другое время.", reply_markup=main_menu_kb())
        return ConversationHandler.END

    booking_id = store.create_booking(u.tg_id, key, title, price, d, t, comment)

    # клиенту
    await q.edit_message_text(
        "✅ *Заявка отправлена мастеру!*\n\n"
        f"Дата/время: *{d} {t}*\n"
        f"Услуга: *{title}*\n\n"
        "Как только подтвердим — пришлю уведомление 🙂",
        parse_mode="Markdown"
    )

    # админу
    link = user_link(u.tg_id, u.username)
    admin_text = (
        "🆕 *Новая запись*\n\n"
        f"• Клиент: *{u.full_name}*\n"
        f"• Телефон: *{u.phone}*\n"
        f"• TG: {link}\n"
        f"• Услуга: *{title}* — *{price} ₽*\n"
        f"• Дата/время: *{d} {t}*\n"
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

    # очистим временные данные
    context.user_data.pop("service_key", None)
    context.user_data.pop("service_title", None)
    context.user_data.pop("service_price", None)
    context.user_data.pop("book_date", None)
    context.user_data.pop("book_time", None)
    context.user_data.pop("comment", None)
    context.user_data.pop("book_cat", None)

    await q.message.reply_text("Меню 👇", reply_markup=main_menu_kb())
    return ConversationHandler.END

# ----- user cancel -----
async def user_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    booking_id = int(q.data.split(":", 1)[1])
    b = store.get_booking(booking_id)
    if not b:
        await q.message.reply_text("Запись не найдена.", reply_markup=main_menu_kb())
        return

    if b.user_id != update.effective_user.id:
        await q.message.reply_text("Нельзя отменить чужую запись.", reply_markup=main_menu_kb())
        return

    store.set_booking_status(booking_id, "cancelled")
    await q.edit_message_text("✅ Запись отменена. Если хотите — запишитесь заново (💅 Записаться).")

    # notify admin
    u = store.get_user(b.user_id)
    if u:
        link = user_link(u.tg_id, u.username)
        msg = (
            "⚠️ *Клиент отменил запись*\n\n"
            f"• Клиент: *{u.full_name}* ({u.phone})\n"
            f"• TG: {link}\n"
            f"• Было: *{b.book_date} {b.book_time}* — *{b.service_title}*\n"
            f"ID: `{b.id}`"
        )
        try:
            await context.bot.send_message(ADMIN_ID, msg, parse_mode="Markdown",
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать клиенту", url=link)]]))
        except Exception:
            pass

# ----- Admin commands / callbacks -----
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    await update.message.reply_text("🛠 Админ-панель:", reply_markup=admin_panel_kb())

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.from_user.id != ADMIN_ID:
        await q.message.reply_text("Доступ запрещён.")
        return

    data = q.data

    if data == "adm:stats":
        users = store.count_users()
        await q.message.reply_text(
            f"📊 Статистика\n\n"
            f"• Пользователей: *{users}*",
            parse_mode="Markdown"
        )
        return

    if data == "adm:today":
        today = now_local().date().isoformat()
        items = store.list_day(today)
        if not items:
            await q.message.reply_text("На сегодня записей нет 🙂")
            return
        lines = [f"📅 *Записи на сегодня* ({today})", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm:next":
        items = store.list_next(25)
        if not items:
            await q.message.reply_text("Ближайших записей нет 🙂")
            return
        lines = ["⏭ *Ближайшие записи*", ""]
        for b in items:
            u = store.get_user(b.user_id)
            who = f"{u.full_name} ({u.phone})" if u else str(b.user_id)
            lines.append(f"• *{b.book_date} {b.book_time}* — {b.service_title} — {who} (ID `{b.id}`)")
        await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if data == "adm:block_help":
        await q.message.reply_text(
            "⛔ Блокировка слота\n\n"
            "Отправьте сообщением в формате:\n"
            "`/block YYYY-MM-DD HH:MM`\n\n"
            "Пример:\n`/block 2026-03-05 14:00`",
            parse_mode="Markdown"
        )
        return

    if data == "adm:unblock_help":
        await q.message.reply_text(
            "✅ Разблокировка слота\n\n"
            "Отправьте:\n"
            "`/unblock YYYY-MM-DD HH:MM`",
            parse_mode="Markdown"
        )
        return

    if data == "adm:time_settings":
        await q.message.reply_text(
            "⚙️ Настройки времени\n\n"
            f"Текущие:\n"
            f"• work_start: `{store.get_setting('work_start','10:00')}`\n"
            f"• work_end: `{store.get_setting('work_end','20:00')}`\n"
            f"• slot_minutes: `{store.get_setting('slot_minutes','60')}`\n"
            f"• lead_days: `{store.get_setting('lead_days','14')}`\n\n"
            "Изменение командами:\n"
            "`/set work_start 10:00`\n"
            "`/set work_end 20:00`\n"
            "`/set slot_minutes 60`\n"
            "`/set lead_days 14`",
            parse_mode="Markdown"
        )
        return

    if data.startswith("adm_confirm:"):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "confirmed")

        # клиенту
        u = store.get_user(b.user_id)
        if u:
            try:
                await context.bot.send_message(
                    u.tg_id,
                    "✅ *Запись подтверждена!*\n\n"
                    f"• {b.book_date} {b.book_time}\n"
                    f"• {b.service_title}\n\n"
                    "Если планы изменятся — нажмите *❌ Отменить запись* в меню.",
                    parse_mode="Markdown",
                    reply_markup=main_menu_kb()
                )
            except Exception:
                pass

        await q.message.reply_text(f"✅ Подтверждено (ID {booking_id})")
        return

    if data.startswith("adm_cancel:"):
        booking_id = int(data.split(":", 1)[1])
        b = store.get_booking(booking_id)
        if not b:
            await q.message.reply_text("Запись не найдена.")
            return
        store.set_booking_status(booking_id, "cancelled")

        u = store.get_user(b.user_id)
        if u:
            link = user_link(u.tg_id, u.username)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать мастеру", url=ADMIN_CONTACT)]]) if ADMIN_CONTACT else None
            try:
                await context.bot.send_message(
                    u.tg_id,
                    "❌ *Запись отменена мастером.*\n\n"
                    f"• {b.book_date} {b.book_time}\n"
                    f"• {b.service_title}\n\n"
                    "Хотите — подберём другое время 🙂",
                    parse_mode="Markdown",
                    reply_markup=kb or main_menu_kb()
                )
            except Exception:
                pass

        await q.message.reply_text(f"❌ Отменено (ID {booking_id})")
        return

# ----- Admin text commands -----
async def admin_text_cmds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return

    txt = (update.message.text or "").strip()

    if txt.startswith("/set "):
        parts = txt.split(maxsplit=2)
        if len(parts) < 3:
            await update.message.reply_text("Формат: /set key value")
            return
        key, val = parts[1], parts[2]
        if key not in ["work_start", "work_end", "slot_minutes", "lead_days", "tz"]:
            await update.message.reply_text("Неизвестный ключ.")
            return
        store.set_setting(key, val)
        await update.message.reply_text(f"✅ Установлено: {key} = {val}")
        return

    if txt.startswith("/block "):
        parts = txt.split()
        if len(parts) != 3:
            await update.message.reply_text("Формат: /block YYYY-MM-DD HH:MM")
            return
        d, t = parts[1], parts[2]
        store.block_slot(d, t)
        await update.message.reply_text(f"⛔ Заблокирован слот {d} {t}")
        return

    if txt.startswith("/unblock "):
        parts = txt.split()
        if len(parts) != 3:
            await update.message.reply_text("Формат: /unblock YYYY-MM-DD HH:MM")
            return
        d, t = parts[1], parts[2]
        store.unblock_slot(d, t)
        await update.message.reply_text(f"✅ Разблокирован слот {d} {t}")
        return

# ----- Errors -----
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error: %s", context.error)

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_PHONE: [MessageHandler((filters.CONTACT | (filters.TEXT & ~filters.COMMAND)), reg_phone)],

            BOOK_SERVICE: [CallbackQueryHandler(booking_callbacks)],
            BOOK_DATE: [CallbackQueryHandler(booking_callbacks)],
            BOOK_TIME: [CallbackQueryHandler(booking_callbacks)],
            BOOK_COMMENT: [
                CallbackQueryHandler(booking_callbacks),
                MessageHandler(filters.TEXT & ~filters.COMMAND, booking_comment_text),
            ],
            BOOK_CONFIRM: [
                CallbackQueryHandler(confirm_booking_cb, pattern="^confirm_booking$"),
                CallbackQueryHandler(booking_callbacks),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )
    app.add_handler(conv)

    # Router for menu buttons
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    # user cancel
    app.add_handler(CallbackQueryHandler(user_cancel_cb, pattern=r"^ucancel:\d+$"))

    # admin
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^(adm:|adm_confirm:|adm_cancel:).+"))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex(r"^/(set|block|unblock)\b"), admin_text_cmds))

    return app

def main():
    log.info("Starting bot...")
    app = build_app()
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()