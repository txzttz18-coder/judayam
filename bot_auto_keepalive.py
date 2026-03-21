import asyncio
import contextlib
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

# ==========================================================
# ASOSIY SOZLAMALAR
# ==========================================================

# Bot tokenni o'zingiz qo'ying
BOT_TOKEN = os.getenv("BOT_TOKEN", "8724558256:AAHjWfRXhOX5xJxQ1I-MnEE3fC8UoJ_khPs")

# Telegram API ma'lumotlari
API_ID = int(os.getenv("API_ID", "25504616"))
API_HASH = os.getenv("API_HASH", "41495a370906cf32c7242e1454ba347e")

# Sessiya saqlanadigan fayl
SESSION_FILE = Path("user_session.txt")

# Railway yoki boshqa hosting porti
APP_PORT = int(os.getenv("PORT", "8086"))

# Public URL bo'lsa shu yerga yozasiz:
# Masalan: https://your-app.up.railway.app
SELF_URL = os.getenv("SELF_URL", "").strip()

# Railway public domeni avtomatik olinadi
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip()

# Har 5 daqiqada ping
KEEP_ALIVE_INTERVAL = 300

# Tashkent vaqti uchun oddiy fixed timezone
# Termuxda tzdata muammosini oldini oladi
TASHKENT_TZ = timezone(timedelta(hours=5))

# Kuzatiladigan kanal va uning discussion group'i
TARGETS = {
    "topaliycapp": {
        "discussion_group": "aliycap_chatt",
        "comment_text": "Karta: 9860 3566 3116 0575\nPubg id 51874772109\nJoniz sog bosin",
    },

    "EXTRA_PUBGMOBILE": {
        "discussion_group": "EXTRAPUBG_Chat",
        "comment_text": "Karta: 9860 3566 3116 0575\nPubg id 51874772109\nJoniz sog bosin",
    },

    "aslamboi": {
        "discussion_group": "Aslamboiiiiiiiiiiiii",
        "comment_text": "Aslamboi qalaysiz men addushi muhlisizman kommentda hech bolmasa 5 top ichida bolib yozaman tahrir qilmayman pubg id 51874772109",
    },
    
    "acrofilms_uz": {
        "discussion_group": "rek_vz_gr_reklamaa",
        "comment_text": "Aslamboi qalaysansiz men addushi muhlisizman siz uc tashab bermaguningizcha postingizdagi kommentda hech bolmasa 5 top ichida bolib yozaman tahrir qilmayman pubg id 51874772109 postni kutib otrganimni isboti <real_time>",
    },
}

# Bir xil postga qayta yozmaslik uchun
processed_posts: dict[tuple[str, int], float] = {}

# Log sozlamasi
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Aiogram bot va dispatcher
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Telethon user client
telethon_client = TelegramClient(StringSession(), API_ID, API_HASH)

# Faqat bitta watcher ochilishi uchun
watcher_started = False
watcher_lock = asyncio.Lock()

# Login jarayoni uchun vaqtinchalik ma'lumotlar
login_state_data: dict[str, str] = {}

# Dastur yopilishi uchun signal
shutdown_event = asyncio.Event()

# ==========================================================
# FSM HOLATLAR
# ==========================================================

class LoginStates(StatesGroup):
    wait_phone = State()
    wait_code = State()
    wait_password = State()

# ==========================================================
# YORDAMCHI FUNKSIYALAR
# ==========================================================

def normalize_username(value: str) -> str:
    """
    Kanal yoki group linkini username holatiga keltiradi.
    Misol:
    https://t.me/aslamboi  -> aslamboi
    @aslamboi              -> aslamboi
    """
    value = (value or "").strip()
    value = value.replace("https://t.me/", "")
    value = value.replace("http://t.me/", "")
    value = value.replace("t.me/", "")
    value = value.split("?")[0]
    value = value.strip("/")
    if value.startswith("@"):
        value = value[1:]
    return value.lower().strip()


def build_targets_config() -> dict[str, dict[str, str]]:
    """
    TARGETS dagi kalitlarni ham normal holatga keltiradi.
    """
    normalized: dict[str, dict[str, str]] = {}
    for channel_username, cfg in TARGETS.items():
        normalized[normalize_username(channel_username)] = {
            "discussion_group": normalize_username(cfg["discussion_group"]),
            "comment_text": cfg["comment_text"],
        }
    return normalized


def get_self_url() -> str:
    """
    Botning o'ziga ping yuborish uchun asosiy URLni aniqlaydi.

    Tartib:
    1) SELF_URL qo'lda berilgan bo'lsa shuni ishlatadi
    2) bo'lmasa Railway bergan RAILWAY_PUBLIC_DOMAIN ni ishlatadi
    3) hech biri bo'lmasa bo'sh string qaytaradi
    """
    raw = (SELF_URL or RAILWAY_PUBLIC_DOMAIN or "").strip().rstrip("/")

    if not raw:
        return ""

    if raw.startswith("http://") or raw.startswith("https://"):
        return raw

    return f"https://{raw}"


TARGETS = build_targets_config()


def normalize_phone(value: str) -> str:
    """
    Telefon raqamni Telegram qabul qiladigan ko'rinishga keltiradi.
    """
    value = (value or "").strip()
    digits = re.sub(r"\D", "", value)
    if not digits:
        return ""
    if value.startswith("+"):
        return f"+{digits}"
    return f"+{digits}"


def normalize_code(value: str) -> str:
    """
    Telegram login kodidagi nuqta, bo'sh joy va boshqa belgilarni olib tashlaydi.
    Masalan:
    12.345 -> 12345
    12 345 -> 12345
    """
    return re.sub(r"\D", "", (value or "").strip())


def now_tashkent_hm() -> str:
    """
    Hozirgi vaqtni faqat soat va daqiqa ko'rinishida qaytaradi.
    """
    return datetime.now(TASHKENT_TZ).strftime("%H:%M")


def build_comment_text(template: str) -> str:
    """
    Komment matnidagi <real_time> joyini hozirgi vaqt bilan almashtiradi.
    """
    return template.replace("<real_time>", now_tashkent_hm())


def cleanup_processed_posts(max_age_seconds: int = 24 * 60 * 60) -> None:
    """
    Juda eski postlarni xotiradan tozalaydi.
    """
    now = time.time()
    old_keys = [key for key, ts in processed_posts.items() if now - ts > max_age_seconds]
    for key in old_keys:
        processed_posts.pop(key, None)


def remember_processed(channel_username: str, post_id: int) -> None:
    """
    Bir xil postga qayta komment yozmaslik uchun eslab qoladi.
    """
    processed_posts[(channel_username, post_id)] = time.time()
    cleanup_processed_posts()


def is_already_processed(channel_username: str, post_id: int) -> bool:
    """
    Post ilgari ishlanganmi tekshiradi.
    """
    return (channel_username, post_id) in processed_posts


def build_main_keyboard():
    """
    Asosiy inline tugmalar.
    """
    kb = InlineKeyboardBuilder()
    kb.button(text="🔐 Login", callback_data="menu_login")
    kb.button(text="📊 Status", callback_data="menu_status")
    kb.button(text="🚪 Logout", callback_data="menu_logout")
    kb.button(text="ℹ️ Yordam", callback_data="menu_help")
    kb.adjust(2, 2)
    return kb.as_markup()


def extract_wait_seconds(exc: Exception) -> int:
    """
    FloodWait xatosidan kutish soniyasini chiqaradi.
    """
    seconds = getattr(exc, "seconds", None)
    if seconds is None:
        seconds = getattr(exc, "value", 0)
    try:
        return int(seconds)
    except Exception:
        return 0


async def ensure_telethon_connected() -> None:
    """
    Telethon client ulanmagan bo'lsa ulaydi.
    """
    if not telethon_client.is_connected():
        await telethon_client.connect()


async def load_session_from_file() -> None:
    """
    Agar sessiya saqlangan bo'lsa, uni xotiraga yuklaydi.
    """
    global telethon_client

    if not SESSION_FILE.exists():
        return

    saved = SESSION_FILE.read_text(encoding="utf-8").strip()
    if not saved:
        return

    telethon_client = TelegramClient(StringSession(saved), API_ID, API_HASH)


async def save_session_to_file() -> None:
    """
    Hozirgi sessiyani faylga yozadi.
    """
    SESSION_FILE.write_text(telethon_client.session.save(), encoding="utf-8")


async def start_comment_watcher() -> None:
    """
    Kanal postlarini kuzatib, yangi post chiqsa komment yozadi.
    """
    global watcher_started

    async with watcher_lock:
        if watcher_started:
            return

        await ensure_telethon_connected()

        resolved_channels = []
        for channel_username in TARGETS.keys():
            try:
                entity = await telethon_client.get_entity(channel_username)
                resolved_channels.append(entity)
                logging.info(f"Kuzatuvga qo'shildi: @{channel_username}")
            except Exception as e:
                logging.error(f"Kanal topilmadi yoki ochilmadi @{channel_username}: {e}")

        if not resolved_channels:
            logging.warning("Hech qanday kanal topilmadi. Kuzatuv ishga tushmadi.")
            return

        async def on_new_channel_post(event):
            try:
                msg = event.message
                if not msg:
                    return

                # Faqat kanal posti bo'lsa ishlaymiz
                if not getattr(msg, "post", False):
                    return

                chat = await event.get_chat()
                channel_username = normalize_username(getattr(chat, "username", "") or "")

                if channel_username not in TARGETS:
                    return

                if is_already_processed(channel_username, msg.id):
                    return

                remember_processed(channel_username, msg.id)

                cfg = TARGETS[channel_username]
                comment_text = build_comment_text(cfg["comment_text"])

                logging.info(
                    f"Yangi post topildi | kanal=@{channel_username} | "
                    f"post_id={msg.id} | discussion=@{cfg['discussion_group']}"
                )

                # Aynan shu kanal posti ostiga komment yozish
                await telethon_client.send_message(
                    entity=chat,
                    message=comment_text,
                    comment_to=msg.id,
                )

                logging.info(
                    f"Komment yozildi | kanal=@{channel_username} | post_id={msg.id}"
                )

            except FloodWaitError as e:
                wait_seconds = extract_wait_seconds(e)
                logging.warning(f"FloodWait chiqdi: {wait_seconds} soniya kutish kerak")
                await asyncio.sleep(wait_seconds + 1)

            except RPCError as e:
                logging.error(f"Telegram RPC xato: {e}")

            except Exception as e:
                logging.error(f"Noma'lum xato: {e}")

        telethon_client.add_event_handler(
            on_new_channel_post,
            events.NewMessage(chats=resolved_channels),
        )

        watcher_started = True
        logging.info("Kanal kuzatuvi ishga tushdi.")


async def ensure_comment_system_ready() -> None:
    """
    Agar user session tayyor bo'lsa, watcher'ni yoqadi.
    """
    await ensure_telethon_connected()

    try:
        authorized = await telethon_client.is_user_authorized()
    except Exception:
        authorized = False

    if authorized:
        await start_comment_watcher()


async def finish_login_and_start_watcher(message: Message) -> None:
    """
    Login muvaffaqiyatli tugagandan keyin sessiyani saqlaydi va watcher'ni ishga tushiradi.
    """
    await save_session_to_file()
    await start_comment_watcher()
    await message.answer(
        "✅ <b>Login muvaffaqiyatli tugadi</b>\n\n"
        "Endi bot yangi kanal postlarini ko'radi va komment yozadi."
    )


# ==========================================================
# WEB SERVER
# ==========================================================

async def handle_root(request: web.Request) -> web.Response:
    """
    Asosiy endpoint.
    """
    return web.Response(
        text=f"OK | bot ishlayapti | {now_tashkent_hm()}",
        content_type="text/plain",
    )


async def handle_ping(request: web.Request) -> web.Response:
    """
    Ping endpoint.
    """
    return web.Response(
        text="pong",
        content_type="text/plain",
    )


async def handle_health(request: web.Request) -> web.Response:
    """
    Railway healthcheck yoki oddiy tekshiruvlar uchun endpoint.
    """
    return web.Response(
        text="ok",
        content_type="text/plain",
    )


async def start_web_server() -> web.AppRunner:
    """
    Railway uchun kichik web serverni ishga tushiradi.
    """
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/ping", handle_ping)
    app.router.add_get("/health", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=APP_PORT)
    await site.start()

    logging.info(f"Web server ishga tushdi: 0.0.0.0:{APP_PORT}")
    return runner


async def self_ping_once() -> None:
    """
    Railway public domain yoki qo'lda berilgan URLga /ping yuboradi.
    """
    base_url = get_self_url()
    if not base_url:
        logging.info("SELF_URL va RAILWAY_PUBLIC_DOMAIN topilmadi, ping o'tkazib yuborildi.")
        return

    url = base_url.rstrip("/") + "/ping"
    timeout = ClientTimeout(total=15)

    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                await resp.text()
                logging.info(f"Ping yuborildi: {url} | status={resp.status}")
    except Exception as e:
        logging.warning(f"Ping yuborishda xato: {e}")


async def keep_alive_loop() -> None:
    """
    Har 5 daqiqada ping yuboradi.
    """
    await asyncio.sleep(10)

    while not shutdown_event.is_set():
        await self_ping_once()

        for _ in range(KEEP_ALIVE_INTERVAL):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


# ==========================================================
# BUYRUQLAR
# ==========================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """
    /start buyrug'i.
    """
    await state.clear()
    await ensure_comment_system_ready()

    try:
        authorized = await telethon_client.is_user_authorized()
    except Exception:
        authorized = False

    if authorized:
        text = (
            "👋 <b>Xush kelibsiz!</b>\n\n"
            "User session tayyor.\n"
            "Yangi post tushsa, bot avtomatik komment yozadi.\n\n"
            f"🕒 Hozirgi vaqt: <b>{now_tashkent_hm()}</b>"
        )
    else:
        text = (
            "👋 <b>Xush kelibsiz!</b>\n\n"
            "Hozir login qilinmagan.\n"
            "Quyidagi tugma yoki /login buyrug'i bilan login qiling."
        )

    await message.answer(text, reply_markup=build_main_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message):
    """
    /help buyrug'i.
    """
    await message.answer(
        "ℹ️ <b>Yordam</b>\n\n"
        "• /start — botni ishga tushirish\n"
        "• /login — telefon raqam orqali login qilish\n"
        "• /status — sessiya holati\n"
        "• /logout — sessiyani o'chirish\n"
        "• /cancel — login jarayonini bekor qilish\n\n"
        "📝 <b>Login tartibi</b>\n"
        "1) Telefon raqam yuborasiz\n"
        "2) Telegram kod yuboradi\n"
        "3) Kodni yozasiz\n"
        "4) Agar 2FA yoqilgan bo'lsa, parol so'raydi\n\n"
        "🔔 Komment matnida <code>&lt;real_time&gt;</code> joyi avtomatik ravishda "
        "<b>soat:daqiqa</b> bilan almashtiriladi."
    )


@router.message(Command("login"))
async def cmd_login(message: Message, state: FSMContext):
    """
    /login buyrug'i.
    """
    await ensure_telethon_connected()

    try:
        if await telethon_client.is_user_authorized():
            await message.answer(
                "✅ Siz allaqachon login qilgansiz.\n"
                "Bot tayyor ishlayapti."
            )
            return
    except Exception:
        pass

    await state.set_state(LoginStates.wait_phone)
    await message.answer(
        "📱 <b>Telefon raqamingizni yuboring</b>\n\n"
        "Misol: <code>+998901234567</code>"
    )


@router.message(Command("status"))
async def cmd_status(message: Message):
    """
    /status buyrug'i.
    """
    try:
        authorized = await telethon_client.is_user_authorized()
    except Exception:
        authorized = False

    status_text = [
        "📊 <b>Status</b>",
        "",
        f"• Sessiya: {'✅ Tayyor' if authorized else '❌ Login qilinmagan'}",
        f"• Kuzatuv: {'✅ Ishlayapti' if watcher_started else '⏸ Hali yoqilmagan'}",
        f"• Kanal soni: <b>{len(TARGETS)}</b>",
        f"• Vaqt: <b>{now_tashkent_hm()}</b>",
    ]
    await message.answer("\n".join(status_text))


@router.message(Command("logout"))
async def cmd_logout(message: Message, state: FSMContext):
    """
    /logout buyrug'i.
    """
    global watcher_started, telethon_client

    await state.clear()

    try:
        if telethon_client.is_connected():
            try:
                await telethon_client.sign_out()
            except Exception:
                pass
            await telethon_client.disconnect()
    except Exception as e:
        logging.error(f"Logout vaqtida xato: {e}")

    watcher_started = False

    if SESSION_FILE.exists():
        try:
            SESSION_FILE.unlink()
        except Exception as e:
            logging.error(f"Sessiya faylini o'chirishda xato: {e}")

    telethon_client = TelegramClient(StringSession(), API_ID, API_HASH)

    await message.answer("🚪 Sessiya o'chirildi. Endi /login bilan qayta kiring.")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    """
    Login jarayonini bekor qilish uchun.
    """
    await state.clear()
    await message.answer("❎ Login jarayoni bekor qilindi.")


# ==========================================================
# INLINE TUGMALAR CALLBACK'LARI
# ==========================================================

@router.callback_query(F.data == "menu_login")
async def cb_menu_login(callback: CallbackQuery, state: FSMContext):
    """
    Inline tugma: Login
    """
    await callback.answer()
    await state.set_state(LoginStates.wait_phone)
    if callback.message:
        await callback.message.answer(
            "📱 <b>Telefon raqamingizni yuboring</b>\n\n"
            "Misol: <code>+998901234567</code>"
        )


@router.callback_query(F.data == "menu_status")
async def cb_menu_status(callback: CallbackQuery):
    """
    Inline tugma: Status
    """
    await callback.answer()

    try:
        authorized = await telethon_client.is_user_authorized()
    except Exception:
        authorized = False

    if callback.message:
        await callback.message.answer(
            "📊 <b>Status</b>\n\n"
            f"• Sessiya: {'✅ Tayyor' if authorized else '❌ Login qilinmagan'}\n"
            f"• Kuzatuv: {'✅ Ishlayapti' if watcher_started else '⏸ Hali yoqilmagan'}\n"
            f"• Kanal soni: <b>{len(TARGETS)}</b>\n"
            f"• Vaqt: <b>{now_tashkent_hm()}</b>"
        )


@router.callback_query(F.data == "menu_logout")
async def cb_menu_logout(callback: CallbackQuery, state: FSMContext):
    """
    Inline tugma: Logout
    """
    global watcher_started, telethon_client

    await callback.answer()
    await state.clear()

    try:
        if telethon_client.is_connected():
            try:
                await telethon_client.sign_out()
            except Exception:
                pass
            await telethon_client.disconnect()
    except Exception as e:
        logging.error(f"Logout vaqtida xato: {e}")

    watcher_started = False

    if SESSION_FILE.exists():
        try:
            SESSION_FILE.unlink()
        except Exception as e:
            logging.error(f"Sessiya faylini o'chirishda xato: {e}")

    telethon_client = TelegramClient(StringSession(), API_ID, API_HASH)

    if callback.message:
        await callback.message.answer("🚪 Sessiya o'chirildi. Endi /login bilan qayta kiring.")


@router.callback_query(F.data == "menu_help")
async def cb_menu_help(callback: CallbackQuery):
    """
    Inline tugma: Help
    """
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "ℹ️ <b>Yordam</b>\n\n"
            "• /start — botni ishga tushirish\n"
            "• /login — telefon raqam orqali login qilish\n"
            "• /status — sessiya holati\n"
            "• /logout — sessiyani o'chirish\n"
            "• /cancel — login jarayonini bekor qilish\n\n"
            "📝 Login paytida kodni <code>12.345</code> ko'rinishida ham yuborishingiz mumkin.\n"
            "Bot nuqta va bo'shliqlarni o'zi olib tashlaydi."
        )


# ==========================================================
# LOGIN JARAYONI
# ==========================================================

@router.message(LoginStates.wait_phone, F.text)
async def process_phone(message: Message, state: FSMContext):
    """
    Login uchun telefon raqam qabul qiladi.
    """
    await ensure_telethon_connected()

    phone = normalize_phone(message.text)
    if not phone:
        await message.answer("❗ Telefon raqam noto'g'ri. Qayta yuboring.")
        return

    try:
        sent = await telethon_client.send_code_request(phone)
        login_state_data.clear()
        login_state_data["phone"] = phone
        login_state_data["phone_code_hash"] = sent.phone_code_hash

        await state.set_state(LoginStates.wait_code)
        await message.answer(
            "✅ Kod yuborildi.\n\n"
            "Telegramdan kelgan kodni yuboring.\n"
            "Misol: <code>12.345</code>"
        )

    except FloodWaitError as e:
        wait_seconds = extract_wait_seconds(e)
        await message.answer(
            f"⏳ Juda tez urinish bo'ldi. {wait_seconds} soniya kuting."
        )

    except RPCError as e:
        await message.answer(f"❗ Telegram xatosi: <code>{e}</code>")

    except Exception as e:
        await message.answer(f"❗ Noma'lum xato: <code>{e}</code>")


@router.message(LoginStates.wait_code, F.text)
async def process_code(message: Message, state: FSMContext):
    """
    Login uchun kod qabul qiladi.
    """
    await ensure_telethon_connected()

    phone = login_state_data.get("phone", "")
    phone_code_hash = login_state_data.get("phone_code_hash", "")
    code = normalize_code(message.text)

    if not phone or not phone_code_hash:
        await state.clear()
        await message.answer(
            "❗ Login ma'lumotlari topilmadi. Qayta /login bosing."
        )
        return

    if not code:
        await message.answer("❗ Kod noto'g'ri. Qayta yuboring.")
        return

    try:
        await telethon_client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=phone_code_hash,
        )

        await state.clear()
        login_state_data.clear()
        await finish_login_and_start_watcher(message)

    except SessionPasswordNeededError:
        await state.set_state(LoginStates.wait_password)
        await message.answer(
            "🔐 <b>2FA yoqilgan ekan</b>\n\n"
            "Endi Telegram parolingizni yuboring."
        )

    except PhoneCodeInvalidError:
        await message.answer("❗ Kod noto'g'ri. Qayta yuboring.")

    except PhoneCodeExpiredError:
        await state.clear()
        login_state_data.clear()
        await message.answer(
            "⌛ Kod eskirib qolgan.\n"
            "Qayta /login qiling."
        )

    except FloodWaitError as e:
        wait_seconds = extract_wait_seconds(e)
        await message.answer(
            f"⏳ Juda tez urinish bo'ldi. {wait_seconds} soniya kuting."
        )

    except RPCError as e:
        await message.answer(f"❗ Telegram xatosi: <code>{e}</code>")

    except Exception as e:
        await message.answer(f"❗ Noma'lum xato: <code>{e}</code>")


@router.message(LoginStates.wait_password, F.text)
async def process_password(message: Message, state: FSMContext):
    """
    2FA parolni qabul qiladi.
    """
    await ensure_telethon_connected()

    password = message.text.strip()
    if not password:
        await message.answer("❗ Parol bo'sh bo'lishi mumkin emas.")
        return

    try:
        await telethon_client.sign_in(password=password)

        await state.clear()
        login_state_data.clear()
        await finish_login_and_start_watcher(message)

    except RPCError as e:
        await message.answer(f"❗ Telegram xatosi: <code>{e}</code>")

    except Exception as e:
        await message.answer(f"❗ Noma'lum xato: <code>{e}</code>")


# ==========================================================
# STARTUP / SHUTDOWN
# ==========================================================

async def main():
    """
    Asosiy ishga tushirish funksiyasi.
    """
    global telethon_client

    logging.info("Bot ishga tushyapti...")

    # Agar oldin sessiya saqlangan bo'lsa, uni yuklaymiz
    if SESSION_FILE.exists():
        try:
            await load_session_from_file()
        except Exception as e:
            logging.error(f"Sessiyani yuklashda xato: {e}")
            telethon_client = TelegramClient(StringSession(), API_ID, API_HASH)

    await ensure_telethon_connected()

    # Web serverni ishga tushiramiz
    web_runner = await start_web_server()

    # Ichki keep-alive ping task
    ping_task = asyncio.create_task(keep_alive_loop())

    try:
        # Agar oldingi sessiya tayyor bo'lsa, watcher'ni ishga tushiramiz
        try:
            if await telethon_client.is_user_authorized():
                logging.info("Oldingi sessiya topildi. Kuzatuv ishga tushiriladi...")
                await start_comment_watcher()
        except Exception as e:
            logging.error(f"Authorization tekshiruvida xato: {e}")

        # Aiogram polling
        await dp.start_polling(bot)

    finally:
        shutdown_event.set()

        # Ping taskni yopamiz
        ping_task.cancel()
        with contextlib.suppress(Exception):
            await ping_task

        # Web serverni yopamiz
        with contextlib.suppress(Exception):
            await web_runner.cleanup()

        # Telethon clientni yopamiz
        with contextlib.suppress(Exception):
            if telethon_client.is_connected():
                await telethon_client.disconnect()

        # Aiogram sessionni yopamiz
        with contextlib.suppress(Exception):
            await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
