#!/usr/bin/env python3
# coding: utf-8
# kino_serial_bot_updated.py
# Aiogram 3.22 ga mos, Termux-friendly.
# #part 1 — konfiguratsiya, helperlar, DB
# #part 2 — admin oqimlari
# #part 3 — user oqimlari va ishga tushirish

import os
import re
import asyncio
import logging
import datetime
import html
import shutil
import hashlib
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import quote_plus

import aiohttp

import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatJoinRequest,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    FSInputFile,
)
from aiogram.client.default import DefaultBotProperties

# --------------------- LOGGING ---------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kino_serial_bot")

# --------------------- KONFIG ---------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8522754363:AAFH-PpaLUciTXS8IfWj2_zjqIbSs20K9Tg").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN kerak. Masalan: export BOT_TOKEN='<token>'")

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "7794986117"))
except Exception:
    ADMIN_ID = 0

DB_FILE = os.getenv("DB_FILE", "kino_serial_bot.db").strip() or "kino_serial_bot.db"
VALIDATION_TTL = int(os.getenv("VALIDATION_TTL", "3600"))
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()

admin_states: Dict[int, Dict[str, Any]] = {}

EPISODES_PER_PAGE = 24
PUBLISH_DELAY_SECONDS = 1.5
NAMLESS_PREVIEW_SECONDS = 20
NAMLESS_CACHE_DIR = Path(os.getenv("NAMLESS_CACHE_DIR", "nameless_cache")).resolve()
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "a01a1fa9bd0bf0292e0f80125bcd01ed").strip()
TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "en-US").strip() or "en-US"
publish_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
bot_identity: Dict[str, Any] = {"id": None, "username": None}


# ===================== HELPERLAR =====================
def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def safe_text(val: Optional[str]) -> str:
    return (val or "").strip()


def normalize_optional_text(val: Optional[str]) -> Optional[str]:
    s = safe_text(val)
    if not s or s == "-":
        return None
    return s


def normalize_language_input(val: Optional[str]) -> Optional[str]:
    s = safe_text(val)
    if not s or s == "-":
        return None
    # Admin faqat ildiz so'zni yuboradi, bot "tilida" qo'shadi.
    low = s.lower()
    if low.endswith(" tilida"):
        return s[:-7].strip()
    if low.endswith("tilida"):
        return s[:-6].strip()
    return s


def display_language(val: Optional[str]) -> str:
    s = normalize_language_input(val)
    if not s:
        return "-"
    return f"{s} tilida"


def normalize_quality_input(val: Optional[str]) -> Optional[str]:
    s = safe_text(val)
    if not s or s == "-":
        return None
    s = s.replace(" ", "")
    m = re.fullmatch(r"(\d+)(?:[pP])?", s)
    if m:
        return m.group(1)
    # Agar admin "1080p" yuborsa, bazada "1080" saqlaymiz.
    m2 = re.fullmatch(r"(\d+)[pP]", s)
    if m2:
        return m2.group(1)
    return s.rstrip("pP")


def display_quality(val: Optional[str]) -> str:
    s = normalize_quality_input(val)
    if not s:
        return "-"
    if s.lower().endswith("p"):
        return s
    if re.fullmatch(r"\d+", s):
        return f"{s}p"
    return s


def make_tg_url(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    v = val.strip()
    if not v:
        return None
    if v.startswith("http://") or v.startswith("https://"):
        return v
    if v.startswith("@"):
        return f"https://t.me/{v[1:]}"
    if v.startswith("t.me/"):
        return f"https://{v}"
    if v.startswith("telegram.me/"):
        return f"https://{v}"
    if re.fullmatch(r"[A-Za-z0-9_]{3,}", v):
        return f"https://t.me/{v}"
    return v


def normalize_invite_for_compare(invite: Optional[str]) -> str:
    if not invite:
        return ""
    v = invite.strip().lower()
    v = re.sub(r"^https?://", "", v)
    v = v.replace("www.", "")
    v = v.rstrip("/")
    return v


def fmt_page_link() -> str:
    link = (settings_cache.get("codes_link") or "").strip()
    if link:
        return link
    if REQUIRED_CHANNEL:
        return REQUIRED_CHANNEL
    return "https://t.me/your_channel"


def build_start_kb() -> InlineKeyboardMarkup:
    link = fmt_page_link()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Kodlarni olish", url=link)]
        ]
    )


def build_series_episode_label(series_code: str, episode_number: int, season_num: Optional[int] = None) -> str:
    if season_num:
        return f"{season_num}-fasl | {episode_number}-qism"
    return f"{episode_number}-qism"


def build_episode_caption(series_title: str, language: Optional[str], episode_number: int, season_num: Optional[int] = None) -> str:
    line1 = f"{series_title}"
    line2 = build_series_episode_label("", episode_number, season_num)
    lang = display_language(language)
    if season_num:
        # "Serial nomi\n1-fasl | 1-qism — uzbek tilida\n\nBizning sahifamiz: ..."
        return f"{line1}\n{season_num}-fasl | {episode_number}-qism — {lang}\n\nBizning sahifamiz: {fmt_page_link()}"
    return f"{line1}\n{episode_number}-qism — {lang}\n\nBizning sahifamiz: {fmt_page_link()}"


def build_movie_caption(title: Optional[str], quality: Optional[str], language: Optional[str],
                        genre: Optional[str], country: Optional[str], year: Optional[str],
                        description: Optional[str], code: str) -> str:
    parts = []
    if title:
        parts.append(f"🎥 Nomi: {title}")
    parts.append(f"📹 Sifati: {display_quality(quality)}")
    parts.append(f"🌐 Til: {display_language(language)}")
    if genre:
        parts.append(f"🎞 Janr: {genre}")
    if country:
        parts.append(f"🏳️ Davlat: {country}")
    if year:
        parts.append(f"📅 Yil: {year}")
    if description:
        parts.append("")
        parts.append(f"✍️ Izoh: {description}")
    parts.append("")
    parts.append(f"🔖 Kod: {code}")
    parts.append(f"📌 Bizning sahifa: {fmt_page_link()}")
    return "\n".join(parts)


def build_movie_share_caption(code: str, title: Optional[str]) -> str:
    if title:
        return f"{title}\nKod: {code}"
    return f"Kod: {code}"


def parse_episode_range(text: str) -> Tuple[int, int]:
    m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", text)
    if not m:
        raise ValueError("Diapazon noto'g'ri")
    a = int(m.group(1))
    b = int(m.group(2))
    if a <= 0 or b <= 0 or a > b:
        raise ValueError("Boshlanish va tugash noto'g'ri")
    return a, b


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)



def normalize_channel_input(val: str) -> str:
    v = safe_text(val)
    if v.startswith("https://t.me/"):
        v = v[len("https://t.me/"):]
    if v.startswith("http://t.me/"):
        v = v[len("http://t.me/"):]
    if v.startswith("t.me/"):
        v = v[len("t.me/"):]
    if v.startswith("@"):
        v = v[1:]
    return v.strip()


def truncate_text(text: Optional[str], limit: int = 700) -> Optional[str]:
    if not text:
        return None
    s = text.strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def build_start_url(kind: str, code: str) -> str:
    prefix = {"movie": "m", "series": "s", "nameless": "n"}.get(kind, "m")
    username = bot_identity.get("username") or "your_bot"
    payload = f"{prefix}-{code}"
    return f"https://t.me/{username}?start={payload}"


def build_public_inline_kb(kind: str, code: str) -> InlineKeyboardMarkup:
    if kind == "movie":
        text = "▶️ Kinoni ko'rish"
    elif kind == "series":
        text = "▶️ Serialni ko'rish"
    else:
        text = "▶️ Videoni ko'rish"
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, url=build_start_url(kind, code))]]
    )


def build_yes_no_inline_kb(prefix: str, code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Ha", callback_data=f"{prefix}:yes:{code}"),
                InlineKeyboardButton(text="❌ Yo'q", callback_data=f"{prefix}:no:{code}"),
            ]
        ]
    )


def extract_media_from_message(message: Message) -> Tuple[Optional[str], Optional[str]]:
    if message.video:
        return message.video.file_id, "video"
    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.document:
        return message.document.file_id, "document"
    if message.animation:
        return message.animation.file_id, "animation"
    return None, None


def has_recent_validation(subscribed: int, last_validated_at: Optional[datetime.datetime]) -> bool:
    if not subscribed or not last_validated_at:
        return False
    return (now_utc() - last_validated_at).total_seconds() < VALIDATION_TTL


async def ensure_user_subscription(user_id: int) -> Tuple[bool, List[Tuple[str, Optional[str], Optional[str], Optional[str]]]]:
    subscribed, last_validated_at = await get_user_record_db(user_id)
    if has_recent_validation(subscribed, last_validated_at):
        return True, []
    ok, missing = await check_user_all(user_id)
    if ok:
        await update_user_last_validated(user_id, now_utc())
        return True, []
    await invalidate_user_subscription(user_id)
    return False, missing


def movie_caption_from_db(movie_row: Tuple[Any, ...], code: str, description_override: Optional[str] = None) -> str:
    title, _file_id, _file_type, year, genre, quality, language, description, country, _downloads = movie_row
    description_value = description_override if description_override is not None else description
    return build_movie_caption(title, quality, language, genre, country, year, description_value, code)


async def edit_published_movie_caption(code: str, new_description: str) -> bool:
    post = await get_publish_post(code)
    movie = await get_movie_db(code)
    if not post or not movie:
        return False
    kind = post[0]
    status = post[2]
    channel_id = post[7]
    message_id = post[8]
    if kind != "movie" or status != "published" or not channel_id or not message_id:
        return False

    new_caption = movie_caption_from_db(movie, code, description_override=new_description)
    try:
        await bot.edit_message_caption(
            chat_id=channel_id,
            message_id=int(message_id),
            caption=new_caption,
            parse_mode="HTML",
            reply_markup=build_public_inline_kb("movie", code),
        )
    except Exception:
        logger.exception("edit_published_movie_caption failed")
        return False

    title, file_id, file_type, year, genre, quality, language, _description, country, downloads = movie
    await update_movie_meta_db(code, title, year, genre, quality, language, new_description, country)
    await update_publish_post_status(code, caption=new_caption)
    return True


async def publish_manual_nameless_from_message(code: str, message: Message, replace_existing: bool = False) -> bool:
    publish_channel = safe_text(await settings_get("publish_channel"))
    if not publish_channel:
        return False

    if not await is_bot_admin_in_channel(publish_channel):
        await safe_send(ADMIN_ID, f"⚠️ Bot {publish_channel} kanalida admin emas yoki kanal topilmadi. Publish to'xtadi.")
        return False

    file_id, file_type = extract_media_from_message(message)
    if not file_id or not file_type:
        return False

    existing_post = await get_publish_post(code)
    if replace_existing and existing_post:
        old_channel_id = existing_post[7]
        old_message_id = existing_post[8]
        if old_channel_id and old_message_id:
            try:
                await bot.delete_message(chat_id=old_channel_id, message_id=int(old_message_id))
            except Exception:
                pass

    await add_movie_db(code, None, file_id, file_type, None, None, None, None, None, None, str(message.chat.id), message.message_id)

    caption = build_channel_nameless_caption(code)
    try:
        sent = None
        if file_type == "video":
            sent = await bot.send_video(
                publish_channel,
                file_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("nameless", code),
                supports_streaming=True,
            )
        elif file_type == "photo":
            sent = await bot.send_photo(
                publish_channel,
                file_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("nameless", code),
            )
        elif file_type == "animation":
            sent = await bot.send_animation(
                publish_channel,
                file_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("nameless", code),
            )
        elif file_type == "document":
            sent = await bot.send_document(
                publish_channel,
                file_id,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("nameless", code),
            )
        else:
            return False
    except Exception:
        logger.exception("publish_manual_nameless_from_message failed")
        return False

    await upsert_publish_post(
        kind="nameless",
        code=code,
        status="published",
        channel_id=publish_channel,
        message_id=sent.message_id if sent else None,
        caption=caption,
    )
    return True


async def queue_old_content_for_publish() -> Tuple[int, int]:
    queued = 0
    skipped = 0

    async def _queue_one(kind: str, code: str, source_title: Optional[str]):
        nonlocal queued, skipped
        post = await get_publish_post(code)
        if post and safe_text(post[2]) == "published":
            skipped += 1
            return
        if post and safe_text(post[2]) in {"queued", "pending_review", "failed_preview", "failed"}:
            skipped += 1
            return
        await queue_publish(kind, code, source_title=source_title)
        queued += 1

    movies = await list_movies_db(limit=5000)
    for row in movies:
        code = str(row[0])
        title = row[1]
        await _queue_one("movie", code, title)

    series = await list_series_db(limit=5000)
    for row in series:
        code = str(row[0])
        title = row[1]
        await _queue_one("series", code, title)

    return queued, skipped



async def tg_api_json(method: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, data=payload or {}) as resp:
            try:
                data = await resp.json(content_type=None)
            except Exception:
                try:
                    text = await resp.text()
                except Exception:
                    text = ""
                data = {"ok": False, "description": text or f"HTTP {resp.status}"}
            if resp.status >= 400 and not data.get("ok"):
                return data
            return data


async def tg_get_me() -> Dict[str, Any]:
    try:
        res = await tg_api_json("getMe")
        if res.get("ok"):
            return res.get("result", {}) or {}
    except Exception:
        logger.exception("getMe failed")
    return {}


async def tg_get_file_path(file_id: str) -> Optional[str]:
    try:
        res = await tg_api_json("getFile", {"file_id": file_id})
        if res.get("ok"):
            return (res.get("result") or {}).get("file_path")
    except Exception:
        logger.exception("getFile failed")
    return None


async def download_telegram_file(file_id: str, destination: Path) -> bool:
    file_id = safe_text(file_id)
    if not file_id:
        logger.error("download_telegram_file: file_id bo'sh")
        return False

    try:
        ensure_parent(str(destination))

        # Aiogram'ning o'z API chaqiruvi avval sinab ko'riladi.
        # Bu manual HTTP qatlamidan ko'ra barqarorroq bo'lishi mumkin.
        tg_file = await bot.get_file(file_id)
        file_path = safe_text(getattr(tg_file, "file_path", None))
        if not file_path:
            logger.error("download_telegram_file: file_path topilmadi. file_id=%r", file_id)
            return False

        # Faylni localga yuklab olamiz.
        await bot.download_file(file_path, destination=destination)
        if not destination.exists() or destination.stat().st_size <= 0:
            logger.error("download_telegram_file: yuklangan fayl bo'sh. file_id=%r", file_id)
            return False
        return True
    except Exception:
        logger.exception("download_telegram_file failed. file_id=%r", file_id)
        return False


def get_nameless_source_path(code: str, file_type: Optional[str] = None) -> Path:
    code = safe_text(code) or "unknown"
    suffix = ".mp4"
    ft = safe_text(file_type).lower()
    if ft == "document":
        suffix = ".bin"
    elif ft == "animation":
        suffix = ".mp4"
    elif ft == "video":
        suffix = ".mp4"
    return NAMLESS_CACHE_DIR / code / f"source{suffix}"


async def cache_nameless_source(code: str, file_id: str, file_type: str) -> Tuple[bool, Optional[str], Optional[Path]]:
    source_path = get_nameless_source_path(code, file_type)
    try:
        ensure_parent(str(source_path))
        ok = await download_telegram_file(file_id, source_path)
        if not ok:
            return False, "Telegram faylni localga yuklab bo'lmadi", None
        return True, None, source_path
    except Exception as exc:
        logger.exception("cache_nameless_source failed. code=%s", code)
        return False, str(exc) or "cache yaratishda xatolik", None


def cleanup_nameless_cache(code: str) -> None:
    code = safe_text(code) or "unknown"
    root = NAMLESS_CACHE_DIR / code
    try:
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
    except Exception:
        pass


async def translate_text(text: Optional[str], target: str = "en") -> Optional[str]:
    s = safe_text(text)
    if not s:
        return None
    url = "https://translate.googleapis.com/translate_a/single"
    params = {"client": "gtx", "sl": "auto", "tl": target, "dt": "t", "q": s}
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        translated = "".join(part[0] for part in data[0] if part and part[0])
        return translated.strip() or None
    except Exception:
        logger.exception("translate_text failed")
        return None


async def tmdb_request(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not TMDB_API_KEY:
        return None
    url = f"https://api.themoviedb.org/3{path}"
    q = dict(params)
    q["api_key"] = TMDB_API_KEY
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=q) as resp:
                resp.raise_for_status()
                return await resp.json()
    except Exception:
        logger.exception("TMDb request failed: %s", path)
        return None


async def tmdb_search(kind: str, query: str) -> Optional[Dict[str, Any]]:
    path = "/search/movie" if kind == "movie" else "/search/tv"
    data = await tmdb_request(path, {"query": query, "language": TMDB_LANGUAGE, "include_adult": "false", "page": 1})
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None
    return results[0]


async def tmdb_details(kind: str, tmdb_id: int) -> Optional[Dict[str, Any]]:
    path = f"/movie/{tmdb_id}" if kind == "movie" else f"/tv/{tmdb_id}"
    return await tmdb_request(path, {"language": TMDB_LANGUAGE})


async def resolve_tmdb(kind: str, source_title: str) -> Optional[Dict[str, Any]]:
    candidates = []
    en = await translate_text(source_title, "en")
    if en and en.lower() != source_title.lower():
        candidates.append(en)
    candidates.append(source_title)
    seen = set()
    for q in candidates:
        qn = safe_text(q)
        if not qn or qn.lower() in seen:
            continue
        seen.add(qn.lower())
        result = await tmdb_search(kind, qn)
        if result and result.get("id"):
            details = await tmdb_details(kind, int(result["id"]))
            if details:
                return details
    return None


def tmdb_poster_url(poster_path: Optional[str]) -> Optional[str]:
    if not poster_path:
        return None
    return f"https://image.tmdb.org/t/p/w780{poster_path}"


async def translate_genres_to_uz(genre_names: List[str]) -> Optional[str]:
    if not genre_names:
        return None
    out = []
    for g in genre_names:
        t = await translate_text(g, "uz")
        out.append(t or g)
    return ", ".join(out)


async def translate_countries_to_uz(country_names: List[str]) -> Optional[str]:
    if not country_names:
        return None
    out = []
    for c in country_names:
        t = await translate_text(c, "uz")
        out.append(t or c)
    return ", ".join(out)


def normalize_tmdb_name_list(values: Optional[List[Any]]) -> List[str]:
    out: List[str] = []
    for item in values or []:
        if isinstance(item, dict):
            name = safe_text(item.get("name"))
        else:
            name = safe_text(item)
        if name:
            out.append(name)
    return out


def _movie_title_from_details(details: Dict[str, Any]) -> str:
    return (details.get("title") or details.get("original_title") or "").strip()


def _series_title_from_details(details: Dict[str, Any]) -> str:
    return (details.get("name") or details.get("original_name") or "").strip()



async def build_channel_movie_caption(code: str, details: Optional[Dict[str, Any]], source_title: Optional[str]) -> Tuple[str, Optional[str], Optional[str]]:
    if not details:
        text = (
            "⚠️ Film nomi noaniq yoki noto'g'ri tarjima\n\n"
            f"🔖 Kod: {html.escape(code)}\n"
            "🛠 Sozlash -> Postni sozlash"
        )
        return text, None, None

    title_src = _movie_title_from_details(details) or (source_title or "")
    title_uz = await translate_text(title_src, "uz") or title_src
    genres = normalize_tmdb_name_list(details.get("genres"))
    countries = normalize_tmdb_name_list(details.get("production_countries"))
    genre_uz = await translate_genres_to_uz(genres) if genres else None
    country_uz = await translate_countries_to_uz(countries) if countries else None
    overview = details.get("overview") or ""
    overview_uz = await translate_text(overview, "uz") if overview else None
    overview_uz = truncate_text(overview_uz or overview, 800)
    year = (details.get("release_date") or "")[:4] or "-"
    rating = details.get("vote_average")
    votes = details.get("vote_count")
    poster = tmdb_poster_url(details.get("poster_path"))
    lines = [
        f"🎬 <b>{html.escape(title_uz or source_title or 'Film')}</b>",
        f"🌐 <b>Til:</b> Uzbek tilida",
    ]
    if genre_uz:
        lines.append(f"🎞 <b>Janr:</b> {html.escape(genre_uz)}")
    lines.append(f"📅 <b>Yil:</b> {html.escape(str(year))}")
    if country_uz:
        lines.append(f"🏳️ <b>Davlat:</b> {html.escape(country_uz)}")
    if rating is not None:
        rating_text = f"{rating:.1f}/10"
        if votes is not None:
            rating_text = f"{rating_text} ({votes})"
        lines.append(f"⭐ <b>Reyting:</b> {html.escape(rating_text)}")
    if overview_uz:
        lines.append("")
        lines.append(f"📝 <b>Izoh:</b> {html.escape(overview_uz)}")
    lines.append("")
    lines.append(f"🔖 <b>Kod:</b> {html.escape(code)}")
    lines.append("📌 <b>Filmni ko'rish uchun pastdagi tugmani bosing</b>")
    return "\n".join(lines), poster, title_uz



async def build_channel_series_caption(code: str, details: Optional[Dict[str, Any]], source_title: Optional[str]) -> Tuple[str, Optional[str], Optional[str]]:
    if not details:
        text = (
            "⚠️ Serial nomi noaniq yoki noto'g'ri tarjima\n\n"
            f"🔖 Kod: {html.escape(code)}\n"
            "🛠 Sozlash -> Postni sozlash"
        )
        return text, None, None

    title_src = _series_title_from_details(details) or (source_title or "")
    title_uz = await translate_text(title_src, "uz") or title_src
    genres = normalize_tmdb_name_list(details.get("genres"))
    countries = normalize_tmdb_name_list(details.get("origin_country"))
    if not countries:
        countries = normalize_tmdb_name_list(details.get("production_countries"))
    genre_uz = await translate_genres_to_uz(genres) if genres else None
    country_uz = await translate_countries_to_uz(countries) if countries else None
    overview = details.get("overview") or ""
    overview_uz = await translate_text(overview, "uz") if overview else None
    overview_uz = truncate_text(overview_uz or overview, 800)
    year = (details.get("first_air_date") or "")[:4] or "-"
    rating = details.get("vote_average")
    votes = details.get("vote_count")
    poster = tmdb_poster_url(details.get("poster_path"))
    lines = [
        f"📺 <b>{html.escape(title_uz or source_title or 'Serial')}</b>",
        f"🌐 <b>Til:</b> Uzbek tilida",
    ]
    if genre_uz:
        lines.append(f"🎞 <b>Janr:</b> {html.escape(genre_uz)}")
    lines.append(f"📅 <b>Yil:</b> {html.escape(str(year))}")
    if country_uz:
        lines.append(f"🏳️ <b>Davlat:</b> {html.escape(country_uz)}")
    if rating is not None:
        rating_text = f"{rating:.1f}/10"
        if votes is not None:
            rating_text = f"{rating_text} ({votes})"
        lines.append(f"⭐ <b>Reyting:</b> {html.escape(rating_text)}")
    if overview_uz:
        lines.append("")
        lines.append(f"📝 <b>Izoh:</b> {html.escape(overview_uz)}")
    lines.append("")
    lines.append(f"🔖 <b>Kod:</b> {html.escape(code)}")
    lines.append("📌 <b>Serialni ko'rish uchun pastdagi tugmani bosing</b>")
    return "\n".join(lines), poster, title_uz


def build_channel_nameless_caption(code: str) -> str:
    return (
        "📹 <b>Filmni tomosha qilish uchun pastdagi videoni ko'rish tugmasini bosing</b>\n\n"
        f"🔖 <b>Kod:</b> {html.escape(code)}"
    )


def replace_caption_title(caption: Optional[str], kind: str, new_title: str) -> Optional[str]:
    if not caption:
        return caption
    lines = caption.splitlines()
    if not lines:
        return caption
    prefix = "🎬" if kind == "movie" else "📺" if kind == "series" else "📹"
    title_line = f"{prefix} <b>{html.escape(new_title)}</b>"
    for idx, line in enumerate(lines):
        if line.startswith("🎬 <b>") or line.startswith("📺 <b>"):
            lines[idx] = title_line
            return "\n".join(lines)
    lines.insert(0, title_line)
    return "\n".join(lines)


def _movie_or_series_caption_prefix(kind: str) -> str:
    return "🎬" if kind == "movie" else "📺" if kind == "series" else "📹"


async def trim_telegram_video_preview(
    file_id: str,
    seconds: int = NAMLESS_PREVIEW_SECONDS,
    source_path: Optional[Path] = None,
) -> Tuple[Optional[tempfile.TemporaryDirectory], Optional[Path], Optional[str]]:
    file_id = safe_text(file_id)
    if not file_id and source_path is None:
        return None, None, "file_id bo'sh"

    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        logger.error("ffmpeg topilmadi. Nameless preview qilinmadi.")
        return None, None, "ffmpeg topilmadi"

    temp_dir = tempfile.TemporaryDirectory(prefix="nameless_preview_")
    temp_root = Path(temp_dir.name)
    output_path = temp_root / "preview.mp4"
    source_file: Optional[Path] = None

    try:
        if source_path is not None and source_path.exists():
            source_file = source_path
        else:
            source_file = temp_root / "source.bin"
            if not await download_telegram_file(file_id, source_file):
                try:
                    temp_dir.cleanup()
                except Exception:
                    pass
                return None, None, "Telegram faylni yuklab bo'lmadi (getFile/download bosqichi)"

        cmd = [
            ffmpeg_bin,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source_file),
            "-t",
            str(seconds),
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "28",
            "-c:a",
            "aac",
            "-movflags",
            "+faststart",
            str(output_path),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await proc.communicate()
        if proc.returncode != 0 or not output_path.exists():
            err_text = safe_text((stderr or b"").decode("utf-8", errors="ignore"))
            logger.error("ffmpeg preview xatosi: %s", err_text or f"returncode={proc.returncode}")
            try:
                temp_dir.cleanup()
            except Exception:
                pass
            return None, None, err_text or f"ffmpeg returncode={proc.returncode}"

        return temp_dir, output_path, None
    except Exception as exc:
        logger.exception("Nameless preview yaratishda xatolik")
        try:
            temp_dir.cleanup()
        except Exception:
            pass
        return None, None, str(exc) or "preview yaratishda kutilmagan xatolik"


async def update_published_post_title(code: str, kind: str, new_title: str) -> bool:
    post = await get_publish_post(code)
    if not post:
        return False

    channel_id = post[7]
    message_id = post[8]
    poster_url = post[9]
    caption = post[10]
    if not channel_id or not message_id:
        await update_publish_post_status(code, title=new_title)
        return False

    new_caption = replace_caption_title(caption, kind, new_title)
    if not new_caption:
        prefix = _movie_or_series_caption_prefix(kind)
        new_caption = f"{prefix} <b>{html.escape(new_title)}</b>\n\n🔖 <b>Kod:</b> {html.escape(code)}"

    try:
        if poster_url:
            await bot.edit_message_caption(
                chat_id=channel_id,
                message_id=int(message_id),
                caption=new_caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb(kind, code),
            )
        else:
            await bot.edit_message_text(
                chat_id=channel_id,
                message_id=int(message_id),
                text=new_caption or "",
                parse_mode="HTML",
                reply_markup=build_public_inline_kb(kind, code),
                disable_web_page_preview=True,
            )
    except Exception:
        logger.exception("Published post title edit failed")

    await update_publish_post_status(
        code,
        title=new_title,
        caption=new_caption,
    )
    return True


async def apply_manual_title_fix(code: str, kind: str, new_title: str) -> bool:
    if kind == "movie":
        mv = await get_movie_db(code)
        if not mv:
            return False
        title, file_id, file_type, year, genre, quality, language, description, country, downloads = mv
        await update_movie_meta_db(code, new_title, year, genre, quality, language, description, country)
    elif kind == "series":
        meta = await get_series_meta(code)
        if not meta:
            return False
        title, language, description, created_at = meta
        await update_series_meta_db(code, new_title, language, description)
    else:
        return False

    await update_published_post_title(code, kind, new_title)
    return True


# ===================== DB =====================
settings_cache: Dict[str, str] = {}


async def init_db():
    ensure_parent(DB_FILE)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                subscribed INTEGER DEFAULT 0,
                last_validated_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                chat_id TEXT PRIMARY KEY,
                username TEXT,
                title TEXT,
                invite TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS join_monitored (
                chat_id TEXT PRIMARY KEY,
                invite TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_join_requests (
                user_id INTEGER,
                chat_id TEXT,
                requested_at TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS publish_posts (
                kind TEXT,
                code TEXT PRIMARY KEY,
                status TEXT,
                title TEXT,
                source_title TEXT,
                search_title TEXT,
                reason TEXT,
                channel_id TEXT,
                message_id INTEGER,
                poster_url TEXT,
                caption TEXT,
                tmdb_id TEXT,
                tmdb_type TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS movies (
                code TEXT PRIMARY KEY,
                title TEXT,
                file_id TEXT,
                file_type TEXT,
                year TEXT,
                genre TEXT,
                quality TEXT,
                language TEXT,
                description TEXT,
                country TEXT,
                downloads INTEGER DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS series (
                series_code TEXT PRIMARY KEY,
                title TEXT,
                language TEXT,
                description TEXT,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS episodes (
                series_code TEXT,
                episode_number INTEGER,
                file_id TEXT,
                file_type TEXT,
                episode_title TEXT,
                downloads INTEGER DEFAULT 0,
                PRIMARY KEY (series_code, episode_number)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS series_seasons (
                series_code TEXT,
                season_number INTEGER,
                start_episode INTEGER,
                end_episode INTEGER,
                PRIMARY KEY (series_code, season_number)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS copyright_allowlist (
                user_id INTEGER PRIMARY KEY,
                added_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS code_recycle_pool (
                kind TEXT NOT NULL,
                scope TEXT DEFAULT '',
                code INTEGER NOT NULL,
                created_at TEXT,
                PRIMARY KEY (kind, scope, code)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS managed_channels (
                chat_id TEXT PRIMARY KEY,
                title TEXT,
                username TEXT,
                purpose TEXT DEFAULT 'storage',
                status TEXT DEFAULT 'active',
                added_at TEXT,
                updated_at TEXT,
                last_snapshot_file_id TEXT,
                last_snapshot_path TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                file_id TEXT,
                file_name TEXT,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS account_backup_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                phase TEXT DEFAULT 'content',
                status TEXT DEFAULT 'idle',
                signature TEXT,
                snapshot_path TEXT,
                item_index INTEGER DEFAULT 0,
                target_index INTEGER DEFAULT 0,
                total_items INTEGER DEFAULT 0,
                total_targets INTEGER DEFAULT 0,
                last_item_key TEXT,
                updated_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS archive_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT,
                code TEXT,
                episode_number INTEGER,
                chat_id TEXT,
                message_id INTEGER,
                file_id TEXT,
                file_type TEXT,
                caption TEXT,
                created_at TEXT
            )
            """
        )
        for ddl in [
            "ALTER TABLE movies ADD COLUMN source_chat_id TEXT",
            "ALTER TABLE movies ADD COLUMN source_message_id INTEGER",
            "ALTER TABLE movies ADD COLUMN archive_chat_id TEXT",
            "ALTER TABLE movies ADD COLUMN archive_message_id INTEGER",
            "ALTER TABLE episodes ADD COLUMN source_chat_id TEXT",
            "ALTER TABLE episodes ADD COLUMN source_message_id INTEGER",
            "ALTER TABLE episodes ADD COLUMN archive_chat_id TEXT",
            "ALTER TABLE episodes ADD COLUMN archive_message_id INTEGER",
        ]:
            try:
                await db.execute(ddl)
            except Exception as e:
                if "duplicate column name" not in str(e).lower() and "duplicate" not in str(e).lower():
                    raise

        await db.commit()

    # default settings
    await settings_set("next_code", await settings_get("next_code") or "100")
    await settings_set("next_series_code", await settings_get("next_series_code") or "1000")
    await settings_set("next_nameless_code", await settings_get("next_nameless_code") or "1")
    await settings_set("codes_link", await settings_get("codes_link") or "")
    await settings_set("publish_channel", await settings_get("publish_channel") or "")
    await settings_set("copyright_enabled", await settings_get("copyright_enabled") or "0")
    await settings_set("last_account_snapshot_path", await settings_get("last_account_snapshot_path") or "")


async def settings_get(key: str) -> Optional[str]:
    if key in settings_cache:
        return settings_cache[key]
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        if row:
            value = row[0]
            settings_cache[key] = value
            return value
        return None


async def settings_set(key: str, value: str):
    settings_cache[key] = value
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            (key, value),
        )
        await db.commit()


async def ensure_code_recycle_table():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS code_recycle_pool (
                kind TEXT NOT NULL,
                scope TEXT DEFAULT '',
                code INTEGER NOT NULL,
                created_at TEXT,
                PRIMARY KEY (kind, scope, code)
            )
            """
        )
        await db.commit()


async def recycle_code(kind: str, code: int, scope: str = ""):
    try:
        code_int = int(code)
    except Exception:
        return
    await ensure_code_recycle_table()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO code_recycle_pool(kind, scope, code, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (safe_text(kind), safe_text(scope), code_int, now_utc().isoformat()),
        )
        await db.commit()


async def pop_recycled_code(kind: str, scope: str = "") -> Optional[int]:
    await ensure_code_recycle_table()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT code
            FROM code_recycle_pool
            WHERE kind = ? AND scope = ?
            ORDER BY code ASC
            LIMIT 1
            """,
            (safe_text(kind), safe_text(scope)),
        )
        row = await cur.fetchone()
        if not row:
            return None
        code = int(row[0])
        await db.execute(
            """
            DELETE FROM code_recycle_pool
            WHERE kind = ? AND scope = ? AND code = ?
            """,
            (safe_text(kind), safe_text(scope), code),
        )
        await db.commit()
        return code


async def allocate_numeric_code(
    kind: str,
    setting_key: str,
    start_value: int,
    scope: str = "",
    nameless_threshold: Optional[int] = None,
    nameless_floor: Optional[int] = None,
) -> str:
    recycled = await pop_recycled_code(kind, scope)
    if recycled is not None:
        return str(recycled)

    current_raw = await settings_get(setting_key)
    try:
        current = int(current_raw or start_value)
    except Exception:
        current = start_value

    if kind == "nameless" and nameless_threshold is not None and nameless_floor is not None:
        cnt = await count_nameless_movies()
        if cnt >= nameless_threshold and current < nameless_floor:
            current = nameless_floor

    await settings_set(setting_key, str(current + 1))
    return str(current)


async def allocate_episode_number(series_code: str) -> int:
    recycled = await pop_recycled_code("episode", scope=str(series_code))
    if recycled is not None:
        return recycled

    max_num = await get_max_episode_number(series_code)
    return int(max_num) + 1


async def recycle_episode_number(series_code: str, episode_number: int):
    await recycle_code("episode", int(episode_number), scope=str(series_code))


async def recycle_movie_code(code: str):
    if re.fullmatch(r"\d+", safe_text(code)):
        await recycle_code("movie", int(code))


async def recycle_series_code(series_code: str):
    if re.fullmatch(r"\d+", safe_text(series_code)):
        await recycle_code("series", int(series_code))


async def recycle_nameless_code(code: str):
    if re.fullmatch(r"\d+", safe_text(code)):
        await recycle_code("nameless", int(code))



async def add_user_db(user_id: int):
    await ensure_account_backup_state_table()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, subscribed, last_validated_at) VALUES(?, 0, NULL)",
            (user_id,),
        )
        await db.commit()


async def update_user_last_validated(user_id: int, validated_at: datetime.datetime):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE users SET subscribed = 1, last_validated_at = ? WHERE user_id = ?",
            (validated_at.isoformat(), user_id),
        )
        await db.commit()


async def invalidate_user_subscription(user_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET subscribed = 0 WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_user_record_db(user_id: int) -> Tuple[int, Optional[datetime.datetime]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT subscribed, last_validated_at FROM users WHERE user_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
    if not row:
        return 0, None
    subscribed = int(row[0] or 0)
    last_validated_at = None
    if row[1]:
        try:
            last_validated_at = datetime.datetime.fromisoformat(row[1])
            if last_validated_at.tzinfo is None:
                last_validated_at = last_validated_at.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            last_validated_at = None
    return subscribed, last_validated_at


async def add_group_db(chat_id: str, username: Optional[str], title: Optional[str], invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO groups(chat_id, username, title, invite) VALUES(?, ?, ?, ?)",
            (str(chat_id), username, title, invite),
        )
        await db.commit()


async def remove_group_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM groups WHERE chat_id = ?", (str(chat_id),))
        await db.commit()


async def list_groups_db() -> List[Tuple[str, Optional[str], Optional[str], Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, username, title, invite FROM groups ORDER BY chat_id")
        return await cur.fetchall()


async def add_join_monitored_db(chat_id: str, invite: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO join_monitored(chat_id, invite) VALUES(?, ?)",
            (str(chat_id), invite),
        )
        await db.commit()


async def remove_join_monitored_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM join_monitored WHERE chat_id = ?", (str(chat_id),))
        await db.commit()


async def list_join_monitored_db() -> List[Tuple[str, Optional[str]]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT chat_id, invite FROM join_monitored ORDER BY chat_id")
        return await cur.fetchall()


async def is_join_monitored_db(chat_id: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM join_monitored WHERE chat_id = ?", (str(chat_id),))
        return await cur.fetchone() is not None


async def add_pending_join_request_db(user_id: int, chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO pending_join_requests(user_id, chat_id, requested_at) VALUES(?, ?, ?)",
            (user_id, str(chat_id), now_utc().isoformat()),
        )
        await db.commit()


async def list_pending_for_user_db(user_id: int) -> List[Tuple[str, str]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT chat_id, requested_at FROM pending_join_requests WHERE user_id = ?",
            (user_id,),
        )
        return await cur.fetchall()


async def add_movie_db(
    code: str,
    title: Optional[str],
    file_id: str,
    file_type: str,
    year: Optional[str],
    genre: Optional[str],
    quality: Optional[str],
    language: Optional[str],
    description: Optional[str],
    country: Optional[str],
    source_chat_id: Optional[str] = None,
    source_message_id: Optional[int] = None,
    archive_chat_id: Optional[str] = None,
    archive_message_id: Optional[int] = None,
):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO movies
            (code, title, file_id, file_type, year, genre, quality, language, description, country, downloads,
             source_chat_id, source_message_id, archive_chat_id, archive_message_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT downloads FROM movies WHERE code = ?), 0), ?, ?, ?, ?)
            """,
            (
                code,
                title,
                file_id,
                file_type,
                year,
                genre,
                quality,
                language,
                description,
                country,
                code,
                source_chat_id,
                source_message_id,
                archive_chat_id,
                archive_message_id,
            ),
        )
        await db.commit()


async def update_movie_meta_db(code: str, title: Optional[str], year: Optional[str], genre: Optional[str],
                               quality: Optional[str], language: Optional[str], description: Optional[str], country: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            UPDATE movies
            SET title = ?, year = ?, genre = ?, quality = ?, language = ?, description = ?, country = ?
            WHERE code = ?
            """,
            (title, year, genre, quality, language, description, country, code),
        )
        await db.commit()


async def remove_movie_db(code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM movies WHERE code = ?", (code,))
        exists = await cur.fetchone()
        if not exists:
            return False
        await db.execute("DELETE FROM movies WHERE code = ?", (code,))
        await db.commit()
    await recycle_movie_code(code)
    return True


async def get_movie_db(code: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT title, file_id, file_type, year, genre, quality, language, description, country, downloads
            FROM movies WHERE code = ?
            """,
            (code,),
        )
        return await cur.fetchone()


async def increment_movie_downloads(code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE movies SET downloads = downloads + 1 WHERE code = ?", (code,))
        await db.commit()


async def add_series_db(series_code: str, title: str, language: Optional[str], description: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO series(series_code, title, language, description, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (series_code, title, language, description, now_utc().isoformat()),
        )
        await db.commit()


async def replace_series_meta_db(series_code: str, title: str, language: Optional[str], description: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE series SET title = ?, language = ?, description = ?, created_at = ? WHERE series_code = ?",
            (title, language, description, now_utc().isoformat(), series_code),
        )
        await db.commit()


async def update_series_meta_db(series_code: str, title: Optional[str], language: Optional[str], description: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE series SET title = ?, language = ?, description = ? WHERE series_code = ?",
            (title, language, description, series_code),
        )
        await db.commit()


async def add_episode_db(
    series_code: str,
    episode_number: int,
    file_id: str,
    file_type: str,
    episode_title: Optional[str] = None,
    source_chat_id: Optional[str] = None,
    source_message_id: Optional[int] = None,
    archive_chat_id: Optional[str] = None,
    archive_message_id: Optional[int] = None,
):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO episodes(
                series_code, episode_number, file_id, file_type, episode_title, downloads,
                source_chat_id, source_message_id, archive_chat_id, archive_message_id
            )
            VALUES (
                ?, ?, ?, ?, ?, COALESCE((SELECT downloads FROM episodes WHERE series_code = ? AND episode_number = ?), 0),
                ?, ?, ?, ?
            )
            """,
            (
                series_code,
                episode_number,
                file_id,
                file_type,
                episode_title,
                series_code,
                episode_number,
                source_chat_id,
                source_message_id,
                archive_chat_id,
                archive_message_id,
            ),
        )
        await db.commit()



async def remove_episode_db(series_code: str, episode_number: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT 1 FROM episodes WHERE series_code = ? AND episode_number = ?",
            (series_code, episode_number),
        )
        exists = await cur.fetchone()
        if not exists:
            return False
        await db.execute(
            "DELETE FROM episodes WHERE series_code = ? AND episode_number = ?",
            (series_code, episode_number),
        )
        await db.commit()
    await recycle_episode_number(series_code, episode_number)
    return True


async def get_series_meta(series_code: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT title, language, description, created_at FROM series WHERE series_code = ?",
            (series_code,),
        )
        return await cur.fetchone()


async def get_series_episodes(series_code: str) -> List[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT episode_number, file_id, file_type, episode_title, downloads
            FROM episodes
            WHERE series_code = ?
            ORDER BY episode_number
            """,
            (series_code,),
        )
        return await cur.fetchall()


async def get_episode_db(series_code: str, episode_number: int) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT file_id, file_type, episode_title, downloads
            FROM episodes
            WHERE series_code = ? AND episode_number = ?
            """,
            (series_code, episode_number),
        )
        return await cur.fetchone()


async def increment_episode_downloads(series_code: str, episode_number: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE episodes SET downloads = downloads + 1 WHERE series_code = ? AND episode_number = ?",
            (series_code, episode_number),
        )
        await db.commit()


async def remove_series_db(series_code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM series WHERE series_code = ?", (series_code,))
        exists = await cur.fetchone()
        if not exists:
            return False

        ep_cur = await db.execute("SELECT episode_number FROM episodes WHERE series_code = ?", (series_code,))
        ep_rows = await ep_cur.fetchall()
        await db.execute("DELETE FROM series_seasons WHERE series_code = ?", (series_code,))
        await db.execute("DELETE FROM episodes WHERE series_code = ?", (series_code,))
        await db.execute("DELETE FROM series WHERE series_code = ?", (series_code,))
        await db.commit()

    for (episode_number,) in ep_rows:
        await recycle_episode_number(series_code, int(episode_number))
    await recycle_series_code(series_code)
    return True


async def clear_series_episodes_db(series_code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        ep_cur = await db.execute("SELECT episode_number FROM episodes WHERE series_code = ?", (series_code,))
        ep_rows = await ep_cur.fetchall()
        await db.execute("DELETE FROM episodes WHERE series_code = ?", (series_code,))
        await db.execute("DELETE FROM series_seasons WHERE series_code = ?", (series_code,))
        await db.commit()

    for (episode_number,) in ep_rows:
        await recycle_episode_number(series_code, int(episode_number))


async def set_series_seasons_db(series_code: str, ranges: List[Tuple[int, int]]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM series_seasons WHERE series_code = ?", (series_code,))
        for idx, (a, b) in enumerate(ranges, start=1):
            await db.execute(
                """
                INSERT INTO series_seasons(series_code, season_number, start_episode, end_episode)
                VALUES (?, ?, ?, ?)
                """,
                (series_code, idx, a, b),
            )
        await db.commit()


async def get_series_seasons_db(series_code: str) -> List[Tuple[int, int, int]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT season_number, start_episode, end_episode
            FROM series_seasons
            WHERE series_code = ?
            ORDER BY season_number
            """,
            (series_code,),
        )
        return await cur.fetchall()


async def get_episode_season(series_code: str, episode_number: int) -> Optional[int]:
    seasons = await get_series_seasons_db(series_code)
    for season_number, start_ep, end_ep in seasons:
        if start_ep <= episode_number <= end_ep:
            return int(season_number)
    return None


async def count_nameless_movies() -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT COUNT(*) FROM movies WHERE title IS NULL OR title = ''")
        row = await cur.fetchone()
        return int(row[0] or 0)


async def get_max_episode_number(series_code: str) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT MAX(episode_number) FROM episodes WHERE series_code = ?", (series_code,))
        row = await cur.fetchone()
        return int(row[0] or 0)


async def update_movie_backup_refs(code: str, chat_id: Optional[str], message_id: Optional[int]) -> None:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            UPDATE movies
            SET archive_chat_id = ?, archive_message_id = ?
            WHERE code = ?
            """,
            (chat_id, message_id, code),
        )
        await db.commit()


async def update_episode_backup_refs(series_code: str, episode_number: int, chat_id: Optional[str], message_id: Optional[int]) -> None:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            UPDATE episodes
            SET archive_chat_id = ?, archive_message_id = ?
            WHERE series_code = ? AND episode_number = ?
            """,
            (chat_id, message_id, series_code, episode_number),
        )
        await db.commit()


async def record_archive_message(
    kind: str,
    code: str,
    episode_number: Optional[int],
    chat_id: str,
    message_id: int,
    file_id: Optional[str],
    file_type: Optional[str],
    caption: Optional[str],
) -> None:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT INTO archive_messages(
                kind, code, episode_number, chat_id, message_id, file_id, file_type, caption, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                kind,
                code,
                episode_number,
                str(chat_id),
                int(message_id),
                file_id,
                file_type,
                caption,
                now_utc().isoformat(),
            ),
        )
        await db.commit()


async def get_movie_db_full(code: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT title, file_id, file_type, year, genre, quality, language, description, country, downloads,
                   source_chat_id, source_message_id, archive_chat_id, archive_message_id
            FROM movies WHERE code = ?
            """,
            (code,),
        )
        return await cur.fetchone()


async def get_episode_db_full(series_code: str, episode_number: int) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT file_id, file_type, episode_title, downloads,
                   source_chat_id, source_message_id, archive_chat_id, archive_message_id
            FROM episodes
            WHERE series_code = ? AND episode_number = ?
            """,
            (series_code, episode_number),
        )
        return await cur.fetchone()


def build_backup_caption_for_movie(code: str, title: Optional[str]) -> str:
    title_text = safe_text(title) or f"Kod {code}"
    return html.escape(title_text)


def build_backup_caption_for_series(series_code: str, series_title: Optional[str], episode_number: int, episode_title: Optional[str] = None) -> str:
    title_text = safe_text(series_title) or f"Serial {series_code}"
    ep_text = safe_text(episode_title) or f"{episode_number}-qism"
    return html.escape(f"{title_text} | {ep_text}")


async def build_backup_content_stream() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []

    movie_rows = await list_movies_db(limit=1000000)
    for code, title, year, genre, quality, language, country, downloads in movie_rows:
        full = await get_movie_db_full(code)
        if not full:
            continue
        (
            title2,
            file_id,
            file_type,
            year2,
            genre2,
            quality2,
            language2,
            description2,
            country2,
            downloads2,
            source_chat_id,
            source_message_id,
            archive_chat_id,
            archive_message_id,
        ) = full
        items.append(
            {
                "kind": "movie",
                "code": code,
                "sort_code": int(code) if str(code).isdigit() else 0,
                "file_id": file_id,
                "file_type": file_type,
                "caption": build_backup_caption_for_movie(code, title2),
                "title": title2,
                "source_chat_id": source_chat_id,
                "source_message_id": source_message_id,
                "archive_chat_id": archive_chat_id,
                "archive_message_id": archive_message_id,
            }
        )

    series_rows = await list_series_db(limit=1000000)
    for series_code, series_title, language, description, created_at in series_rows:
        eps = await get_series_episodes(series_code)
        for ep in eps:
            episode_number = int(ep[0])
            full_ep = await get_episode_db_full(series_code, episode_number)
            if not full_ep:
                continue
            file_id, file_type, episode_title, downloads2, source_chat_id, source_message_id, archive_chat_id, archive_message_id = full_ep
            items.append(
                {
                    "kind": "episode",
                    "code": series_code,
                    "sort_code": int(series_code) if str(series_code).isdigit() else 0,
                    "episode_number": episode_number,
                    "file_id": file_id,
                    "file_type": file_type,
                    "caption": build_backup_caption_for_series(series_code, series_title, episode_number, episode_title),
                    "series_title": series_title,
                    "episode_title": episode_title,
                    "source_chat_id": source_chat_id,
                    "source_message_id": source_message_id,
                    "archive_chat_id": archive_chat_id,
                    "archive_message_id": archive_message_id,
                }
            )

    items.sort(key=lambda x: (0 if x["kind"] == "movie" else 1, x["sort_code"], x.get("episode_number") or 0))
    return items




def _backup_item_key(item: Dict[str, Any]) -> str:
    kind = safe_text(item.get("kind"))
    code = safe_text(item.get("code"))
    episode = item.get("episode_number")
    return f"{kind}:{code}:{episode if episode is not None else ''}"


def _build_backup_signature(items: List[Dict[str, Any]], targets: List[str]) -> str:
    raw = "|".join(targets) + "||" + "|".join(_backup_item_key(item) for item in items)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


async def ensure_account_backup_state_table():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS account_backup_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                phase TEXT DEFAULT 'content',
                status TEXT DEFAULT 'idle',
                signature TEXT,
                snapshot_path TEXT,
                item_index INTEGER DEFAULT 0,
                target_index INTEGER DEFAULT 0,
                total_items INTEGER DEFAULT 0,
                total_targets INTEGER DEFAULT 0,
                last_item_key TEXT,
                updated_at TEXT
            )
            """
        )
        await db.commit()


async def get_account_backup_state() -> Optional[Dict[str, Any]]:
    await ensure_account_backup_state_table()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT phase, status, signature, snapshot_path, item_index, target_index, total_items, total_targets, last_item_key, updated_at
            FROM account_backup_state
            WHERE id = 1
            """
        )
        row = await cur.fetchone()
        if not row:
            return None
        return {
            "phase": row[0],
            "status": row[1],
            "signature": row[2],
            "snapshot_path": row[3],
            "item_index": int(row[4] or 0),
            "target_index": int(row[5] or 0),
            "total_items": int(row[6] or 0),
            "total_targets": int(row[7] or 0),
            "last_item_key": row[8],
            "updated_at": row[9],
        }


async def set_account_backup_state(
    *,
    phase: str,
    status: str,
    signature: str,
    snapshot_path: str,
    item_index: int,
    target_index: int,
    total_items: int,
    total_targets: int,
    last_item_key: Optional[str] = None,
):
    await ensure_account_backup_state_table()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT INTO account_backup_state(
                id, phase, status, signature, snapshot_path,
                item_index, target_index, total_items, total_targets, last_item_key, updated_at
            ) VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                phase = excluded.phase,
                status = excluded.status,
                signature = excluded.signature,
                snapshot_path = excluded.snapshot_path,
                item_index = excluded.item_index,
                target_index = excluded.target_index,
                total_items = excluded.total_items,
                total_targets = excluded.total_targets,
                last_item_key = excluded.last_item_key,
                updated_at = excluded.updated_at
            """,
            (
                phase,
                status,
                signature,
                snapshot_path,
                int(item_index),
                int(target_index),
                int(total_items),
                int(total_targets),
                last_item_key,
                now_utc().isoformat(),
            ),
        )
        await db.commit()


async def clear_account_backup_state():
    await ensure_account_backup_state_table()
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM account_backup_state WHERE id = 1")
        await db.commit()


async def mark_account_backup_progress(
    *,
    phase: str,
    signature: str,
    snapshot_path: str,
    item_index: int,
    target_index: int,
    total_items: int,
    total_targets: int,
    last_item_key: Optional[str] = None,
    status: str = "running",
):
    await set_account_backup_state(
        phase=phase,
        status=status,
        signature=signature,
        snapshot_path=snapshot_path,
        item_index=item_index,
        target_index=target_index,
        total_items=total_items,
        total_targets=total_targets,
        last_item_key=last_item_key,
    )


async def reset_account_backup_state_for_restart(signature: str, snapshot_path: str, total_items: int, total_targets: int):
    await set_account_backup_state(
        phase="content",
        status="running",
        signature=signature,
        snapshot_path=snapshot_path,
        item_index=0,
        target_index=0,
        total_items=total_items,
        total_targets=total_targets,
        last_item_key=None,
    )

BACKUP_ITEM_DELAY_SECONDS = 1.6
BACKUP_MAX_RETRIES = 5


async def send_backup_item_to_channel(channel_id: str, item: Dict[str, Any]) -> Optional[Any]:
    file_id = safe_text(item.get("file_id"))
    file_type = safe_text(item.get("file_type")).lower()
    caption = item.get("caption") or None
    kind = safe_text(item.get("kind"))
    code = safe_text(item.get("code"))
    episode_number = item.get("episode_number")

    source_chat_id = item.get("source_chat_id")
    source_message_id = item.get("source_message_id")
    archive_chat_id = item.get("archive_chat_id")
    archive_message_id = item.get("archive_message_id")

    async def _copy_from(chat_id, message_id):
        if not chat_id or not message_id:
            return None
        return await bot.copy_message(
            chat_id=channel_id,
            from_chat_id=str(chat_id),
            message_id=int(message_id),
            caption=caption,
            parse_mode="HTML",
        )

    for attempt in range(1, BACKUP_MAX_RETRIES + 1):
        try:
            await asyncio.sleep(BACKUP_ITEM_DELAY_SECONDS)

            sent = None
            if file_type == "video":
                sent = await bot.send_video(channel_id, file_id, caption=caption, parse_mode="HTML", supports_streaming=True)
            elif file_type == "photo":
                sent = await bot.send_photo(channel_id, file_id, caption=caption, parse_mode="HTML")
            elif file_type == "animation":
                sent = await bot.send_animation(channel_id, file_id, caption=caption, parse_mode="HTML")
            elif file_type == "document":
                sent = await bot.send_document(channel_id, file_id, caption=caption, parse_mode="HTML")
            elif file_type == "audio":
                sent = await bot.send_audio(channel_id, file_id, caption=caption, parse_mode="HTML")
            elif file_type == "voice":
                sent = await bot.send_voice(channel_id, file_id, caption=caption, parse_mode="HTML")
            else:
                sent = await _copy_from(source_chat_id, source_message_id)

            if sent:
                if kind == "movie":
                    await update_movie_backup_refs(code, str(channel_id), getattr(sent, "message_id", None))
                elif kind == "episode":
                    await update_episode_backup_refs(code, int(episode_number), str(channel_id), getattr(sent, "message_id", None))
                await record_archive_message(kind, code, episode_number, str(channel_id), getattr(sent, "message_id", 0), file_id, file_type, caption)
                return sent

            if source_chat_id and source_message_id:
                sent = await _copy_from(source_chat_id, source_message_id)
                if sent:
                    if kind == "movie":
                        await update_movie_backup_refs(code, str(channel_id), getattr(sent, "message_id", None))
                    elif kind == "episode":
                        await update_episode_backup_refs(code, int(episode_number), str(channel_id), getattr(sent, "message_id", None))
                    await record_archive_message(kind, code, episode_number, str(channel_id), getattr(sent, "message_id", 0), file_id, file_type, caption)
                    return sent

            if archive_chat_id and archive_message_id:
                sent = await _copy_from(archive_chat_id, archive_message_id)
                if sent:
                    if kind == "movie":
                        await update_movie_backup_refs(code, str(channel_id), getattr(sent, "message_id", None))
                    elif kind == "episode":
                        await update_episode_backup_refs(code, int(episode_number), str(channel_id), getattr(sent, "message_id", None))
                    await record_archive_message(kind, code, episode_number, str(channel_id), getattr(sent, "message_id", 0), file_id, file_type, caption)
                    return sent

            return None
        except Exception as e:
            msg = str(e).lower()
            if "wrong file identifier" in msg or "http url specified" in msg:
                if source_chat_id and source_message_id:
                    try:
                        sent = await _copy_from(source_chat_id, source_message_id)
                        if sent:
                            if kind == "movie":
                                await update_movie_backup_refs(code, str(channel_id), getattr(sent, "message_id", None))
                            elif kind == "episode":
                                await update_episode_backup_refs(code, int(episode_number), str(channel_id), getattr(sent, "message_id", None))
                            await record_archive_message(kind, code, episode_number, str(channel_id), getattr(sent, "message_id", 0), file_id, file_type, caption)
                            return sent
                    except Exception:
                        pass
                if archive_chat_id and archive_message_id:
                    try:
                        sent = await _copy_from(archive_chat_id, archive_message_id)
                        if sent:
                            if kind == "movie":
                                await update_movie_backup_refs(code, str(channel_id), getattr(sent, "message_id", None))
                            elif kind == "episode":
                                await update_episode_backup_refs(code, int(episode_number), str(channel_id), getattr(sent, "message_id", None))
                            await record_archive_message(kind, code, episode_number, str(channel_id), getattr(sent, "message_id", 0), file_id, file_type, caption)
                            return sent
                    except Exception:
                        pass
                logger.warning("backup bad file_id: channel=%s kind=%s code=%s", channel_id, kind, code)
                return None

            if attempt >= BACKUP_MAX_RETRIES:
                logger.exception("send_backup_item_to_channel failed chat=%s type=%s attempt=%s/%s", channel_id, file_type, attempt, BACKUP_MAX_RETRIES)
                return None
            await asyncio.sleep(2 * attempt)

    return None



async def _export_account_snapshot_to_channels_impl():
    snapshot_dir = Path(DB_FILE).with_name("account_snapshots")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"snapshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(DB_FILE, snapshot_path)
    await settings_set("last_account_snapshot_path", str(snapshot_path))

    rows = await list_managed_channels_db()
    active_targets = [r[0] for r in rows if safe_text(r[3]) == "storage" and safe_text(r[4]) == "active"]
    if not active_targets:
        await safe_send(ADMIN_ID, "⚠️ Saqlash uchun aktiv channel topilmadi.")
        return False

    items = await build_backup_content_stream()
    if not items:
        await safe_send(ADMIN_ID, "⚠️ Saqlash uchun kontent topilmadi.")
        return False

    signature = _build_backup_signature(items, active_targets)
    existing = await get_account_backup_state()

    start_item_index = 0
    start_target_index = 0
    start_snapshot_target_index = 0
    phase = "content"
    if existing and existing.get("signature") == signature and existing.get("status") in {"running", "paused"}:
        phase = existing.get("phase") or "content"
        start_item_index = min(max(int(existing.get("item_index") or 0), 0), len(items))
        start_target_index = min(max(int(existing.get("target_index") or 0), 0), len(active_targets))
        start_snapshot_target_index = start_target_index if phase == "snapshot" else 0
    else:
        await reset_account_backup_state_for_restart(signature, str(snapshot_path), len(items), len(active_targets))

    progress_msg = await safe_send(
        ADMIN_ID,
        f"⏳ Akkount saqlanmoqda...\nKontentlar: {len(items)}\nChannellar: {len(active_targets)}\nBoshlanish: {start_item_index + 1 if start_item_index < len(items) else len(items)}",
    )

    sent_count = 0
    fail_count = 0

    try:
        for idx in range(start_item_index, len(items)):
            item = items[idx]
            item_key = _backup_item_key(item)
            target_begin = start_target_index if idx == start_item_index and phase == "content" else 0
            for t_idx in range(target_begin, len(active_targets)):
                chat_id = active_targets[t_idx]
                try:
                    sent = await send_backup_item_to_channel(chat_id, item)
                    if sent:
                        sent_count += 1
                    else:
                        fail_count += 1
                except Exception:
                    fail_count += 1
                    logger.exception("backup send failed for channel=%s item=%s", chat_id, item.get("code"))
                    await safe_send(ADMIN_ID, f"⚠️ Yuborishda xatolik: {chat_id} | {item.get('code')}")
                    await mark_account_backup_progress(
                        phase="content",
                        signature=signature,
                        snapshot_path=str(snapshot_path),
                        item_index=idx,
                        target_index=t_idx,
                        total_items=len(items),
                        total_targets=len(active_targets),
                        last_item_key=item_key,
                        status="paused",
                    )
                    continue

                await mark_account_backup_progress(
                    phase="content",
                    signature=signature,
                    snapshot_path=str(snapshot_path),
                    item_index=idx,
                    target_index=t_idx + 1,
                    total_items=len(items),
                    total_targets=len(active_targets),
                    last_item_key=item_key,
                    status="running",
                )

            await mark_account_backup_progress(
                phase="content",
                signature=signature,
                snapshot_path=str(snapshot_path),
                item_index=idx + 1,
                target_index=0,
                total_items=len(items),
                total_targets=len(active_targets),
                last_item_key=item_key,
                status="running",
            )
            if progress_msg:
                try:
                    await progress_msg.edit_text(
                        f"⏳ Akkount saqlanmoqda...\nKontentlar: {len(items)}\nYuborilgan elementlar: {idx + 1}\nChannellar: {len(active_targets)}\nMuvaffaqiyat: {sent_count}\nXatolik: {fail_count}",
                    )
                except Exception:
                    pass

        await mark_account_backup_progress(
            phase="snapshot",
            signature=signature,
            snapshot_path=str(snapshot_path),
            item_index=len(items),
            target_index=0,
            total_items=len(items),
            total_targets=len(active_targets),
            last_item_key=None,
            status="running",
        )

        await safe_send(ADMIN_ID, f"📦 DB snapshot tayyorlandi: {snapshot_path.name}")
        for t_idx, chat_id in enumerate(active_targets[start_snapshot_target_index:], start=start_snapshot_target_index):
            try:
                sent = await bot.send_document(chat_id, FSInputFile(str(snapshot_path)), caption="DB snapshot")
                await set_managed_channel_snapshot(chat_id, str(sent.document.file_id), str(snapshot_path))
                await mark_account_backup_progress(
                    phase="snapshot",
                    signature=signature,
                    snapshot_path=str(snapshot_path),
                    item_index=len(items),
                    target_index=t_idx + 1,
                    total_items=len(items),
                    total_targets=len(active_targets),
                    last_item_key=None,
                    status="running",
                )
                await asyncio.sleep(1.0)
            except Exception:
                logger.exception("snapshot send failed to %s", chat_id)
                await safe_send(ADMIN_ID, f"⚠️ Snapshot yuborishda xatolik: {chat_id}")
                await mark_account_backup_progress(
                    phase="snapshot",
                    signature=signature,
                    snapshot_path=str(snapshot_path),
                    item_index=len(items),
                    target_index=t_idx,
                    total_items=len(items),
                    total_targets=len(active_targets),
                    last_item_key=None,
                    status="paused",
                )
                continue

        try:
            admin_doc = await bot.send_document(ADMIN_ID, FSInputFile(str(snapshot_path)), caption="DB snapshot")
            await store_account_snapshot_message_id(admin_doc.message_id if admin_doc else None)
        except Exception:
            logger.exception("admin snapshot send failed")

        await clear_account_backup_state()
        await safe_send(
            ADMIN_ID,
            f"✅ Akkount saqlandi.\nKontentlar yuborildi: {len(items)}\nChannellar: {len(active_targets)}\nSnapshot: {snapshot_path.name}",
            reply_markup=admin_account_kb(),
        )
        return True
    except Exception:
        logger.exception("export_account_snapshot_to_channels interrupted")
        await mark_account_backup_progress(
            phase="content" if phase == "content" else phase,
            signature=signature,
            snapshot_path=str(snapshot_path),
            item_index=min(len(items), start_item_index),
            target_index=min(len(active_targets), start_target_index),
            total_items=len(items),
            total_targets=len(active_targets),
            last_item_key=None,
            status="paused",
        )
        await safe_send(ADMIN_ID, "⚠️ Akkount saqlash to'xtab qoldi. Keyingi urinishda shu joyidan davom etadi.")
        return False


async def list_movies_db(limit: int = 500) -> List[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT code, title, year, genre, quality, language, country, downloads
            FROM movies
            ORDER BY CAST(code AS INTEGER) ASC
            LIMIT ?
            """,
            (limit,),
        )
        return await cur.fetchall()


async def list_series_db(limit: int = 500) -> List[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            """
            SELECT series_code, title, language, description, created_at
            FROM series
            ORDER BY CAST(series_code AS INTEGER) ASC
            LIMIT ?
            """,
            (limit,),
        )
        return await cur.fetchall()


async def export_db_copy() -> str:
    src = Path(DB_FILE)
    dst = Path(f"{src.stem}_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}{src.suffix}")
    shutil.copyfile(src, dst)
    return str(dst)


async def make_movies_report_file() -> str:
    rows = await list_movies_db()
    path = Path(f"movies_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    lines = ["KINO RO'YXATI", "====================", ""]
    for code, title, year, genre, quality, language, country, downloads in rows:
        lines.append(f"Kod: {code}")
        lines.append(f"Nomi: {title or '-'}")
        lines.append(f"Yil: {year or '-'} | Janr: {genre or '-'} | Sifat: {display_quality(quality)} | Til: {display_language(language)} | Davlat: {country or '-'} | Yuklab olingan: {downloads or 0}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)


async def make_series_report_file() -> str:
    rows = await list_series_db()
    path = Path(f"series_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    lines = ["SERIAL RO'YXATI", "====================", ""]
    for code, title, language, description, created_at in rows:
        lines.append(f"Kod: {code}")
        lines.append(f"Nomi: {title or '-'}")
        lines.append(f"Til: {display_language(language)}")
        lines.append(f"Yaratilgan: {created_at or '-'}")
        if description:
            lines.append(f"Izoh: {description}")
        seasons = await get_series_seasons_db(code)
        if seasons:
            season_line = ", ".join([f"{sn}-fasl:{a}-{b}" for sn, a, b in seasons])
            lines.append(f"Fasllar: {season_line}")
        eps = await get_series_episodes(code)
        lines.append(f"Qismlar: {len(eps)}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)



# ===================== PUBLISH QUEUE =====================
async def upsert_publish_post(kind: str, code: str, status: str, title: Optional[str] = None,
                             source_title: Optional[str] = None, search_title: Optional[str] = None,
                             reason: Optional[str] = None, channel_id: Optional[str] = None,
                             message_id: Optional[int] = None, poster_url: Optional[str] = None,
                             caption: Optional[str] = None, tmdb_id: Optional[str] = None,
                             tmdb_type: Optional[str] = None):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT INTO publish_posts(
                kind, code, status, title, source_title, search_title, reason, channel_id,
                message_id, poster_url, caption, tmdb_id, tmdb_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                kind=excluded.kind,
                status=excluded.status,
                title=excluded.title,
                source_title=excluded.source_title,
                search_title=excluded.search_title,
                reason=excluded.reason,
                channel_id=excluded.channel_id,
                message_id=excluded.message_id,
                poster_url=excluded.poster_url,
                caption=excluded.caption,
                tmdb_id=excluded.tmdb_id,
                tmdb_type=excluded.tmdb_type,
                updated_at=excluded.updated_at
            """,
            (
                kind, code, status, title, source_title, search_title, reason, channel_id,
                message_id, poster_url, caption, tmdb_id, tmdb_type, now_utc().isoformat(), now_utc().isoformat()
            ),
        )
        await db.commit()


async def update_publish_post_status(code: str, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    values = list(fields.values())
    set_sql = ", ".join([f"{k}=?" for k in keys] + ["updated_at=?"])
    values.append(now_utc().isoformat())
    values.append(code)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(f"UPDATE publish_posts SET {set_sql} WHERE code = ?", values)
        await db.commit()


async def get_publish_post(code: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT kind, code, status, title, source_title, search_title, reason, channel_id, message_id, poster_url, caption, tmdb_id, tmdb_type, created_at, updated_at FROM publish_posts WHERE code = ?",
            (code,),
        )
        return await cur.fetchone()


async def list_queued_publish_posts() -> List[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT kind, code FROM publish_posts WHERE status = 'queued' ORDER BY created_at ASC"
        )
        return await cur.fetchall()


async def is_bot_admin_in_channel(channel: str) -> bool:
    bot_id = bot_identity.get("id")
    if not bot_id:
        return False
    try:
        res = await tg_api_json("getChatMember", {"chat_id": channel, "user_id": bot_id})
        if not res.get("ok"):
            return False
        return res.get("result", {}).get("status") in {"administrator", "creator"}
    except Exception:
        return False


async def queue_publish(kind: str, code: str, source_title: Optional[str] = None):
    await upsert_publish_post(kind=kind, code=code, status="queued", source_title=source_title, search_title=source_title)
    await publish_queue.put({"kind": kind, "code": code, "source_title": source_title})


async def load_pending_publish_jobs():
    rows = await list_queued_publish_posts()
    for kind, code in rows:
        await publish_queue.put({"kind": kind, "code": code})


async def publish_movie_or_series(kind: str, code: str, source_title: Optional[str] = None, force_details: Optional[Dict[str, Any]] = None):
    publish_channel = safe_text(await settings_get("publish_channel"))
    if not publish_channel:
        await upsert_publish_post(kind=kind, code=code, status="queued", reason="publish_channel_not_set", source_title=source_title)
        return False

    if not await is_bot_admin_in_channel(publish_channel):
        await safe_send(ADMIN_ID, f"⚠️ Bot {publish_channel} kanalida admin emas yoki kanal topilmadi. Publish to'xtadi.")
        await upsert_publish_post(kind=kind, code=code, status="queued", reason="bot_not_admin", channel_id=publish_channel, source_title=source_title)
        return False

    post = await get_publish_post(code)
    manual_title_override = safe_text(post[3]) if post and post[3] else None

    details = force_details or await resolve_tmdb(kind, source_title or "")
    if not details:
        reason = "Film nomi noaniq yoki noto'g'ri tarjima" if kind == "movie" else "Serial nomi noaniq yoki noto'g'ri tarjima"
        await upsert_publish_post(kind=kind, code=code, status="pending_review", reason=reason, channel_id=publish_channel, source_title=source_title)
        sent = await bot.send_message(
            publish_channel,
            f"⚠️ {reason}\n\n🔖 Kod: {code}\n🛠 Sozlash -> Postni sozlash",
        )
        await update_publish_post_status(code, status="pending_review", channel_id=publish_channel, message_id=sent.message_id, reason=reason)
        await asyncio.sleep(PUBLISH_DELAY_SECONDS)
        return False

    if kind == "movie":
        caption, poster_url, title_uz = await build_channel_movie_caption(code, details, source_title)
        if manual_title_override:
            title_uz = manual_title_override
            caption = replace_caption_title(caption, "movie", title_uz) or caption
        genres = normalize_tmdb_name_list(details.get("genres"))
        countries = normalize_tmdb_name_list(details.get("production_countries"))
        year = (details.get("release_date") or "")[:4] or None
        overview = details.get("overview") or None
        genre_uz = await translate_genres_to_uz(genres) if genres else None
        country_uz = await translate_countries_to_uz(countries) if countries else None
        overview_uz = truncate_text(await translate_text(overview, "uz") if overview else overview, 800)
        mv = await get_movie_db(code)
        if mv:
            _, file_id, file_type, _, _, quality, _, _, _, _ = mv
            await update_movie_meta_db(code, title_uz or source_title, year, genre_uz, quality, "uzbek", overview_uz, country_uz)
        sent = None
        if poster_url:
            sent = await bot.send_photo(
                publish_channel,
                poster_url,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("movie", code),
            )
        else:
            sent = await bot.send_message(
                publish_channel,
                caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("movie", code),
                disable_web_page_preview=True,
            )
        await update_publish_post_status(code, status="published", channel_id=publish_channel, message_id=sent.message_id, poster_url=poster_url, caption=caption, title=title_uz, tmdb_id=str(details.get("id")), tmdb_type="movie", reason=None)
        await asyncio.sleep(PUBLISH_DELAY_SECONDS)
        return True

    if kind == "series":
        caption, poster_url, title_uz = await build_channel_series_caption(code, details, source_title)
        if manual_title_override:
            title_uz = manual_title_override
            caption = replace_caption_title(caption, "series", title_uz) or caption
        genres = normalize_tmdb_name_list(details.get("genres"))
        countries = normalize_tmdb_name_list(details.get("origin_country"))
        if not countries:
            countries = normalize_tmdb_name_list(details.get("production_countries"))
        overview = details.get("overview") or None
        genre_uz = await translate_genres_to_uz(genres) if genres else None
        country_uz = await translate_countries_to_uz(countries) if countries else None
        overview_uz = truncate_text(await translate_text(overview, "uz") if overview else overview, 800)
        await update_series_meta_db(code, title_uz or source_title, "uzbek", overview_uz)
        sent = None
        if poster_url:
            sent = await bot.send_photo(
                publish_channel,
                poster_url,
                caption=caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("series", code),
            )
        else:
            sent = await bot.send_message(
                publish_channel,
                caption,
                parse_mode="HTML",
                reply_markup=build_public_inline_kb("series", code),
                disable_web_page_preview=True,
            )
        await update_publish_post_status(code, status="published", channel_id=publish_channel, message_id=sent.message_id, poster_url=poster_url, caption=caption, title=title_uz, tmdb_id=str(details.get("id")), tmdb_type="tv", reason=None)
        await asyncio.sleep(PUBLISH_DELAY_SECONDS)
        return True

    return False


async def publish_nameless_movie(code: str, file_id: str, file_type: str):
    publish_channel = safe_text(await settings_get("publish_channel"))
    if not publish_channel:
        await upsert_publish_post(kind="nameless", code=code, status="queued", reason="publish_channel_not_set")
        return False

    if not await is_bot_admin_in_channel(publish_channel):
        await safe_send(ADMIN_ID, f"⚠️ Bot {publish_channel} kanalida admin emas yoki kanal topilmadi. Publish to'xtadi.")
        await upsert_publish_post(kind="nameless", code=code, status="queued", reason="bot_not_admin", channel_id=publish_channel)
        return False

    file_id = safe_text(file_id)
    file_type = safe_text(file_type)
    caption = build_channel_nameless_caption(code)
    temp_dir: Optional[tempfile.TemporaryDirectory] = None
    preview_path: Optional[Path] = None
    source_path = get_nameless_source_path(code, file_type)

    try:
        if not source_path.exists():
            cache_ok, cache_error, cached_path = await cache_nameless_source(code, file_id, file_type)
            if not cache_ok or cached_path is None:
                missing_reason = cache_error or "nameless source cache qilinmadi"
                err = f"Nameless preview yaratilmadi. code={code}. Sabab: {missing_reason}"
                logger.error(err)
                await safe_send(
                    ADMIN_ID,
                    f"⚠️ Nameless preview yaratilmadi.\nKod: {code}\nSabab: {missing_reason}\n\nOriginal media yuborilmadi.",
                    reply_markup=admin_settings_kb(),
                )
                await upsert_publish_post(kind="nameless", code=code, status="failed_preview", reason=missing_reason, channel_id=publish_channel)
                return False
            source_path = cached_path

        temp_dir, preview_path, preview_error = await trim_telegram_video_preview(
            file_id,
            NAMLESS_PREVIEW_SECONDS,
            source_path=source_path,
        )
        if not preview_path:
            missing_reason = preview_error or "noma'lum"
            err = f"Nameless preview yaratilmadi. code={code}. Sabab: {missing_reason}"
            logger.error(err)
            await safe_send(
                ADMIN_ID,
                f"⚠️ Nameless preview yaratilmadi.\nKod: {code}\nSabab: {missing_reason}\n\nOriginal media yuborilmadi.",
                reply_markup=admin_settings_kb(),
            )
            await upsert_publish_post(kind="nameless", code=code, status="failed_preview", reason=missing_reason, channel_id=publish_channel)
            return False

        sent = await bot.send_video(
            publish_channel,
            FSInputFile(preview_path),
            caption=caption,
            parse_mode="HTML",
            reply_markup=build_public_inline_kb("nameless", code),
            supports_streaming=True,
        )

        await upsert_publish_post(kind="nameless", code=code, status="published", channel_id=publish_channel, message_id=sent.message_id, caption=caption)
        await asyncio.sleep(PUBLISH_DELAY_SECONDS)
        return True
    except Exception as exc:
        logger.exception("nameless publish failed")
        await safe_send(
            ADMIN_ID,
            f"⚠️ Nameless publish xatosi.\nKod: {code}\nSabab: {exc}",
            reply_markup=admin_settings_kb(),
        )
        await upsert_publish_post(kind="nameless", code=code, status="failed", reason="publish_failed", channel_id=publish_channel)
        return False
    finally:
        try:
            if temp_dir is not None:
                temp_dir.cleanup()
        except Exception:
            pass
        try:
            cleanup_nameless_cache(code)
        except Exception:
            pass


async def process_publish_job(job: Dict[str, Any]):
    kind = job.get("kind")
    code = job.get("code")
    if not kind or not code:
        return
    publish_channel = safe_text(await settings_get("publish_channel"))
    if not publish_channel:
        await asyncio.sleep(5)
        await publish_queue.put(job)
        return
    if kind == "nameless":
        mv = await get_movie_db(code)
        if not mv:
            return
        _, file_id, file_type, *_ = mv
        await publish_nameless_movie(code, file_id, file_type)
        return
    if kind == "movie":
        mv = await get_movie_db(code)
        if not mv:
            return
        source_title = mv[0]
        await publish_movie_or_series("movie", code, source_title=source_title)
        return
    if kind == "series":
        meta = await get_series_meta(code)
        if not meta:
            return
        source_title = meta[0]
        await publish_movie_or_series("series", code, source_title=source_title)
        return


async def publish_worker():
    while True:
        job = await publish_queue.get()
        try:
            await process_publish_job(job)
        except Exception:
            logger.exception("publish_worker error")
        finally:
            publish_queue.task_done()


# ===================== CHANNEL / ACCOUNT HELPERLAR =====================
async def get_chat_info(chat_id: str) -> Dict[str, Any]:
    return await tg_api_json("getChat", {"chat_id": chat_id})


async def is_bot_admin_in_channel(channel: str) -> bool:
    bot_id = bot_identity.get("id")
    if not bot_id:
        return False
    try:
        res = await tg_api_json("getChatMember", {"chat_id": channel, "user_id": bot_id})
        if not res.get("ok"):
            return False
        return res.get("result", {}).get("status") in {"administrator", "creator"}
    except Exception:
        return False


async def transfer_publish_posts(old_channel: str, new_channel: str):
    rows = await list_published_posts_for_channel(old_channel)
    if not rows:
        return
    for kind, code, message_id in rows:
        try:
            await bot.copy_message(
                chat_id=new_channel,
                from_chat_id=old_channel,
                message_id=int(message_id),
                caption=None,
            )
            await update_publish_post_channel(code, new_channel)
            await asyncio.sleep(0.6)
        except Exception:
            logger.exception("transfer_publish_posts failed for %s", code)


async def add_managed_channel_db(
    chat_id: str,
    title: Optional[str],
    username: Optional[str],
    purpose: str = "storage",
    status: str = "active",
):
    """Insert or update a managed channel without ever breaking existing snapshot data.

    This version is intentionally split into two safe steps:
    1) INSERT OR IGNORE the row if it does not exist yet.
    2) UPDATE the mutable fields.

    That avoids placeholder-count mistakes and keeps:
    - added_at
    - last_snapshot_file_id
    - last_snapshot_path
    intact for existing rows.
    """
    chat_id = str(chat_id)
    now = now_utc().isoformat()

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO managed_channels(
                chat_id,
                added_at,
                updated_at,
                last_snapshot_file_id,
                last_snapshot_path
            )
            VALUES(?, ?, ?, NULL, NULL)
            """,
            (chat_id, now, now),
        )

        await db.execute(
            """
            UPDATE managed_channels
            SET
                title = ?,
                username = ?,
                purpose = ?,
                status = ?,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (
                title,
                username,
                purpose,
                status,
                now,
                chat_id,
            ),
        )
        await db.commit()


async def remove_managed_channel_db(chat_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM managed_channels WHERE chat_id = ?", (str(chat_id),))
        await db.commit()


async def list_managed_channels_db() -> List[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT chat_id, title, username, purpose, status, added_at, updated_at FROM managed_channels ORDER BY added_at DESC"
        )
        return await cur.fetchall()


async def get_managed_channel_db(chat_id: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT chat_id, title, username, purpose, status, added_at, updated_at, last_snapshot_file_id, last_snapshot_path FROM managed_channels WHERE chat_id = ?",
            (str(chat_id),),
        )
        return await cur.fetchone()


async def update_managed_channel_status(chat_id: str, status: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE managed_channels SET status = ?, updated_at = ? WHERE chat_id = ?",
            (status, now_utc().isoformat(), str(chat_id)),
        )
        await db.commit()


async def set_managed_channel_snapshot(chat_id: str, file_id: str, file_path: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "UPDATE managed_channels SET last_snapshot_file_id = ?, last_snapshot_path = ?, updated_at = ? WHERE chat_id = ?",
            (file_id, file_path, now_utc().isoformat(), str(chat_id)),
        )
        await db.execute(
            "INSERT INTO account_snapshots(chat_id, file_id, file_name, created_at) VALUES(?, ?, ?, ?)",
            (str(chat_id), file_id, Path(file_path).name, now_utc().isoformat()),
        )
        await db.commit()


async def get_latest_snapshot_for_channel(chat_id: str) -> Optional[Tuple[Any, ...]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT file_id, file_name FROM account_snapshots WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
            (str(chat_id),),
        )
        return await cur.fetchone()


async def account_channel_watcher():
    await asyncio.sleep(10)
    while True:
        try:
            rows = await list_managed_channels_db()
            for chat_id, title, username, purpose, status, added_at, updated_at in rows:
                if safe_text(status) != "pending":
                    continue
                if await is_bot_admin_in_channel(chat_id):
                    await update_managed_channel_status(chat_id, "active")
                    await safe_send(ADMIN_ID, f"✅ Channel admin tasdiqlandi: {chat_id}")
        except Exception:
            logger.exception("account_channel_watcher error")
        await asyncio.sleep(60)


async def export_account_snapshot_to_channels():
    return await _export_account_snapshot_to_channels_impl()


async def import_account_snapshot_from_channel(chat_id: str):
    row = await get_managed_channel_db(chat_id)
    if not row:
        return False
    file_id = row[7]
    file_path = row[8]
    if not file_id and not file_path:
        return False
    target_path = Path(DB_FILE).with_suffix(".imported.db")
    try:
        if file_id:
            await bot.download(file_id, destination=str(target_path))
        elif file_path and Path(file_path).exists():
            shutil.copy2(file_path, target_path)
        else:
            return False
        shutil.copy2(target_path, DB_FILE)
        await reload_settings_cache_from_db()
        return True
    except Exception:
        logger.exception("import_account_snapshot_from_channel failed")
        return False


async def reload_settings_cache_from_db():
    settings_cache.clear()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT key, value FROM settings")
        rows = await cur.fetchall()
    for k, v in rows:
        settings_cache[k] = v


# ===================== KEYBOARDLAR =====================
def admin_main_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🎬 Kino qo'shish"), KeyboardButton(text="🎫 Nomsiz kino")],
        [KeyboardButton(text="📺 Serial qo'shish"), KeyboardButton(text="⚙️ Sozlash")],
        [KeyboardButton(text="📣 Broadcast"), KeyboardButton(text="📚 Ro'yxatlar")],
        [KeyboardButton(text="🗂 DB eksport"), KeyboardButton(text="👥 Foydalanuvchilar")],
        [KeyboardButton(text="➕ Guruh qo'shish"), KeyboardButton(text="➖ Guruh o'chirish")],
        [KeyboardButton(text="👁 Join monitoring"), KeyboardButton(text="🗑 Remove Movie")],
        [KeyboardButton(text="🤖 Bu bot aklount")],
        [KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_settings_kb() -> ReplyKeyboardMarkup:
    copyright_button = "🛡 Copyright o'chirish" if copyright_is_enabled_cached() else "🛡 Copyright yoqish"
    rows = [
        [KeyboardButton(text="📣 Publish channel"), KeyboardButton(text="🚀 Eskilarni post qilish")],
        [KeyboardButton(text="📝 Caption"), KeyboardButton(text="🛠 Postni tuzatish")],
        [KeyboardButton(text="📝 Nomni tuzatish"), KeyboardButton(text="🎞 Nomsizni post qilish")],
        [KeyboardButton(text="🎬 Kinoni sozlash"), KeyboardButton(text="📺 Serialni sozlash")],
        [KeyboardButton(text="🔗 Set ssilka"), KeyboardButton(text="🧹 Reset ssilka")],
        [KeyboardButton(text=copyright_button), KeyboardButton(text="👤 Copyright ruxsatlari")],
        [KeyboardButton(text="↩️ Orqaga")],
        [KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_account_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="➕ Channel qo'shish"), KeyboardButton(text="➖ Channel o'chirish")],
        [KeyboardButton(text="📋 Channel ro'yxati"), KeyboardButton(text="💾 Akkountga saqlash")],
        [KeyboardButton(text="📂 Akkountga kirish")],
        [KeyboardButton(text="↩️ Orqaga"), KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_copyright_menu_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="➕ Ruxsat berish"), KeyboardButton(text="➖ Ruxsatni olish")],
        [KeyboardButton(text="📄 Ro'yxat")],
        [KeyboardButton(text="↩️ Orqaga"), KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_series_settings_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="♻️ Serial replace")],
        [KeyboardButton(text="➕ Serial davomiga qo'shish")],
        [KeyboardButton(text="🧩 Fasllarga bo'lish")],
        [KeyboardButton(text="↩️ Orqaga"), KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_flow_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Cancel")]],
        resize_keyboard=True,
    )


def admin_cancel_kb() -> ReplyKeyboardMarkup:
    return admin_flow_kb()


def collect_episodes_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="Tugatdim ✅")],
        [KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def groups_inline_kb(missing: List[Tuple[str, Optional[str], Optional[str], Optional[str]]]) -> InlineKeyboardMarkup:
    rows = []
    for chat_id, username, title, invite in missing:
        url = make_tg_url(invite) or make_tg_url(username)
        button_text = "Qo'shilish"
        if url:
            rows.append([InlineKeyboardButton(text=button_text, url=url)])
        else:
            rows.append([InlineKeyboardButton(text=button_text, callback_data=f"dummy:{chat_id}")])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_movie_kb(code: str, title: Optional[str]) -> InlineKeyboardMarkup:
    from urllib.parse import quote_plus
    link = fmt_page_link()
    share_text = quote_plus(build_movie_share_caption(code, title))
    share_url = f"https://t.me/share/url?url={quote_plus(link)}&text={share_text}"
    rows = [
        [InlineKeyboardButton(text="🔁 Ulashish", url=share_url)],
        [InlineKeyboardButton(text="❌ Yashirish", callback_data=f"movie:hide:{code}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_user_share_kb(code: str, title: Optional[str]) -> InlineKeyboardMarkup:
    return build_movie_kb(code, title)


def build_episodes_inline_kb(series_code: str, episodes: List[Tuple[Any, ...]], page: int = 0, per_page: int = EPISODES_PER_PAGE) -> InlineKeyboardMarkup:
    total = len(episodes)
    if total == 0:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Bo'sh", callback_data="dummy:0")]])
    pages = (total - 1) // per_page + 1
    page = max(0, min(page, pages - 1))
    start = page * per_page
    chunk = episodes[start:start + per_page]

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for ep in chunk:
        ep_num = int(ep[0])
        row.append(InlineKeyboardButton(text=f"{ep_num}-qism", callback_data=f"play:{series_code}:{ep_num}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"page:{series_code}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1}/{pages}", callback_data="dummy:page"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"page:{series_code}:{page + 1}"))
    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_series_episodes_kb(series_code: str, episodes: List[Tuple[Any, ...]], page: int = 0, per_page: int = EPISODES_PER_PAGE) -> InlineKeyboardMarkup:
    return build_episodes_inline_kb(series_code, episodes, page=page, per_page=per_page)




# ===================== COPYRIGHT / CONTENT PROTECTION =====================
def copyright_is_enabled_cached() -> bool:
    return safe_text(settings_cache.get("copyright_enabled", "0")).lower() in {"1", "true", "yes", "on"}


async def copyright_is_enabled() -> bool:
    val = await settings_get("copyright_enabled")
    return safe_text(val or "0").lower() in {"1", "true", "yes", "on"}


async def set_copyright_enabled(enabled: bool) -> None:
    await settings_set("copyright_enabled", "1" if enabled else "0")


async def is_copyright_allowed(user_id: int) -> bool:
    if int(user_id) == int(ADMIN_ID):
        return True
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT 1 FROM copyright_allowlist WHERE user_id = ?", (int(user_id),))
        return await cur.fetchone() is not None


async def add_copyright_allow_user(user_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "INSERT OR IGNORE INTO copyright_allowlist(user_id, added_at) VALUES(?, ?)",
            (int(user_id), now_utc().isoformat()),
        )
        await db.commit()
        return cur.rowcount > 0


async def remove_copyright_allow_user(user_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("DELETE FROM copyright_allowlist WHERE user_id = ?", (int(user_id),))
        await db.commit()
        return cur.rowcount > 0


async def list_copyright_allow_users() -> List[Tuple[int, str]]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT user_id, added_at FROM copyright_allowlist ORDER BY user_id ASC")
        rows = await cur.fetchall()
    return [(int(uid), safe_text(added_at)) for uid, added_at in rows]


async def should_protect_content(chat_id: int) -> bool:
    if int(chat_id) == int(ADMIN_ID):
        return False
    if not await copyright_is_enabled():
        return False
    return not await is_copyright_allowed(int(chat_id))


_original_send_message = bot.send_message
_original_send_video = bot.send_video
_original_send_document = bot.send_document
_original_send_animation = bot.send_animation
_original_send_photo = bot.send_photo
_original_send_audio = bot.send_audio
_original_send_voice = bot.send_voice


async def _send_message_wrapped(chat_id, text, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_message(chat_id, text, *args, protect_content=protect_content, **kwargs)


async def _send_video_wrapped(chat_id, video, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_video(chat_id, video, *args, protect_content=protect_content, **kwargs)


async def _send_document_wrapped(chat_id, document, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_document(chat_id, document, *args, protect_content=protect_content, **kwargs)


async def _send_animation_wrapped(chat_id, animation, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_animation(chat_id, animation, *args, protect_content=protect_content, **kwargs)


async def _send_photo_wrapped(chat_id, photo, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_photo(chat_id, photo, *args, protect_content=protect_content, **kwargs)


async def _send_audio_wrapped(chat_id, audio, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_audio(chat_id, audio, *args, protect_content=protect_content, **kwargs)


async def _send_voice_wrapped(chat_id, voice, *args, protect_content: Optional[bool] = None, **kwargs):
    if protect_content is None:
        protect_content = await should_protect_content(int(chat_id))
    return await _original_send_voice(chat_id, voice, *args, protect_content=protect_content, **kwargs)


bot.send_message = _send_message_wrapped
bot.send_video = _send_video_wrapped
bot.send_document = _send_document_wrapped
bot.send_animation = _send_animation_wrapped
bot.send_photo = _send_photo_wrapped
bot.send_audio = _send_audio_wrapped
bot.send_voice = _send_voice_wrapped


# ===================== XAVFSIZ YUBORISH =====================
async def safe_send(chat_id: int, text: str, reply_markup=None, disable_web_page_preview: bool = True):
    try:
        return await bot.send_message(
            chat_id,
            text,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
    except Exception:
        logger.exception("safe_send xatosi: %s", chat_id)
        return None


async def safe_send_media(chat_id: int, file_id: str, file_type: str, caption: Optional[str] = None, reply_markup=None):
    try:
        if file_type == "video":
            return await bot.send_video(chat_id, file_id, caption=caption, reply_markup=reply_markup)
        if file_type == "document":
            return await bot.send_document(chat_id, file_id, caption=caption, reply_markup=reply_markup)
        if file_type == "animation":
            return await bot.send_animation(chat_id, file_id, caption=caption, reply_markup=reply_markup)
        if file_type == "photo":
            return await bot.send_photo(chat_id, file_id, caption=caption, reply_markup=reply_markup)
        return await bot.send_document(chat_id, file_id, caption=caption, reply_markup=reply_markup)
    except Exception:
        logger.exception("safe_send_media xatosi: %s", chat_id)
        return None


async def send_any_media_like_message(chat_id: int, message: Message, caption: Optional[str] = None, reply_markup=None):
    try:
        if message.video:
            return await bot.send_video(chat_id, message.video.file_id, caption=caption, reply_markup=reply_markup)
        if message.document:
            return await bot.send_document(chat_id, message.document.file_id, caption=caption, reply_markup=reply_markup)
        if message.animation:
            return await bot.send_animation(chat_id, message.animation.file_id, caption=caption, reply_markup=reply_markup)
        if message.photo:
            return await bot.send_photo(chat_id, message.photo[-1].file_id, caption=caption, reply_markup=reply_markup)
        if message.audio:
            return await bot.send_audio(chat_id, message.audio.file_id, caption=caption, reply_markup=reply_markup)
        if message.voice:
            return await bot.send_voice(chat_id, message.voice.file_id, caption=caption, reply_markup=reply_markup)
        return await bot.send_message(chat_id, caption or "", reply_markup=reply_markup)
    except Exception:
        logger.exception("send_any_media_like_message xatosi: %s", chat_id)
        return None


# ===================== SUB CHECK =====================
async def check_user_all(user_id: int) -> Tuple[bool, List[Tuple[str, Optional[str], Optional[str], Optional[str]]]]:
    missing: List[Tuple[str, Optional[str], Optional[str], Optional[str]]] = []

    monitored = await list_join_monitored_db()
    groups = await list_groups_db()

    # Monitored join-request chatlar
    for chat_id, invite in monitored:
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if getattr(member, "status", None) in ("member", "administrator", "creator"):
                continue
        except Exception:
            pass

        # Pending bo'lsa, hali kutilyapti deb hisoblaymiz
        pending_rows = await list_pending_for_user_db(user_id)
        pending_chat_ids = {str(row[0]) for row in pending_rows}
        if str(chat_id) in pending_chat_ids:
            continue

        # U yerda aynan nima borligini topamiz
        found = False
        for g_chat_id, g_username, g_title, g_invite in groups:
            if str(g_chat_id) == str(chat_id):
                missing.append((g_chat_id, g_username, g_title, g_invite))
                found = True
                break
        if not found:
            missing.append((chat_id, None, None, invite))

    # Oddiy majburiy guruhlar
    for chat_id, username, title, invite in groups:
        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if getattr(member, "status", None) in ("member", "administrator", "creator"):
                continue
        except Exception:
            pass
        if (chat_id, username, title, invite) not in missing:
            missing.append((chat_id, username, title, invite))

    return (len(missing) == 0), missing


# ===================== BROADCAST =====================
async def broadcast_to_users(file_id: Optional[str], file_type: Optional[str], text: str) -> Dict[str, int]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT user_id FROM users ORDER BY user_id")
        rows = await cur.fetchall()

    success = 0
    failed = 0
    for (uid,) in rows:
        try:
            if file_id and file_type:
                await safe_send_media(uid, file_id, file_type, caption=text)
            else:
                await safe_send(uid, text)
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.03)
    return {"success": success, "failed": failed, "total": len(rows)}


# ===================== ADMIN HANDLERS =====================
async def reset_to_main_admin():
    admin_states.pop(ADMIN_ID, None)
    await safe_send(ADMIN_ID, "🔧 Admin panel.", reply_markup=admin_main_kb())


async def reset_to_settings_admin():
    admin_states[ADMIN_ID] = {"action": "settings_menu", "step": "menu"}
    await safe_send(ADMIN_ID, "⚙️ Sozlash menyusi.", reply_markup=admin_settings_kb())


async def reset_to_series_settings_admin():
    admin_states[ADMIN_ID] = {"action": "series_settings_menu", "step": "menu"}
    await safe_send(ADMIN_ID, "📺 Serial sozlash menyusi.", reply_markup=admin_series_settings_kb())


async def handle_admin_text(message: Message):
    text = safe_text(message.text)
    st = admin_states.get(ADMIN_ID)

    if text == "❌ Cancel":
        await reset_to_main_admin()
        return

    if text == "↩️ Orqaga":
        if st and st.get("action") == "series_settings_menu":
            await reset_to_settings_admin()
        elif st and st.get("action") == "copyright_menu":
            await reset_to_settings_admin()
        elif st and st.get("action") == "settings_menu":
            await reset_to_main_admin()
        elif st and st.get("action", "").startswith("series_"):
            await reset_to_series_settings_admin()
        else:
            await reset_to_main_admin()
        return

    if st and st.get("action") == "copyright_menu":
        if text == "➕ Ruxsat berish":
            admin_states[ADMIN_ID] = {"action": "copyright_add", "step": "wait_user_id"}
            await safe_send(ADMIN_ID, "Ruxsat beriladigan user ID yuboring.", reply_markup=admin_flow_kb())
            return
        if text == "➖ Ruxsatni olish":
            admin_states[ADMIN_ID] = {"action": "copyright_remove", "step": "wait_user_id"}
            await safe_send(ADMIN_ID, "Ruxsat olinadigan user ID yuboring.", reply_markup=admin_flow_kb())
            return
        if text == "📄 Ro'yxat":
            rows = await list_copyright_allow_users()
            if not rows:
                await safe_send(ADMIN_ID, "Ruxsat berilgan userlar yo'q.", reply_markup=admin_copyright_menu_kb())
                return
            lines = [f"Jami: {len(rows)}"] + [f"{uid} | {added_at or '-'}" for uid, added_at in rows]
            await safe_send(ADMIN_ID, "\n".join(lines), reply_markup=admin_copyright_menu_kb())
            return
        await safe_send(ADMIN_ID, "Copyright menyusi.", reply_markup=admin_copyright_menu_kb())
        return

    if text == "⚙️ Sozlash":
        admin_states[ADMIN_ID] = {"action": "settings_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "Qaysi bo'limni sozlaymiz?", reply_markup=admin_settings_kb())
        return

    if text == "📣 Publish channel":
        ch = await settings_get("publish_channel")
        if ch:
            await safe_send(ADMIN_ID, f"Publish channel allaqachon qo'shilgan: {ch}", reply_markup=admin_settings_kb())
            return
        admin_states[ADMIN_ID] = {"action": "set_publish_channel", "step": "wait_channel"}
        await safe_send(ADMIN_ID, "Publish channel username yoki id yuboring. Masalan: @kanal yoki -1001234567890", reply_markup=admin_flow_kb())
        return

    if text == "🚀 Eskilarni post qilish":
        admin_states[ADMIN_ID] = {"action": "publish_old", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Eski kino yoki serial kodini yuboring. Hammasini birin-birin post qilish uchun +++ yozing.", reply_markup=admin_flow_kb())
        return

    if text == "📝 Caption":
        admin_states[ADMIN_ID] = {"action": "caption_fix", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Captionini o'zgartirmoqchi bo'lgan kino kodini yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "🛠 Postni tuzatish":
        admin_states[ADMIN_ID] = {"action": "fix_post", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Tuzatish uchun kino yoki serial kodini yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "🎞 Nomsizni post qilish":
        admin_states[ADMIN_ID] = {"action": "manual_nameless", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Nomsiz kino kodini yuboring.", reply_markup=admin_flow_kb())
        return

    if text in {"🛠 Postni sozlash", "🛠 Postni tuzatish"}:
        admin_states[ADMIN_ID] = {"action": "fix_post", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Tuzatish uchun kodni yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "📝 Nomni tuzatish":
        admin_states[ADMIN_ID] = {"action": "rename_post", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Nomini tuzatmoqchi bo'lgan kino yoki serial kodini yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "🎬 Kinoni sozlash":
        admin_states[ADMIN_ID] = {"action": "movie_replace", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Kinoni replace qilish uchun kodni yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "📺 Serialni sozlash":
        admin_states[ADMIN_ID] = {"action": "series_settings_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "Serial bo'limi tanlandi.", reply_markup=admin_series_settings_kb())
        return

    if text == "🔗 Set ssilka":
        admin_states[ADMIN_ID] = {"action": "set_share", "step": "wait_share"}
        await safe_send(ADMIN_ID, "Bizning sahifamizda chiqadigan ssilkani yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "🧹 Reset ssilka":
        await settings_set("codes_link", "")
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, "✅ Ssilka reset qilindi.", reply_markup=admin_settings_kb())
        return

    if text in {"🛡 Copyright yoqish", "🛡 Copyright o'chirish"}:
        enabled = await copyright_is_enabled()
        await set_copyright_enabled(not enabled)
        admin_states.pop(ADMIN_ID, None)
        msg = "✅ Copyright yoqildi." if not enabled else "✅ Copyright o'chirildi."
        await safe_send(ADMIN_ID, msg, reply_markup=admin_settings_kb())
        return

    if text == "👤 Copyright ruxsatlari":
        admin_states[ADMIN_ID] = {"action": "copyright_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "Copyright ruxsatlari bo'limi.", reply_markup=admin_copyright_menu_kb())
        return

    if text == "🤖 Bu bot aklount":
        admin_states[ADMIN_ID] = {"action": "account_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "Akkount boshqaruvi.", reply_markup=admin_account_kb())
        return

    if text == "♻️ Serial replace":
        admin_states[ADMIN_ID] = {"action": "series_replace", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Replace qilish uchun serial kodini yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "➕ Serial davomiga qo'shish":
        admin_states[ADMIN_ID] = {"action": "series_continue", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Davom qo'shish uchun serial kodini yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "🧩 Fasllarga bo'lish":
        admin_states[ADMIN_ID] = {"action": "season_split", "step": "wait_code"}
        await safe_send(ADMIN_ID, "Faslga bo'lish uchun serial kodini yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "📣 Broadcast":
        admin_states[ADMIN_ID] = {"action": "broadcast", "step": "wait_media", "file_id": None, "file_type": None, "caption": None}
        await safe_send(ADMIN_ID, "Broadcast uchun media/video yuboring. Media bo'lmasa oddiy matn yuborishingiz ham mumkin.", reply_markup=admin_flow_kb())
        return

    if text == "📚 Ro'yxatlar":
        await safe_send(
            ADMIN_ID,
            "Qaysi ro'yxat kerak?\n• 🎬 Kinolar\n• 📺 Seriallar",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🎬 Kinolar ro'yxati"), KeyboardButton(text="📺 Seriallar ro'yxati")],
                    [KeyboardButton(text="↩️ Orqaga"), KeyboardButton(text="❌ Cancel")],
                ],
                resize_keyboard=True,
            ),
        )
        return

    if text == "🎬 Kinolar ro'yxati":
        path = await make_movies_report_file()
        await bot.send_document(ADMIN_ID, FSInputFile(path), caption="🎬 Kinolar ro'yxati")
        return

    if text == "📺 Seriallar ro'yxati":
        path = await make_series_report_file()
        await bot.send_document(ADMIN_ID, FSInputFile(path), caption="📺 Seriallar ro'yxati")
        return

    if text == "🗂 DB eksport":
        path = await export_db_copy()
        await bot.send_document(ADMIN_ID, FSInputFile(path), caption="🗂 Database backup")
        return

    if text == "👥 Foydalanuvchilar":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT COUNT(*) FROM users")
            total = int((await cur.fetchone())[0] or 0)
            cur2 = await db.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 200")
            rows = await cur2.fetchall()
        sample = "\n".join(str(r[0]) for r in rows) if rows else "Topilmadi."
        await safe_send(ADMIN_ID, f"Foydalanuvchilar soni: {total}\n\nBirinchi ID lar:\n{sample}", reply_markup=admin_main_kb())
        return

    if text == "➕ Guruh qo'shish":
        admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_link"}
        await safe_send(ADMIN_ID, "Guruh linki yoki @username yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "➖ Guruh o'chirish":
        admin_states[ADMIN_ID] = {"action": "remove_group", "step": "wait_chatid"}
        await safe_send(ADMIN_ID, "O'chirish uchun chat_id yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "👁 Join monitoring":
        await safe_send(
            ADMIN_ID,
            "Qaysi amal kerak?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="➕ Monitoring qo'shish"), KeyboardButton(text="➖ Monitoring o'chirish")],
                    [KeyboardButton(text="📄 Monitoring ro'yxati")],
                    [KeyboardButton(text="↩️ Orqaga"), KeyboardButton(text="❌ Cancel")],
                ],
                resize_keyboard=True,
            ),
        )
        return

    if text == "➕ Monitoring qo'shish":
        admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_link"}
        await safe_send(ADMIN_ID, "Join-request monitoring uchun invite link yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "➖ Monitoring o'chirish":
        admin_states[ADMIN_ID] = {"action": "remove_join", "step": "wait_chatid"}
        await safe_send(ADMIN_ID, "Monitoringdan o'chirish uchun chat_id yuboring:", reply_markup=admin_flow_kb())
        return

    if text == "📄 Monitoring ro'yxati":
        rows = await list_join_monitored_db()
        if not rows:
            await safe_send(ADMIN_ID, "Monitoring ro'yxati bo'sh.", reply_markup=admin_main_kb())
            return
        lines = [f"{i+1}. {chat_id} | {invite or '-'}" for i, (chat_id, invite) in enumerate(rows)]
        await safe_send(ADMIN_ID, "Monitoring ro'yxati:\n\n" + "\n".join(lines), reply_markup=admin_main_kb())
        return

    if text == "🗑 Remove Movie":
        admin_states[ADMIN_ID] = {"action": "remove_movie", "step": "wait_code"}
        await safe_send(ADMIN_ID, "O'chirish uchun kod yuboring (kino: 100, serial: 1000, qism: 1000-1):", reply_markup=admin_flow_kb())
        return

    # ===== KINO QO'SHISH =====
    if text == "🎬 Kino qo'shish":
        admin_states[ADMIN_ID] = {"action": "add_movie", "step": "wait_media"}
        await safe_send(ADMIN_ID, "Kino uchun media/video yuboring.", reply_markup=admin_flow_kb())
        return

    if text == "🎫 Nomsiz kino":
        admin_states[ADMIN_ID] = {"action": "add_nameless", "step": "wait_media"}
        await safe_send(ADMIN_ID, "Nomsiz kino uchun media/video yuboring.", reply_markup=admin_flow_kb())
        return

    # ===== SERIAL QO'SHISH =====
    if text == "📺 Serial qo'shish":
        admin_states[ADMIN_ID] = {"action": "add_series", "step": "collect_episodes", "episodes": []}
        await safe_send(ADMIN_ID, "Serial epizodlarini ketma-ket yuboring. Tugatgach 'Tugatdim ✅' ni bosing.", reply_markup=collect_episodes_kb())
        return

    await safe_send(ADMIN_ID, "Admin: menyudan amal tanlang.", reply_markup=admin_main_kb())


async def handle_admin_stateful(message: Message):
    text = safe_text(message.text)
    st = admin_states.get(ADMIN_ID)
    if not st:
        await handle_admin_text(message)
        return

    action = st.get("action")
    step = st.get("step")

    # Har qanday faol jarayonda Cancel/Back avval ushlanadi.
    if text == "❌ Cancel":
        if action == "series_settings_menu":
            await reset_to_main_admin()
        elif action == "settings_menu":
            await reset_to_main_admin()
        elif action in {"movie_replace", "add_movie", "add_nameless", "add_group", "remove_group", "add_join", "remove_join", "remove_movie", "broadcast", "add_series", "publish_old", "caption_fix", "manual_nameless"}:
            await reset_to_main_admin()
        elif action in {"series_replace", "series_continue", "season_split"}:
            await reset_to_series_settings_admin()
        else:
            await reset_to_main_admin()
        return

    if text == "↩️ Orqaga":
        if action == "series_settings_menu":
            await reset_to_settings_admin()
        elif action == "settings_menu":
            await reset_to_main_admin()
        elif action in {"series_replace", "series_continue", "season_split"}:
            await reset_to_series_settings_admin()
        else:
            await reset_to_main_admin()
        return

    if action in {"settings_menu", "series_settings_menu", "copyright_menu"} and step == "menu":
        await handle_admin_text(message)
        return

    if action == "set_publish_channel" and step == "wait_channel":
        channel = normalize_channel_input(text)
        if not channel:
            await safe_send(ADMIN_ID, "Kanal noto'g'ri.", reply_markup=admin_cancel_kb())
            return
        existing = await settings_get("publish_channel")
        if existing:
            await safe_send(ADMIN_ID, f"Publish channel allaqachon mavjud: {existing}", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        if not await is_bot_admin_in_channel(channel):
            await safe_send(ADMIN_ID, f"⚠️ Bot ushbu kanalda admin emas yoki kanal topilmadi: {channel}", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await settings_set("publish_channel", channel)
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"✅ Publish channel saqlandi: {channel}", reply_markup=admin_settings_kb())
        await load_pending_publish_jobs()
        return

    if action == "fix_post" and step == "wait_code":
        code = text.strip()
        post = await get_publish_post(code)
        if not post:
            await safe_send(ADMIN_ID, "Bunday post topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        kind = post[0]
        status = post[2]
        reason = post[6]
        if kind == "nameless":
            await safe_send(ADMIN_ID, "Nomsiz postda tuzatish kerak emas.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "fix_post", "step": "wait_original_title", "code": code, "kind": kind}
        await safe_send(ADMIN_ID, f"Topildi. Holat: {status}\nSabab: {reason or '-'}\nEndi original inglizcha nom yuboring:", reply_markup=admin_cancel_kb())
        return

    if action == "fix_post" and step == "wait_original_title":
        original_title = safe_text(text)
        if not original_title:
            await safe_send(ADMIN_ID, "Nom bo'sh bo'lmasin.", reply_markup=admin_cancel_kb())
            return
        code = st.get("code")
        kind = st.get("kind")
        details = await resolve_tmdb(kind, original_title)
        if not details:
            await safe_send(ADMIN_ID, "TMDb topmadi. Yana bir marta to'g'ri original nom yuboring.", reply_markup=admin_cancel_kb())
            return
        old = await get_publish_post(code)
        old_channel = old[7] if old else None
        old_message_id = old[8] if old else None
        try:
            if old_channel and old_message_id:
                await bot.delete_message(old_channel, int(old_message_id))
        except Exception:
            pass
        publish_channel = safe_text(await settings_get("publish_channel"))
        if kind == "movie":
            caption, poster_url, title_uz = await build_channel_movie_caption(code, details, original_title)
            genres = normalize_tmdb_name_list(details.get("genres"))
            countries = normalize_tmdb_name_list(details.get("production_countries"))
            year = (details.get("release_date") or "")[:4] or None
            overview = details.get("overview") or None
            genre_uz = await translate_genres_to_uz(genres) if genres else None
            country_uz = await translate_countries_to_uz(countries) if countries else None
            overview_uz = truncate_text(await translate_text(overview, "uz") if overview else overview, 800)
            mv = await get_movie_db(code)
            if mv:
                _, file_id, file_type, _, _, quality, _, _, _, _ = mv
                await update_movie_meta_db(code, title_uz or original_title, year, genre_uz, quality, "uzbek", overview_uz, country_uz)
            if poster_url:
                sent = await bot.send_photo(publish_channel, poster_url, caption=caption, parse_mode="HTML", reply_markup=build_public_inline_kb("movie", code))
            else:
                sent = await bot.send_message(publish_channel, caption, parse_mode="HTML", reply_markup=build_public_inline_kb("movie", code), disable_web_page_preview=True)
            await update_publish_post_status(code, status="published", channel_id=publish_channel, message_id=sent.message_id, poster_url=poster_url, caption=caption, title=title_uz, tmdb_id=str(details.get("id")), tmdb_type="movie", reason=None)
            await safe_send(ADMIN_ID, f"✅ Film posti tuzatildi: {code}", reply_markup=admin_settings_kb())
        else:
            caption, poster_url, title_uz = await build_channel_series_caption(code, details, original_title)
            genres = normalize_tmdb_name_list(details.get("genres"))
            countries = normalize_tmdb_name_list(details.get("origin_country"))
            if not countries:
                countries = normalize_tmdb_name_list(details.get("production_countries"))
            overview = details.get("overview") or None
            genre_uz = await translate_genres_to_uz(genres) if genres else None
            country_uz = await translate_countries_to_uz(countries) if countries else None
            overview_uz = truncate_text(await translate_text(overview, "uz") if overview else overview, 800)
            await update_series_meta_db(code, title_uz or original_title, "uzbek", overview_uz)
            if poster_url:
                sent = await bot.send_photo(publish_channel, poster_url, caption=caption, parse_mode="HTML", reply_markup=build_public_inline_kb("series", code))
            else:
                sent = await bot.send_message(publish_channel, caption, parse_mode="HTML", reply_markup=build_public_inline_kb("series", code), disable_web_page_preview=True)
            await update_publish_post_status(code, status="published", channel_id=publish_channel, message_id=sent.message_id, poster_url=poster_url, caption=caption, title=title_uz, tmdb_id=str(details.get("id")), tmdb_type="tv", reason=None)
            await safe_send(ADMIN_ID, f"✅ Serial posti tuzatildi: {code}", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "rename_post" and step == "wait_code":
        code = text.strip()
        post = await get_publish_post(code)
        kind = post[0] if post else None
        if not kind:
            if await get_movie_db(code):
                kind = "movie"
            elif await get_series_meta(code):
                kind = "series"
        if not kind:
            await safe_send(ADMIN_ID, "Bunday kino yoki serial topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        if kind == "nameless":
            await safe_send(ADMIN_ID, "Nomsiz videoda nom yo'q.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "rename_post", "step": "wait_title", "code": code, "kind": kind}
        await safe_send(ADMIN_ID, f"Topildi. Endi yangi nomni yuboring.\nKod: {code}\nTur: {kind}", reply_markup=admin_flow_kb())
        return

    if action == "rename_post" and step == "wait_title":
        new_title = safe_text(text)
        if not new_title:
            await safe_send(ADMIN_ID, "Nom bo'sh bo'lmasin.", reply_markup=admin_cancel_kb())
            return
        code = st.get("code")
        kind = st.get("kind")
        ok = await apply_manual_title_fix(code, kind, new_title)
        if not ok:
            await safe_send(ADMIN_ID, "Nomni tuzatishda xatolik bo'ldi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await safe_send(ADMIN_ID, f"✅ Nom yangilandi.\nKod: {code}\nYangi nom: {new_title}", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "copyright_add" and step == "wait_user_id":
        try:
            user_id = int(text.strip())
        except Exception:
            await safe_send(ADMIN_ID, "User ID faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        added = await add_copyright_allow_user(user_id)
        admin_states.pop(ADMIN_ID, None)
        if added:
            await safe_send(ADMIN_ID, f"✅ Ruxsat berildi: {user_id}", reply_markup=admin_copyright_menu_kb())
        else:
            await safe_send(ADMIN_ID, f"User oldinroq ro'yxatda bor edi: {user_id}", reply_markup=admin_copyright_menu_kb())
        return

    if action == "copyright_remove" and step == "wait_user_id":
        try:
            user_id = int(text.strip())
        except Exception:
            await safe_send(ADMIN_ID, "User ID faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        removed = await remove_copyright_allow_user(user_id)
        admin_states.pop(ADMIN_ID, None)
        if removed:
            await safe_send(ADMIN_ID, f"✅ Ruxsat olib tashlandi: {user_id}", reply_markup=admin_copyright_menu_kb())
        else:
            await safe_send(ADMIN_ID, f"User ro'yxatda topilmadi: {user_id}", reply_markup=admin_copyright_menu_kb())
        return

    # --- GROUPS ---
    if action == "add_group" and step == "wait_link":
        invite = make_tg_url(text) or text
        admin_states[ADMIN_ID] = {"action": "add_group", "step": "wait_chatid", "invite": invite}
        await safe_send(ADMIN_ID, "Endi chat_id yuboring (masalan: -1001234567890).", reply_markup=admin_flow_kb())
        return

    if action == "add_group" and step == "wait_chatid":
        invite = st.get("invite")
        try:
            chat_id = str(int(text))
        except Exception:
            if re.fullmatch(r"-?\d{5,}", text):
                chat_id = text
            else:
                await safe_send(ADMIN_ID, "Chat ID noto'g'ri.", reply_markup=admin_flow_kb())
                return
        try:
            try:
                ch = await bot.get_chat(chat_id)
                username = getattr(ch, "username", None)
                title = getattr(ch, "title", None)
            except Exception:
                username = None
                title = None
            await add_group_db(chat_id, username, title, invite)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"Guruh saqlandi: {chat_id}", reply_markup=admin_main_kb())
        except Exception:
            logger.exception("add_group error")
            await safe_send(ADMIN_ID, "Guruh saqlashda xatolik.", reply_markup=admin_main_kb())
        return

    if action == "remove_group" and step == "wait_chatid":
        admin_states.pop(ADMIN_ID, None)
        await remove_group_db(text)
        await safe_send(ADMIN_ID, f"Guruh o'chirildi: {text}", reply_markup=admin_main_kb())
        return

    if action == "add_join" and step == "wait_link":
        invite = make_tg_url(text) or text
        admin_states[ADMIN_ID] = {"action": "add_join", "step": "wait_chatid", "invite": invite}
        await safe_send(ADMIN_ID, "Endi monitoring chat_id yuboring.", reply_markup=admin_flow_kb())
        return

    if action == "add_join" and step == "wait_chatid":
        invite = st.get("invite")
        try:
            chat_id = str(int(text))
        except Exception:
            if re.fullmatch(r"-?\d{5,}", text):
                chat_id = text
            else:
                await safe_send(ADMIN_ID, "Chat ID noto'g'ri.", reply_markup=admin_flow_kb())
                return
        await add_join_monitored_db(chat_id, invite)
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"Monitoring qo'shildi: {chat_id}", reply_markup=admin_main_kb())
        return

    if action == "remove_join" and step == "wait_chatid":
        admin_states.pop(ADMIN_ID, None)
        await remove_join_monitored_db(text)
        await safe_send(ADMIN_ID, f"Monitoring o'chirildi: {text}", reply_markup=admin_main_kb())
        return

    # --- SHARE LINK ---
    if action == "set_share" and step == "wait_share":
        await settings_set("codes_link", text.strip())
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"Share link saqlandi: {text.strip()}", reply_markup=admin_main_kb())
        return

    if action == "remove_share" and step == "confirm":
        await settings_set("codes_link", "")
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, "Share link o'chirildi.", reply_markup=admin_main_kb())
        return

    if action == "publish_channel_replace" and step == "confirm":
        if text == "Yo'q":
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, "✅ Publish channel o'zgartirish bekor qilindi.", reply_markup=admin_settings_kb())
            return
        if text == "Ha":
            admin_states[ADMIN_ID] = {"action": "publish_channel_replace", "step": "wait_channel"}
            await safe_send(ADMIN_ID, "Yangi publish channel username yoki id yuboring.", reply_markup=admin_flow_kb())
            return
        await safe_send(ADMIN_ID, "Ha yoki Yo'q ni tanlang.", reply_markup=admin_flow_kb())
        return

    if action == "publish_channel_replace" and step == "wait_channel":
        channel = normalize_channel_input(text)
        if not channel:
            await safe_send(ADMIN_ID, "Kanal noto'g'ri.", reply_markup=admin_flow_kb())
            return
        if not await is_bot_admin_in_channel(channel):
            await safe_send(ADMIN_ID, "Bot bu kanalda admin emas. Avval admin qiling va qayta urinib ko'ring.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        old_channel = safe_text(await settings_get("publish_channel"))
        await settings_set("publish_channel", channel)
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"✅ Publish channel yangilandi: {channel}", reply_markup=admin_settings_kb())
        if old_channel and old_channel != channel:
            await transfer_publish_posts(old_channel, channel)
        return

    if action == "account_menu":
        if text == "➕ Channel qo'shish":
            admin_states[ADMIN_ID] = {"action": "account_add_channel", "step": "wait_channel"}
            await safe_send(ADMIN_ID, "Channel chat_id yuboring. Bot u yerda admin bo'lishi kerak.", reply_markup=admin_flow_kb())
            return
        if text == "➖ Channel o'chirish":
            admin_states[ADMIN_ID] = {"action": "account_remove_channel", "step": "wait_channel"}
            await safe_send(ADMIN_ID, "O'chiriladigan channel chat_id yuboring.", reply_markup=admin_flow_kb())
            return
        if text == "📋 Channel ro'yxati":
            rows = await list_managed_channels_db()
            if not rows:
                await safe_send(ADMIN_ID, "Ro'yxat bo'sh.", reply_markup=admin_account_kb())
                return
            lines = ["📋 Channel ro'yxati:"]
            for chat_id, title, username, purpose, status, added_at, updated_at in rows:
                lines.append(f"• {chat_id} | {title or '-'} | @{username or '-'} | {purpose} | {status}")
            await safe_send(ADMIN_ID, "\n".join(lines), reply_markup=admin_account_kb())
            return
        if text == "💾 Akkountga saqlash":
            admin_states[ADMIN_ID] = {"action": "account_save", "step": "confirm"}
            await safe_send(ADMIN_ID, "Akkountni saqlashni yoqamizmi?", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Ha"), KeyboardButton(text="Yo'q")]], resize_keyboard=True))
            return
        if text == "📂 Akkountga kirish":
            admin_states[ADMIN_ID] = {"action": "account_enter", "step": "wait_channel"}
            await safe_send(ADMIN_ID, "Kirish uchun channel chat_id yuboring.", reply_markup=admin_flow_kb())
            return

    # --- YANGI AMALLAR ---
    if action == "publish_old" and step == "wait_code":
        code = text.strip()
        if code == "+++":
            queued, skipped = await queue_old_content_for_publish()
            admin_states.pop(ADMIN_ID, None)
            await safe_send(
                ADMIN_ID,
                f"✅ Eski kontentlar navbatga qo'yildi.\nNavbatga qo'yildi: {queued}\nO'tkazib yuborildi: {skipped}",
                reply_markup=admin_settings_kb(),
            )
            return
        if not re.fullmatch(r"\d+", code):
            await safe_send(ADMIN_ID, "Kod faqat son bo'lsin yoki +++ yozing.", reply_markup=admin_flow_kb())
            return
        if await get_movie_db(code):
            post = await get_publish_post(code)
            if post and safe_text(post[2]) == "published":
                await safe_send(ADMIN_ID, "Bu kino allaqachon post qilingan.", reply_markup=admin_settings_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            data = await get_movie_db(code)
            await queue_publish("movie", code, source_title=data[0] if data else None)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Kino navbatga qo'yildi: {code}", reply_markup=admin_settings_kb())
            return
        if await get_series_meta(code):
            post = await get_publish_post(code)
            if post and safe_text(post[2]) == "published":
                await safe_send(ADMIN_ID, "Bu serial allaqachon post qilingan.", reply_markup=admin_settings_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            meta = await get_series_meta(code)
            await queue_publish("series", code, source_title=meta[0] if meta else None)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Serial navbatga qo'yildi: {code}", reply_markup=admin_settings_kb())
            return
        await safe_send(ADMIN_ID, "Bunday kino yoki serial topilmadi.", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "caption_fix" and step == "wait_code":
        code = text.strip()
        movie = await get_movie_db(code)
        post = await get_publish_post(code)
        if not movie or movie[0] is None:
            await safe_send(ADMIN_ID, "Bunday namli kino topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        if not post or safe_text(post[0]) != "movie" or safe_text(post[2]) != "published":
            await safe_send(ADMIN_ID, "Bu kino hali post qilinmagan yoki caption o'zgartirishga tayyor emas.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "caption_fix", "step": "wait_caption", "code": code}
        await safe_send(ADMIN_ID, "Yangi caption/izohni yuboring.", reply_markup=admin_flow_kb())
        return

    if action == "caption_fix" and step == "wait_caption":
        new_caption = safe_text(text)
        if not new_caption:
            await safe_send(ADMIN_ID, "Caption bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        code = st.get("code")
        ok = await edit_published_movie_caption(code, new_caption)
        if not ok:
            await safe_send(ADMIN_ID, "Captionni yangilashda xatolik bo'ldi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await safe_send(ADMIN_ID, f"✅ Caption yangilandi. Kod: {code}", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "fix_post" and step == "wait_code":
        code = text.strip()
        post = await get_publish_post(code)
        if not post:
            await safe_send(ADMIN_ID, "Bunday post topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        if safe_text(post[0]) == "movie":
            mv = await get_movie_db(code)
            if not mv:
                await safe_send(ADMIN_ID, "Kino ma'lumoti topilmadi.", reply_markup=admin_settings_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            admin_states[ADMIN_ID] = {"action": "fix_post", "step": "wait_new_caption", "code": code, "kind": "movie"}
            await safe_send(ADMIN_ID, "Yangi caption yuboring.", reply_markup=admin_flow_kb())
            return
        if safe_text(post[0]) == "series":
            meta = await get_series_meta(code)
            if not meta:
                await safe_send(ADMIN_ID, "Serial ma'lumoti topilmadi.", reply_markup=admin_settings_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            admin_states[ADMIN_ID] = {"action": "fix_post", "step": "wait_new_caption", "code": code, "kind": "series"}
            await safe_send(ADMIN_ID, "Yangi caption yuboring.", reply_markup=admin_flow_kb())
            return
        await safe_send(ADMIN_ID, "Bu post turi hozircha qo'llab-quvvatlanmaydi.", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "fix_post" and step == "wait_new_caption":
        code = st.get("code")
        new_caption = safe_text(text)
        if not new_caption:
            await safe_send(ADMIN_ID, "Caption bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        post = await get_publish_post(code)
        if not post:
            await safe_send(ADMIN_ID, "Post topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        kind = safe_text(post[0])
        channel_id = safe_text(post[7])
        message_id = post[8]
        try:
            if kind == "movie":
                await bot.edit_message_caption(chat_id=channel_id, message_id=int(message_id), caption=new_caption, parse_mode="HTML", reply_markup=build_public_inline_kb(kind, code))
            else:
                await bot.edit_message_text(chat_id=channel_id, message_id=int(message_id), text=new_caption, parse_mode="HTML", reply_markup=build_public_inline_kb(kind, code), disable_web_page_preview=True)
            await update_publish_post_status(code, caption=new_caption)
            await safe_send(ADMIN_ID, f"✅ Post yangilandi. Kod: {code}", reply_markup=admin_settings_kb())
        except Exception:
            logger.exception("fix_post update failed")
            await safe_send(ADMIN_ID, "Post yangilashda xatolik bo'ldi.", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "manual_nameless" and step == "wait_code":
        code = text.strip()
        movie = await get_movie_db(code)
        if not movie or movie[0] is not None:
            await safe_send(ADMIN_ID, "Bunday nomsiz kod topilmadi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        post = await get_publish_post(code)
        if post and safe_text(post[0]) == "nameless" and safe_text(post[2]) == "published":
            admin_states[ADMIN_ID] = {"action": "manual_nameless", "step": "confirm_replace", "code": code}
            await safe_send(ADMIN_ID, "Bu nameless allaqachon post qilingan. Video/rasmini almashtirasizmi?", reply_markup=build_yes_no_inline_kb("nm_replace", code))
            return
        admin_states[ADMIN_ID] = {"action": "manual_nameless", "step": "wait_media", "code": code, "replace_existing": False}
        await safe_send(ADMIN_ID, "Post qilish uchun video yoki rasm yuboring.", reply_markup=admin_flow_kb())
        return

    if action == "manual_nameless" and step == "wait_media":
        code = st.get("code")
        replace_existing = bool(st.get("replace_existing"))
        ok = await publish_manual_nameless_from_message(code, message, replace_existing=replace_existing)
        if not ok:
            await safe_send(ADMIN_ID, "Nomsizni post qilishda xatolik bo'ldi.", reply_markup=admin_settings_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await safe_send(ADMIN_ID, f"✅ Nomsiz post qilindi. Kod: {code}", reply_markup=admin_settings_kb())
        admin_states.pop(ADMIN_ID, None)
        return

    if action == "account_add_channel" and step == "wait_channel":
        channel = normalize_channel_input(text)
        if not channel:
            await safe_send(ADMIN_ID, "Kanal noto'g'ri.", reply_markup=admin_flow_kb())
            return
        info = await get_chat_info(channel)
        if not info.get("ok"):
            await safe_send(ADMIN_ID, "Channel topilmadi yoki bot kira olmadi.", reply_markup=admin_account_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        result = info.get("result", {})
        title = result.get("title")
        username = result.get("username")
        if await is_bot_admin_in_channel(channel):
            await add_managed_channel_db(channel, title, username, purpose="storage", status="active")
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Channel qo'shildi: {channel}", reply_markup=admin_account_kb())
        else:
            await add_managed_channel_db(channel, title, username, purpose="storage", status="pending")
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"⚠️ Channel pending holatga qo'shildi: {channel}\nBotga admin huquqi berilgach avtomatik aktivlashadi.", reply_markup=admin_account_kb())
        return

    if action == "account_remove_channel" and step == "wait_channel":
        channel = normalize_channel_input(text)
        await remove_managed_channel_db(channel)
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"✅ Channel o'chirildi: {channel}", reply_markup=admin_account_kb())
        return

    if action == "account_save" and step == "confirm":
        if text == "Yo'q":
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, "Akkount saqlash bekor qilindi.", reply_markup=admin_account_kb())
            return
        if text == "Ha":
            admin_states.pop(ADMIN_ID, None)
            ok = await export_account_snapshot_to_channels()
            if not ok:
                await safe_send(ADMIN_ID, "Snapshot saqlashda xatolik bo'ldi.", reply_markup=admin_account_kb())
            else:
                await safe_send(ADMIN_ID, "✅ Akkount saqlandi. Agar jarayon to‘xtasa, keyingi bosishda shu joyidan davom etadi.", reply_markup=admin_account_kb())
            return
        await safe_send(ADMIN_ID, "Ha yoki Yo'q ni tanlang.", reply_markup=admin_flow_kb())
        return

    if action == "account_enter" and step == "wait_channel":
        channel = normalize_channel_input(text)
        if not channel:
            await safe_send(ADMIN_ID, "Kanal noto'g'ri.", reply_markup=admin_flow_kb())
            return
        row = await get_managed_channel_db(channel)
        if not row:
            await safe_send(ADMIN_ID, "Bunday channel ro'yxatda yo'q.", reply_markup=admin_account_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        if not await is_bot_admin_in_channel(channel):
            await safe_send(ADMIN_ID, "Bot bu kanalda admin emas. Adminlik berilgach qayta urinib ko'ring.", reply_markup=admin_account_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await settings_set("publish_channel", channel)
        admin_states.pop(ADMIN_ID, None)
        await safe_send(ADMIN_ID, f"✅ Akkountga kirildi: {channel}", reply_markup=admin_settings_kb())
        return

    # --- KINO QO'SHISH ---
    if action == "add_movie" and step == "wait_media":
        file_id, file_type = extract_media_from_message(message)
        if not file_id or not file_type:
            await safe_send(ADMIN_ID, "Video, rasm yoki fayl yuboring.", reply_markup=admin_flow_kb())
            return
        admin_states[ADMIN_ID] = {
            "action": "add_movie",
            "step": "wait_title",
            "file_id": file_id,
            "file_type": file_type,
        }
        await safe_send(ADMIN_ID, "Kino nomini ingliz tilida yuboring. Masalan: Avatar The Way of Water", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_title":
        title = safe_text(text)
        if not title:
            await safe_send(ADMIN_ID, "Kino nomi bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_quality", "title": title})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Kino sifatini yuboring. Masalan: 1080 yoki 720", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_quality":
        quality = normalize_quality_input(text)
        if not quality:
            await safe_send(ADMIN_ID, "Sifat bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        data = admin_states.pop(ADMIN_ID, None) or {}
        code = await allocate_numeric_code("movie", "next_code", 100)
        await add_movie_db(
            code=code,
            title=data.get("title"),
            file_id=data.get("file_id"),
            file_type=data.get("file_type"),
            year=None,
            genre=None,
            quality=quality,
            language=None,
            description=None,
            country=None,
            source_chat_id=data.get("source_chat_id"),
            source_message_id=data.get("source_message_id"),
        )
        await queue_publish("movie", code, source_title=data.get("title"))
        await safe_send(ADMIN_ID, f"✅ Kino saqlandi. Kod: {code}", reply_markup=admin_main_kb())
        return

    # --- NOMSIZ KINO ---
    if action == "add_nameless" and step == "wait_media":
        file_id, file_type = extract_media_from_message(message)
        if not file_id or not file_type:
            await safe_send(ADMIN_ID, "Video, rasm yoki fayl yuboring.", reply_markup=admin_flow_kb())
            return
        code = await allocate_numeric_code("nameless", "next_nameless_code", 1, nameless_threshold=3000, nameless_floor=5000)
        await add_movie_db(code, None, file_id, file_type, None, None, None, None, None, None, str(message.chat.id), message.message_id)
        await cache_nameless_source(code, file_id, file_type)
        await safe_send(
            ADMIN_ID,
            f"✅ Nomsiz media saqlandi. Kod: {code}\n\nBu endi qo'lda post qilinadi.",
            reply_markup=admin_main_kb(),
        )
        return

    # --- SERIAL ADD ---
    if action == "add_series" and step == "collect_episodes":
        if text == "Tugatdim ✅":
            episodes = st.get("episodes", [])
            if not episodes:
                await safe_send(ADMIN_ID, "Hech qanday qism yuborilmadi.", reply_markup=admin_main_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            admin_states[ADMIN_ID] = {"action": "add_series", "step": "wait_meta", "episodes": episodes}
            await safe_send(ADMIN_ID, "Serial nomini yuboring. Tilni ham qo'shishingiz mumkin: Nom — uzbek", reply_markup=admin_flow_kb())
            return

        file_id = None
        file_type = None
        if message.video:
            file_id = message.video.file_id
            file_type = "video"
        elif message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.animation:
            file_id = message.animation.file_id
            file_type = "animation"
        else:
            await safe_send(ADMIN_ID, "Serial qismi uchun video yoki fayl yuboring.", reply_markup=collect_episodes_kb())
            return
        episodes = st.get("episodes", [])
        episodes.append((file_id, file_type, str(message.chat.id), message.message_id))
        st["episodes"] = episodes
        st["last_action"] = "add_series"
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, f"✅ {len(episodes)}-qism qabul qilindi.", reply_markup=collect_episodes_kb())
        return

    if action == "add_series" and step == "wait_meta":
        meta = safe_text(message.text)
        if not meta:
            await safe_send(ADMIN_ID, "Serial nomini yuboring.", reply_markup=admin_flow_kb())
            return
        title = meta
        language = None
        if "—" in meta:
            left, right = [p.strip() for p in meta.split("—", 1)]
            title = left
            language = normalize_language_input(right)
        elif "-" in meta:
            left, right = [p.strip() for p in meta.split("-", 1)]
            title = left
            language = normalize_language_input(right)
        if not language:
            await safe_send(ADMIN_ID, "Tilni ham yuboring. Masalan: Serial nomi — uzbek", reply_markup=admin_flow_kb())
            return
        data = admin_states.pop(ADMIN_ID, None) or {}
        episodes = data.get("episodes", [])
        series_code = await allocate_numeric_code("series", "next_series_code", 1000)
        await add_series_db(series_code, title, language, None)
        for fid, ftype, src_chat, src_msg in episodes:
            ep_num = await allocate_episode_number(series_code)
            await add_episode_db(series_code, ep_num, fid, ftype, None, src_chat, src_msg)
        await queue_publish("series", series_code, source_title=title)
        await safe_send(ADMIN_ID, f"✅ Serial saqlandi. Kod: {series_code}\nJami qism: {len(episodes)}", reply_markup=admin_main_kb())
        return

    # --- MOVIE REPLACE ---
    if action == "movie_replace" and step == "wait_code":
        if not re.fullmatch(r"\d+", text):
            await safe_send(ADMIN_ID, "Kod faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        mv = await get_movie_db(text)
        if not mv:
            await safe_send(ADMIN_ID, "Bunday kino topilmadi.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "movie_replace", "step": "wait_media", "code": text}
        await safe_send(ADMIN_ID, "Yangi media/video yuboring.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_media":
        file_id = None
        file_type = None
        if message.video:
            file_id = message.video.file_id
            file_type = "video"
        elif message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.animation:
            file_id = message.animation.file_id
            file_type = "animation"
        else:
            await safe_send(ADMIN_ID, "Video yoki fayl yuboring.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_title", "file_id": file_id, "file_type": file_type})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi kino nomini yuboring.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_title":
        title = normalize_optional_text(text)
        if not title:
            await safe_send(ADMIN_ID, "Nom bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_language", "title": title})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi tilni yuboring. Masalan: uzbek", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_language":
        language = normalize_language_input(text)
        if not language:
            await safe_send(ADMIN_ID, "Til bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_quality", "language": language})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi sifatni yuboring. Masalan: 1080", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_quality":
        quality = normalize_quality_input(text)
        if not quality:
            await safe_send(ADMIN_ID, "Sifat bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_genre", "quality": quality})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi janr yuboring. '-' bo'lsa bo'sh qoldiriladi.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_genre":
        st.update({"step": "wait_country", "genre": normalize_optional_text(text)})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi davlat yuboring. '-' bo'lsa bo'sh qoldiriladi.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_country":
        st.update({"step": "wait_year", "country": normalize_optional_text(text)})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi yil yuboring. '-' bo'lsa bo'sh qoldiriladi.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_year":
        st.update({"step": "wait_description", "year": normalize_optional_text(text)})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yangi izoh yuboring. '-' bo'lsa izoh bo'lmaydi.", reply_markup=admin_flow_kb())
        return

    if action == "movie_replace" and step == "wait_description":
        description = normalize_optional_text(text)
        data = admin_states.pop(ADMIN_ID, None) or {}
        await add_movie_db(
            code=data.get("code"),
            title=data.get("title"),
            file_id=data.get("file_id"),
            file_type=data.get("file_type"),
            year=data.get("year"),
            genre=data.get("genre"),
            quality=data.get("quality"),
            language=data.get("language"),
            description=description,
            country=data.get("country"),
            source_chat_id=data.get("source_chat_id"),
            source_message_id=data.get("source_message_id"),
        )
        await safe_send(ADMIN_ID, f"✅ Kino yangilandi. Kod: {data.get('code')}", reply_markup=admin_main_kb())
        return

    # --- SERIAL REPLACE ---
    if action == "series_replace" and step == "wait_code":
        if not re.fullmatch(r"\d+", text):
            await safe_send(ADMIN_ID, "Kod faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        meta = await get_series_meta(text)
        if not meta:
            await safe_send(ADMIN_ID, "Bunday serial topilmadi.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        await clear_series_episodes_db(text)
        admin_states[ADMIN_ID] = {"action": "series_replace", "step": "collect_episodes", "series_code": text, "episodes": []}
        await safe_send(ADMIN_ID, "Yangi serial qismlarini yuboring. Tugatgach 'Tugatdim ✅' bosing.", reply_markup=collect_episodes_kb())
        return

    if action == "series_replace" and step == "collect_episodes":
        if text == "Tugatdim ✅":
            eps = st.get("episodes", [])
            if not eps:
                await safe_send(ADMIN_ID, "Hech qanday qism yuborilmadi.", reply_markup=admin_main_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            admin_states[ADMIN_ID] = {"action": "series_replace", "step": "wait_meta", "series_code": st.get("series_code"), "episodes": eps}
            await safe_send(ADMIN_ID, "Serial nomi va tilini yuboring. Masalan: Serial nomi — uzbek", reply_markup=admin_flow_kb())
            return

        file_id = None
        file_type = None
        if message.video:
            file_id = message.video.file_id
            file_type = "video"
        elif message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.animation:
            file_id = message.animation.file_id
            file_type = "animation"
        else:
            await safe_send(ADMIN_ID, "Video yoki fayl yuboring.", reply_markup=collect_episodes_kb())
            return
        eps = st.get("episodes", [])
        eps.append((file_id, file_type, str(message.chat.id), message.message_id))
        st["episodes"] = eps
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, f"✅ {len(eps)}-qism qabul qilindi.", reply_markup=collect_episodes_kb())
        return

    if action == "series_replace" and step == "wait_meta":
        meta = safe_text(text)
        if not meta:
            await safe_send(ADMIN_ID, "Serial nomi va tilini yuboring.", reply_markup=admin_flow_kb())
            return
        title = meta
        language = None
        if "—" in meta:
            left, right = [p.strip() for p in meta.split("—", 1)]
            title = left
            language = normalize_language_input(right)
        elif "-" in meta:
            left, right = [p.strip() for p in meta.split("-", 1)]
            title = left
            language = normalize_language_input(right)
        if not language:
            await safe_send(ADMIN_ID, "Tilni ham yuboring. Masalan: Serial nomi — uzbek", reply_markup=admin_flow_kb())
            return
        data = admin_states.pop(ADMIN_ID, None) or {}
        series_code = data.get("series_code")
        await replace_series_meta_db(series_code, title, language, None)
        for fid, ftype, src_chat, src_msg in data.get("episodes", []):
            ep_num = await allocate_episode_number(series_code)
            await add_episode_db(series_code, ep_num, fid, ftype, None, src_chat, src_msg)
        await safe_send(ADMIN_ID, f"✅ Serial replace qilindi. Kod: {series_code}", reply_markup=admin_main_kb())
        return

    # --- SERIAL CONTINUE ---
    if action == "series_continue" and step == "wait_code":
        if not re.fullmatch(r"\d+", text):
            await safe_send(ADMIN_ID, "Kod faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        meta = await get_series_meta(text)
        if not meta:
            await safe_send(ADMIN_ID, "Bunday serial topilmadi.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "series_continue", "step": "collect_episodes", "series_code": text, "episodes": []}
        await safe_send(ADMIN_ID, "Davom qismi uchun epizodlarni yuboring. Tugatgach 'Tugatdim ✅' bosing.", reply_markup=collect_episodes_kb())
        return

    if action == "series_continue" and step == "collect_episodes":
        if text == "Tugatdim ✅":
            eps = st.get("episodes", [])
            if not eps:
                await safe_send(ADMIN_ID, "Hech qanday qism yuborilmadi.", reply_markup=admin_main_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            series_code = st.get("series_code")
            start_num = await get_max_episode_number(series_code) + 1
            allocated_start = None
            for fid, ftype, src_chat, src_msg in eps:
                ep_num = await allocate_episode_number(series_code)
                if allocated_start is None:
                    allocated_start = ep_num
                await add_episode_db(series_code, ep_num, fid, ftype, None, src_chat, src_msg)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Serial davomiga {len(eps)} qism qo'shildi. Boshlanish: {allocated_start if allocated_start is not None else start_num}", reply_markup=admin_main_kb())
            return

        file_id = None
        file_type = None
        if message.video:
            file_id = message.video.file_id
            file_type = "video"
        elif message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.animation:
            file_id = message.animation.file_id
            file_type = "animation"
        else:
            await safe_send(ADMIN_ID, "Video yoki fayl yuboring.", reply_markup=collect_episodes_kb())
            return
        eps = st.get("episodes", [])
        eps.append((file_id, file_type, str(message.chat.id), message.message_id))
        st["episodes"] = eps
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, f"✅ {len(eps)}-yangi qism qabul qilindi.", reply_markup=collect_episodes_kb())
        return

    # --- SEASON SPLIT ---
    if action == "season_split" and step == "wait_code":
        if not re.fullmatch(r"\d+", text):
            await safe_send(ADMIN_ID, "Kod faqat son bo'lsin.", reply_markup=admin_flow_kb())
            return
        meta = await get_series_meta(text)
        if not meta:
            await safe_send(ADMIN_ID, "Bunday serial topilmadi.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        eps = await get_series_episodes(text)
        if not eps:
            await safe_send(ADMIN_ID, "Serialda qism yo'q.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        admin_states[ADMIN_ID] = {"action": "season_split", "step": "wait_range", "series_code": text, "ranges": []}
        await safe_send(
            ADMIN_ID,
            "1-fasl uchun diapazon yuboring. Masalan: 1-17\nKeyingi fasl uchun ham xuddi shu uslubda yuborasiz.\nTugatish uchun 'Tugatdim ✅' bosing.",
            reply_markup=collect_episodes_kb(),
        )
        return

    if action == "season_split" and step == "wait_range":
        if text == "Tugatdim ✅":
            ranges = st.get("ranges", [])
            if not ranges:
                await safe_send(ADMIN_ID, "Hech qanday fasl qo'shilmadi.", reply_markup=admin_main_kb())
                admin_states.pop(ADMIN_ID, None)
                return
            # Yakuniy tekshiruv: takrorlanish yo'qmi
            used = set()
            for a, b in ranges:
                for n in range(a, b + 1):
                    if n in used:
                        await safe_send(ADMIN_ID, f"❌ Takrorlangan qism topildi: {n}", reply_markup=admin_series_settings_kb())
                        admin_states.pop(ADMIN_ID, None)
                        return
                    used.add(n)
            await set_series_seasons_db(st.get("series_code"), ranges)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Fasllar saqlandi. Jami fasl: {len(ranges)}", reply_markup=admin_main_kb())
            return

        try:
            a, b = parse_episode_range(text)
        except Exception:
            await safe_send(ADMIN_ID, "Diapazon noto'g'ri. Masalan: 1-17", reply_markup=collect_episodes_kb())
            return

        ranges = st.get("ranges", [])
        # takroriy / o'zaro kesishma yo'qligini tekshiramiz
        for old_a, old_b in ranges:
            if not (b < old_a or a > old_b):
                await safe_send(ADMIN_ID, f"❌ Diapazon oldingi fasl bilan kesishdi: {old_a}-{old_b}", reply_markup=collect_episodes_kb())
                return
        ranges.append((a, b))
        st["ranges"] = ranges
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, f"✅ {len(ranges)}-fasl qabul qilindi: {a}-{b}", reply_markup=collect_episodes_kb())
        return

    # --- REMOVE MOVIE / SERIES / EPISODE ---
    if action == "remove_movie" and step == "wait_code":
        code = text.strip()
        admin_states.pop(ADMIN_ID, None)
        if re.fullmatch(r"\d+-\d+", code):
            sc, epn = code.split("-", 1)
            deleted = await remove_episode_db(sc, int(epn))
            if deleted:
                await safe_send(ADMIN_ID, f"✅ Qism o'chirildi: {code}", reply_markup=admin_main_kb())
            else:
                await safe_send(ADMIN_ID, "Bunday qism topilmadi.", reply_markup=admin_main_kb())
            return

        if re.fullmatch(r"\d+", code):
            mv = await get_movie_db(code)
            if mv:
                ok = await remove_movie_db(code)
                await safe_send(ADMIN_ID, "✅ Kino o'chirildi." if ok else "Kino o'chmadi.", reply_markup=admin_main_kb())
                return
            meta = await get_series_meta(code)
            if meta:
                ok = await remove_series_db(code)
                await safe_send(ADMIN_ID, "✅ Serial o'chirildi." if ok else "Serial o'chmadi.", reply_markup=admin_main_kb())
                return
        await safe_send(ADMIN_ID, "Bunday kod topilmadi.", reply_markup=admin_main_kb())
        return

    # --- BROADCAST ---
    if action == "broadcast" and step == "wait_media":
        # Media bo'lsa saqlaymiz, bo'lmasa textni ham media sifatida qabul qilamiz.
        file_id = None
        file_type = None
        if message.video:
            file_id = message.video.file_id
            file_type = "video"
        elif message.document:
            file_id = message.document.file_id
            file_type = "document"
        elif message.animation:
            file_id = message.animation.file_id
            file_type = "animation"
        elif message.photo:
            file_id = message.photo[-1].file_id
            file_type = "photo"
        elif message.audio:
            file_id = message.audio.file_id
            file_type = "audio"
        else:
            await safe_send(ADMIN_ID, "Avval media yoki video yuboring.", reply_markup=admin_flow_kb())
            return

        st["file_id"] = file_id
        st["file_type"] = file_type
        st["step"] = "wait_text"
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Endi broadcast matnini yuboring. Bu media ostida yuboriladi.", reply_markup=admin_flow_kb())
        return

    if action == "broadcast" and step == "wait_text":
        caption = safe_text(message.text)
        if not caption:
            await safe_send(ADMIN_ID, "Broadcast matni bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        data = admin_states.pop(ADMIN_ID, None) or {}
        file_id = data.get("file_id")
        file_type = data.get("file_type")
        result = await broadcast_to_users(file_id, file_type, caption)
        await safe_send(
            ADMIN_ID,
            f"✅ Broadcast yakunlandi.\nJami: {result['total']}\nYuborildi: {result['success']}\nXatolik: {result['failed']}",
            reply_markup=admin_main_kb(),
        )
        return

    # --- SETTINGS HANDLERS ---
    if action == "remove_share":
        admin_states[ADMIN_ID] = {"action": "remove_share", "step": "confirm"}
        await safe_send(ADMIN_ID, "Tasdiqlash uchun biror matn yuboring. Kerak emas bo'lsa Cancel bosing.", reply_markup=admin_flow_kb())
        return

    await safe_send(ADMIN_ID, "Admin: buyruqni menyudan tanlang.", reply_markup=admin_main_kb())


# ===================== CALLBACKLAR =====================
@dp.callback_query(lambda c: c.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    user_id = call.from_user.id
    ok, missing = await check_user_all(user_id)
    if ok:
        await update_user_last_validated(user_id, now_utc())
        await call.answer("Tekshiruv muvaffaqiyatli ✅", show_alert=False)
        await safe_send(user_id, "✅ A'zolik tasdiqlandi. Endi kod yuboring.")
        return
    await invalidate_user_subscription(user_id)
    kb = groups_inline_kb(missing)
    await call.answer("Hali barcha guruhlarga qo'shilmagansiz.", show_alert=True)
    await safe_send(user_id, "Kodni olish uchun quyidagilarga a'zo bo'ling:", reply_markup=kb)


@dp.callback_query(lambda c: c.data.startswith("movie:hide:"))
async def cb_movie_hide(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("Yashirildi", show_alert=False)


@dp.callback_query(lambda c: c.data.startswith("dummy:"))
async def cb_dummy(call: CallbackQuery):
    await call.answer("Bu tugma faqat eslatma uchun.", show_alert=True)



@dp.callback_query(lambda c: c.data.startswith("nm_replace:"))
async def cb_nm_replace(call: CallbackQuery):
    try:
        _, decision, code = call.data.split(":", 2)
    except Exception:
        await call.answer("Noto'g'ri amal", show_alert=True)
        return

    if decision == "no":
        admin_states.pop(ADMIN_ID, None)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await call.answer("Bekor qilindi", show_alert=False)
        await safe_send(ADMIN_ID, "Nomsiz almashtirish bekor qilindi.", reply_markup=admin_settings_kb())
        return

    if decision == "yes":
        admin_states[ADMIN_ID] = {"action": "manual_nameless", "step": "wait_media", "code": code, "replace_existing": True}
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await call.answer("Media yuboring", show_alert=False)
        await safe_send(ADMIN_ID, "Almashtirish uchun video yoki rasm yuboring.", reply_markup=admin_flow_kb())
        return

    await call.answer("Noto'g'ri amal", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("page:"))
async def cb_episode_page(call: CallbackQuery):
    try:
        _, series_code, page_str = call.data.split(":")
        page = int(page_str)
    except Exception:
        await call.answer("Noto'g'ri sahifa", show_alert=True)
        return
    eps = await get_series_episodes(series_code)
    kb = build_episodes_inline_kb(series_code, eps, page=page, per_page=EPISODES_PER_PAGE)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("play:"))
async def cb_play_episode(call: CallbackQuery):
    try:
        _, series_code, ep_str = call.data.split(":")
        epn = int(ep_str)
    except Exception:
        await call.answer("Noto'g'ri qism", show_alert=True)
        return

    subscribed, last_validated_at = await get_user_record_db(call.from_user.id)
    now = now_utc()
    need_validation = True
    if subscribed and last_validated_at:
        if (now - last_validated_at).total_seconds() < VALIDATION_TTL:
            need_validation = False
    if need_validation:
        ok, missing = await check_user_all(call.from_user.id)
        if not ok:
            await invalidate_user_subscription(call.from_user.id)
            await call.answer("Avval a'zolikni tasdiqlang.", show_alert=True)
            await safe_send(call.from_user.id, "Kodni olish uchun quyidagilarga a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
            return
        await update_user_last_validated(call.from_user.id, now)

    ep = await get_episode_db(series_code, epn)
    meta = await get_series_meta(series_code)
    if not ep or not meta:
        await call.answer("Topilmadi", show_alert=True)
        return

    file_id, file_type, ep_title, downloads = ep
    title, language, description, created_at = meta
    season_num = await get_episode_season(series_code, epn)

    caption = build_episode_caption(title, language, epn, season_num=season_num)
    kb = build_movie_kb(f"{series_code}-{epn}", title or "Serial")

    try:
        if file_type == "video":
            await bot.send_video(call.from_user.id, file_id, caption=caption, reply_markup=kb)
        elif file_type == "document":
            await bot.send_document(call.from_user.id, file_id, caption=caption, reply_markup=kb)
        elif file_type == "animation":
            await bot.send_animation(call.from_user.id, file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(call.from_user.id, file_id, caption=caption, reply_markup=kb)
    except Exception:
        logger.exception("Episode send error")
        await safe_send(call.from_user.id, "❌ Media yuborishda xatolik yuz berdi.")
        return

    await increment_episode_downloads(series_code, epn)
    await call.answer("Yuborildi ✅", show_alert=False)


# ===================== JOIN REQUEST =====================
@dp.chat_join_request()
async def on_chat_join_request(event: ChatJoinRequest):
    # Bot adminlarga xabar yubormaydi.
    try:
        await add_pending_join_request_db(event.from_user.id, str(event.chat.id))
    except Exception:
        logger.exception("pending join save error")


# ===================== BACKGROUND CHECK =====================
async def background_sub_check():
    await asyncio.sleep(5)
    while True:
        try:
            async with aiosqlite.connect(DB_FILE) as db:
                cur = await db.execute("SELECT user_id FROM users")
                users = await cur.fetchall()
            for (uid,) in users:
                ok, _missing = await check_user_all(int(uid))
                if not ok:
                    await invalidate_user_subscription(int(uid))
        except Exception:
            logger.exception("background_sub_check error")
        await asyncio.sleep(3600)


async def handle_start_payload(message: Message, payload: str) -> bool:
    p = payload.strip()
    if not p:
        return False
    if re.fullmatch(r"m-\d+", p):
        code = p.split("-", 1)[1]
        return await send_movie_to_user(message.from_user.id, code)
    if re.fullmatch(r"s-\d+", p):
        code = p.split("-", 1)[1]
        return await send_series_to_user(message.from_user.id, code)
    if re.fullmatch(r"n-\d+", p):
        code = p.split("-", 1)[1]
        return await send_movie_to_user(message.from_user.id, code)
    if re.fullmatch(r"\d+", p):
        if await send_movie_to_user(message.from_user.id, p):
            return True
        if await send_series_to_user(message.from_user.id, p):
            return True
    return False


async def send_movie_to_user(user_id: int, code: str):
    mv = await get_movie_db(code)
    if not mv:
        return False
    subscribed, last_validated_at = await get_user_record_db(user_id)
    now = now_utc()
    need_validation = True
    if subscribed and last_validated_at and (now - last_validated_at).total_seconds() < VALIDATION_TTL:
        need_validation = False
    if need_validation:
        ok, missing = await check_user_all(user_id)
        if not ok:
            await invalidate_user_subscription(user_id)
            await safe_send(user_id, "Kodni olish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
            return True
        await update_user_last_validated(user_id, now)
    title, file_id, file_type, year, genre, quality, language, description, country, downloads = mv
    caption = build_movie_caption(title, quality, language, genre, country, year, description, code)
    kb = build_user_share_kb(code, title)
    try:
        if file_type == "video":
            await bot.send_video(user_id, file_id, caption=caption, reply_markup=kb)
        elif file_type == "document":
            await bot.send_document(user_id, file_id, caption=caption, reply_markup=kb)
        elif file_type == "animation":
            await bot.send_animation(user_id, file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(user_id, file_id, caption=caption, reply_markup=kb)
    except Exception:
        logger.exception("movie send failed")
        await safe_send(user_id, "❌ Media yuborishda xatolik yuz berdi.")
        return True
    await increment_movie_downloads(code)
    return True


async def send_series_to_user(user_id: int, code: str):
    meta = await get_series_meta(code)
    if not meta:
        return False
    ok, missing = await check_user_all(user_id)
    if not ok:
        await invalidate_user_subscription(user_id)
        await safe_send(user_id, "Serialni ko'rish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
        return True
    await update_user_last_validated(user_id, now_utc())
    title, language, description, created_at = meta
    eps = await get_series_episodes(code)
    if not eps:
        await safe_send(user_id, "❌ Bu serialda qism topilmadi.")
        return True
    seasons = await get_series_seasons_db(code)
    if seasons:
        season_lines = [f"{sn}-fasl: {a}-{b}" for sn, a, b in seasons]
        await safe_send(user_id, f"📺 {title}\n" + "\n".join(season_lines) + f"\n\nBizning sahifamiz: {fmt_page_link()}")
    first_ep = eps[0]
    first_ep_num, first_file_id, first_file_type, first_ep_title, first_downloads = first_ep
    first_season = await get_episode_season(code, first_ep_num)
    preview_caption = build_episode_caption(title, language, first_ep_num, season_num=first_season)
    try:
        if first_file_type == "video":
            await bot.send_video(user_id, first_file_id, caption=preview_caption)
        elif first_file_type == "document":
            await bot.send_document(user_id, first_file_id, caption=preview_caption)
        elif first_file_type == "animation":
            await bot.send_animation(user_id, first_file_id, caption=preview_caption)
        else:
            await bot.send_document(user_id, first_file_id, caption=preview_caption)
    except Exception:
        logger.exception("preview send failed")
    kb = build_series_episodes_kb(code, eps, page=0, per_page=EPISODES_PER_PAGE)
    await safe_send(user_id, "Epizodlardan birini tanlang:", reply_markup=kb)
    return True


# ===================== COMMANDS / USER HANDLERS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not message.from_user:
        return
    await add_user_db(message.from_user.id)
    txt = safe_text(message.text)
    payload = None
    parts = txt.split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1].strip()

    if payload:
        handled = await handle_start_payload(message, payload)
        if handled:
            if message.from_user.id == ADMIN_ID:
                await safe_send(ADMIN_ID, "🔧 Admin panelga xush kelibsiz.", reply_markup=admin_main_kb())
            return

    ok, missing = await check_user_all(message.from_user.id)
    if ok:
        await update_user_last_validated(message.from_user.id, now_utc())
        await safe_send(message.from_user.id, "👋 <b>Salom alaykum...</b>\n\nPastdagi tugma orqali kodlarni oling.", reply_markup=build_start_kb())
    else:
        await invalidate_user_subscription(message.from_user.id)
        await safe_send(
            message.from_user.id,
            "👋 <b>Salom alaykum...</b>\n\nPastdagi tugma orqali kodlarni oling.",
            reply_markup=groups_inline_kb(missing),
        )
    if message.from_user.id == ADMIN_ID:
        await safe_send(ADMIN_ID, "🔧 Admin panelga xush kelibsiz.", reply_markup=admin_main_kb())


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not message.from_user:
        return
    if message.from_user.id != ADMIN_ID:
        ok, missing = await ensure_user_subscription(message.from_user.id)
        if not ok:
            await safe_send(message.from_user.id, "Kodni olish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
            return
    txt = (
        "ℹ️ Yordam\n"
        "/start — bosh sahifa\n"
        "/help — yordam\n"
        "/settings — admin sozlamalari\n"
        "Kod yuborish:\n"
        "• Kino: 100\n"
        "• Serial qismi: 1000-1\n"
    )
    await safe_send(message.from_user.id, txt)


@dp.message(Command("settings"))
async def cmd_settings(message: Message):
    if not message.from_user:
        return
    if message.from_user.id != ADMIN_ID:
        ok, missing = await ensure_user_subscription(message.from_user.id)
        if not ok:
            await safe_send(message.from_user.id, "Kodni olish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
            return
        await safe_send(message.from_user.id, "⚙️ Bu buyruq admin uchun mo'ljallangan.")
        return
    admin_states[ADMIN_ID] = {"action": "settings_menu", "step": "menu"}
    await safe_send(ADMIN_ID, "⚙️ Sozlash menyusi.", reply_markup=admin_settings_kb())


@dp.message(lambda m: m.from_user is not None and m.from_user.id == ADMIN_ID)
async def admin_message_router(message: Message):
    if admin_states.get(ADMIN_ID):
        await handle_admin_stateful(message)
        return
    if message.text and message.text.strip():
        await handle_admin_stateful(message)
        return
    await safe_send(ADMIN_ID, "Admin: menyudan amal tanlang.", reply_markup=admin_main_kb())


@dp.message(lambda m: m.from_user is not None and m.from_user.id != ADMIN_ID)
async def user_message_router(message: Message):
    if not message.from_user:
        return
    await add_user_db(message.from_user.id)

    ok, missing = await ensure_user_subscription(message.from_user.id)
    if not ok:
        await safe_send(
            message.from_user.id,
            "Kodni olish uchun avval a'zo bo'ling:",
            reply_markup=groups_inline_kb(missing),
        )
        return

    txt = safe_text(message.text)

    if re.fullmatch(r"m-\d+", txt):
        code = txt.split("-", 1)[1]
        if await send_movie_to_user(message.from_user.id, code):
            return
    if re.fullmatch(r"s-\d+", txt):
        code = txt.split("-", 1)[1]
        if await send_series_to_user(message.from_user.id, code):
            return
    if re.fullmatch(r"n-\d+", txt):
        code = txt.split("-", 1)[1]
        if await send_movie_to_user(message.from_user.id, code):
            return

    # Serial qism: 1000-1
    m = re.fullmatch(r"(\d+)-(\d+)", txt)
    if m:
        series_code = m.group(1)
        epn = int(m.group(2))
        subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
        now = now_utc()
        need_validation = True
        if has_recent_validation(subscribed, last_validated_at):
            need_validation = False
        if need_validation:
            ok, missing = await check_user_all(message.from_user.id)
            if not ok:
                await invalidate_user_subscription(message.from_user.id)
                await safe_send(message.from_user.id, "Kodni olish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
                return
            await update_user_last_validated(message.from_user.id, now)

        ep = await get_episode_db(series_code, epn)
        meta = await get_series_meta(series_code)
        if not ep or not meta:
            await safe_send(message.from_user.id, "❌ Bunday qism topilmadi.")
            return

        file_id, file_type, ep_title, downloads = ep
        title, language, description, created_at = meta
        season_num = await get_episode_season(series_code, epn)
        caption = build_episode_caption(title, language, epn, season_num=season_num)
        kb = build_movie_kb(f"{series_code}-{epn}", title)

        try:
            if file_type == "video":
                await bot.send_video(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            elif file_type == "document":
                await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            elif file_type == "animation":
                await bot.send_animation(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            else:
                await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
        except Exception:
            logger.exception("episode send failed")
            await safe_send(message.from_user.id, "❌ Media yuborishda xatolik yuz berdi.")
            return
        await increment_episode_downloads(series_code, epn)
        return

    # Kino yoki serial kodi: 100
    if re.fullmatch(r"\d+", txt):
        code = txt
        mv = await get_movie_db(code)
        if mv:
            subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
            now = now_utc()
            need_validation = True
            if has_recent_validation(subscribed, last_validated_at):
                need_validation = False
            if need_validation:
                ok, missing = await check_user_all(message.from_user.id)
                if not ok:
                    await invalidate_user_subscription(message.from_user.id)
                    await safe_send(message.from_user.id, "Kodni olish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
                    return
                await update_user_last_validated(message.from_user.id, now)

            title, file_id, file_type, year, genre, quality, language, description, country, downloads = mv
            caption = build_movie_caption(title, quality, language, genre, country, year, description, code)
            kb = build_movie_kb(code, title)
            try:
                if file_type == "video":
                    await bot.send_video(message.from_user.id, file_id, caption=caption, reply_markup=kb)
                elif file_type == "document":
                    await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
                elif file_type == "animation":
                    await bot.send_animation(message.from_user.id, file_id, caption=caption, reply_markup=kb)
                else:
                    await bot.send_document(message.from_user.id, file_id, caption=caption, reply_markup=kb)
            except Exception:
                logger.exception("movie send failed")
                await safe_send(message.from_user.id, "❌ Media yuborishda xatolik yuz berdi.")
                return
            await increment_movie_downloads(code)
            return

        meta = await get_series_meta(code)
        if meta:
            ok, missing = await check_user_all(message.from_user.id)
            if not ok:
                await invalidate_user_subscription(message.from_user.id)
                await safe_send(message.from_user.id, "Serialni ko'rish uchun avval a'zo bo'ling:", reply_markup=groups_inline_kb(missing))
                return
            await update_user_last_validated(message.from_user.id, now_utc())

            title, language, description, created_at = meta
            eps = await get_series_episodes(code)
            if not eps:
                await safe_send(message.from_user.id, "❌ Bu serialda qism topilmadi.")
                return

            seasons = await get_series_seasons_db(code)
            if seasons:
                season_lines = [f"{sn}-fasl: {a}-{b}" for sn, a, b in seasons]
                await safe_send(message.from_user.id, f"📺 {title}\n" + "\n".join(season_lines) + f"\n\nBizning sahifamiz: {fmt_page_link()}")

            first_ep = eps[0]
            first_ep_num, first_file_id, first_file_type, first_ep_title, first_downloads = first_ep
            first_season = await get_episode_season(code, first_ep_num)
            preview_caption = build_episode_caption(title, language, first_ep_num, season_num=first_season)

            try:
                if first_file_type == "video":
                    await bot.send_video(message.from_user.id, first_file_id, caption=preview_caption)
                elif first_file_type == "document":
                    await bot.send_document(message.from_user.id, first_file_id, caption=preview_caption)
                elif first_file_type == "animation":
                    await bot.send_animation(message.from_user.id, first_file_id, caption=preview_caption)
                else:
                    await bot.send_document(message.from_user.id, first_file_id, caption=preview_caption)
            except Exception:
                logger.exception("preview send failed")

            kb = build_episodes_inline_kb(code, eps, page=0, per_page=EPISODES_PER_PAGE)
            await safe_send(message.from_user.id, "Epizodlardan birini tanlang:", reply_markup=kb)
            return

        await safe_send(message.from_user.id, "❌ Bunday kod topilmadi.")
        return

    await safe_send(message.from_user.id, "ℹ️ Kino yoki serial kodini yuboring.")


# ===================== STARTUP =====================
async def main():
    await init_db()
    if not await settings_get("codes_link"):
        await settings_set("codes_link", REQUIRED_CHANNEL or "")
    me = await tg_get_me()
    bot_identity["id"] = me.get("id")
    bot_identity["username"] = me.get("username")
    asyncio.create_task(background_sub_check())
    asyncio.create_task(account_channel_watcher())
    asyncio.create_task(publish_worker())
    await load_pending_publish_jobs()
    logger.info("Bot ishga tushdi. username=%s", bot_identity.get("username"))
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot to'xtadi.")
    except Exception:
        logger.exception("Kutilmagan xatolik yuz berdi.")
