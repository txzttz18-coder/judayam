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
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

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
logger = logging.getLogger("kino_serial")

# --------------------- KONFIG ---------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8522754363:AAFH-PpaLUciTXS8IfWj2_zjqIbSs20K9Tg").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN kerak. Masalan: export BOT_TOKEN='<token>'")

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", "7794986117"))
except Exception:
    ADMIN_ID = 0

DB_FILE = os.getenv("DB_FILE", "kino_serial.db").strip() or "kino_serial.db"
VALIDATION_TTL = int(os.getenv("VALIDATION_TTL", "3600"))
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "").strip()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
dp = Dispatcher()

admin_states: Dict[int, Dict[str, Any]] = {}

EPISODES_PER_PAGE = 24


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
        await db.commit()

    # default settings
    await settings_set("next_code", await settings_get("next_code") or "100")
    await settings_set("next_series_code", await settings_get("next_series_code") or "1000")
    await settings_set("next_nameless_code", await settings_get("next_nameless_code") or "1")
    await settings_set("codes_link", await settings_get("codes_link") or "")


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


async def add_user_db(user_id: int):
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


async def add_movie_db(code: str, title: Optional[str], file_id: str, file_type: str,
                       year: Optional[str], genre: Optional[str], quality: Optional[str],
                       language: Optional[str], description: Optional[str], country: Optional[str]):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO movies
            (code, title, file_id, file_type, year, genre, quality, language, description, country, downloads)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT downloads FROM movies WHERE code = ?), 0))
            """,
            (code, title, file_id, file_type, year, genre, quality, language, description, country, code),
        )
        await db.commit()


async def remove_movie_db(code: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("DELETE FROM movies WHERE code = ?", (code,))
        await db.commit()
        return cur.rowcount > 0


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


async def add_episode_db(series_code: str, episode_number: int, file_id: str, file_type: str, episode_title: Optional[str] = None):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO episodes(series_code, episode_number, file_id, file_type, episode_title, downloads)
            VALUES (?, ?, ?, ?, ?, COALESCE((SELECT downloads FROM episodes WHERE series_code = ? AND episode_number = ?), 0))
            """,
            (series_code, episode_number, file_id, file_type, episode_title, series_code, episode_number),
        )
        await db.commit()


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
        await db.execute("DELETE FROM series_seasons WHERE series_code = ?", (series_code,))
        await db.execute("DELETE FROM episodes WHERE series_code = ?", (series_code,))
        cur = await db.execute("DELETE FROM series WHERE series_code = ?", (series_code,))
        await db.commit()
        return cur.rowcount > 0


async def clear_series_episodes_db(series_code: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM episodes WHERE series_code = ?", (series_code,))
        await db.execute("DELETE FROM series_seasons WHERE series_code = ?", (series_code,))
        await db.commit()


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


# ===================== KEYBOARDLAR =====================
def admin_main_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🎬 Kino qo'shish"), KeyboardButton(text="🎫 Nomsiz kino")],
        [KeyboardButton(text="📺 Serial qo'shish"), KeyboardButton(text="⚙️ Sozlash")],
        [KeyboardButton(text="📣 Broadcast"), KeyboardButton(text="📚 Ro'yxatlar")],
        [KeyboardButton(text="🗂 DB eksport"), KeyboardButton(text="👥 Foydalanuvchilar")],
        [KeyboardButton(text="➕ Guruh qo'shish"), KeyboardButton(text="➖ Guruh o'chirish")],
        [KeyboardButton(text="👁 Join monitoring"), KeyboardButton(text="🗑 Remove Movie")],
        [KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_settings_kb() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🎬 Kinoni sozlash"), KeyboardButton(text="📺 Serialni sozlash")],
        [KeyboardButton(text="🔗 Set ssilka"), KeyboardButton(text="🧹 Reset ssilka")],
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
        elif st and st.get("action") == "settings_menu":
            await reset_to_main_admin()
        elif st and st.get("action", "").startswith("series_"):
            await reset_to_series_settings_admin()
        else:
            await reset_to_main_admin()
        return

    if text == "⚙️ Sozlash":
        admin_states[ADMIN_ID] = {"action": "settings_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "Qaysi bo'limni sozlaymiz?", reply_markup=admin_settings_kb())
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
        elif action in {"movie_replace", "add_movie", "add_nameless", "add_group", "remove_group", "add_join", "remove_join", "remove_movie", "broadcast", "add_series"}:
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

    if action in {"settings_menu", "series_settings_menu"} and step == "menu":
        await handle_admin_text(message)
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

    # --- KINO QO'SHISH ---
    if action == "add_movie" and step == "wait_media":
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
        admin_states[ADMIN_ID] = {
            "action": "add_movie",
            "step": "wait_title",
            "file_id": file_id,
            "file_type": file_type,
        }
        await safe_send(ADMIN_ID, "Kino nomini yuboring. Nomsiz bo'lsa '-' yozing.", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_title":
        title = normalize_optional_text(text)
        if title == "-":
            title = None
        if title is None:
            await safe_send(ADMIN_ID, "Kino nomi kerak. Agar nomsiz bo'lsa Nomsiz kino tugmasidan foydalaning.", reply_markup=admin_main_kb())
            admin_states.pop(ADMIN_ID, None)
            return
        st.update({"step": "wait_language", "title": title})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Kino tilini yuboring. Masalan: uzbek / rus / english", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_language":
        language = normalize_language_input(text)
        if not language:
            await safe_send(ADMIN_ID, "Til bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_quality", "language": language})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Sifatni yuboring. Masalan: 1080 yoki 720", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_quality":
        quality = normalize_quality_input(text)
        if not quality:
            await safe_send(ADMIN_ID, "Sifat bo'sh bo'lmasin.", reply_markup=admin_flow_kb())
            return
        st.update({"step": "wait_genre", "quality": quality})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Janr yuboring. Kerak bo'lmasa '-' yozing.", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_genre":
        genre = normalize_optional_text(text)
        st.update({"step": "wait_country", "genre": genre})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Davlat yuboring. Kerak bo'lmasa '-' yozing.", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_country":
        country = normalize_optional_text(text)
        st.update({"step": "wait_year", "country": country})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Yil yuboring. Kerak bo'lmasa '-' yozing.", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_year":
        year = normalize_optional_text(text)
        st.update({"step": "wait_description", "year": year})
        admin_states[ADMIN_ID] = st
        await safe_send(ADMIN_ID, "Izoh yuboring. Izoh yo'q bo'lsa '-' yozing.", reply_markup=admin_flow_kb())
        return

    if action == "add_movie" and step == "wait_description":
        description = normalize_optional_text(text)
        data = admin_states.pop(ADMIN_ID, None) or {}
        nxt = await settings_get("next_code")
        code = str(int(nxt or "100"))
        await settings_set("next_code", str(int(code) + 1))
        await add_movie_db(
            code=code,
            title=data.get("title"),
            file_id=data.get("file_id"),
            file_type=data.get("file_type"),
            year=data.get("year"),
            genre=data.get("genre"),
            quality=data.get("quality"),
            language=data.get("language"),
            description=description,
            country=data.get("country"),
        )
        await safe_send(ADMIN_ID, f"✅ Kino saqlandi. Kod: {code}", reply_markup=admin_main_kb())
        return

    # --- NOMSIZ KINO ---
    if action == "add_nameless" and step == "wait_media":
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
        data = admin_states.pop(ADMIN_ID, None) or {}
        nxt = await settings_get("next_nameless_code")
        try:
            n = int(nxt or "1")
        except Exception:
            n = 1
        cnt = await count_nameless_movies()
        if cnt >= 3000 and n < 5000:
            n = 5000
        code = str(n)
        await settings_set("next_nameless_code", str(n + 1))
        await add_movie_db(code, None, file_id, file_type, None, None, None, None, None, None)
        await safe_send(ADMIN_ID, f"✅ Nomsiz kino saqlandi. Kod: {code}", reply_markup=admin_main_kb())
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
        episodes.append((file_id, file_type))
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
        nxt = await settings_get("next_series_code")
        try:
            series_code = str(int(nxt or "1000"))
        except Exception:
            series_code = "1000"
        await settings_set("next_series_code", str(int(series_code) + 1))
        await add_series_db(series_code, title, language, None)
        for idx, (fid, ftype) in enumerate(episodes, start=1):
            await add_episode_db(series_code, idx, fid, ftype, None)
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
        eps.append((file_id, file_type))
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
        for idx, (fid, ftype) in enumerate(data.get("episodes", []), start=1):
            await add_episode_db(series_code, idx, fid, ftype, None)
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
            for idx, (fid, ftype) in enumerate(eps, start=start_num):
                await add_episode_db(series_code, idx, fid, ftype, None)
            admin_states.pop(ADMIN_ID, None)
            await safe_send(ADMIN_ID, f"✅ Serial davomiga {len(eps)} qism qo'shildi. Boshlanish: {start_num}", reply_markup=admin_main_kb())
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
        eps.append((file_id, file_type))
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
            async with aiosqlite.connect(DB_FILE) as db:
                cur = await db.execute(
                    "DELETE FROM episodes WHERE series_code = ? AND episode_number = ?",
                    (sc, int(epn)),
                )
                await db.commit()
            if cur.rowcount > 0:
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


# ===================== COMMANDS / USER HANDLERS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not message.from_user:
        return
    await add_user_db(message.from_user.id)
    ok, missing = await check_user_all(message.from_user.id)
    if ok:
        await update_user_last_validated(message.from_user.id, now_utc())
        await safe_send(message.from_user.id, "👋 Assalomu alaykum! Kodni yuboring.")
    else:
        await invalidate_user_subscription(message.from_user.id)
        await safe_send(
            message.from_user.id,
            "👋 Assalomu alaykum! Davom etish uchun quyidagi a'zoliklarni bajaring:",
            reply_markup=groups_inline_kb(missing),
        )
    if message.from_user.id == ADMIN_ID:
        await safe_send(ADMIN_ID, "🔧 Admin panelga xush kelibsiz.", reply_markup=admin_main_kb())


@dp.message(Command("help"))
async def cmd_help(message: Message):
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
    if message.from_user.id == ADMIN_ID:
        admin_states[ADMIN_ID] = {"action": "settings_menu", "step": "menu"}
        await safe_send(ADMIN_ID, "⚙️ Sozlash menyusi.", reply_markup=admin_settings_kb())
    else:
        await safe_send(message.from_user.id, "⚙️ Bu buyruq admin uchun mo'ljallangan.")


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
    txt = safe_text(message.text)

    # Serial qism: 1000-1
    m = re.fullmatch(r"(\d+)-(\d+)", txt)
    if m:
        series_code = m.group(1)
        epn = int(m.group(2))
        subscribed, last_validated_at = await get_user_record_db(message.from_user.id)
        now = now_utc()
        need_validation = True
        if subscribed and last_validated_at and (now - last_validated_at).total_seconds() < VALIDATION_TTL:
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
            if subscribed and last_validated_at and (now - last_validated_at).total_seconds() < VALIDATION_TTL:
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
    asyncio.create_task(background_sub_check())
    logger.info("Bot ishga tushdi.")
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
