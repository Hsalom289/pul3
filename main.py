# -*- coding: utf-8 -*-
# Aiogram 3.x
# pip install aiogram

import asyncio
import logging
import sqlite3
import datetime
from datetime import timedelta
import urllib.parse
import shutil
import os

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import StateFilter
from aiogram.filters.command import Command
from aiogram.types import (
    KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

import gzip

logging.basicConfig(level=logging.INFO)

# ================== CONFIG ==================
TOKEN = "8436934185:AAGWATHLhZ0B04TDi79OoULLDAMJ4w78-Ik"
CHANNEL = "@uyzar_elonlar"

# YANGI majburiy kanal
SECOND_CHANNEL = "@zarafshon_kanal"
# Ikkala kanal (gate va penalty shu ro‘yxat bo‘yicha ishlaydi)
MANDATORY_CHANNELS = [CHANNEL, SECOND_CHANNEL]

# Majburiy bot (faqat /start bosilganini tekshirish uchun)
HELPER_BOT_USERNAME = "@uyzarbot"
HELPER_BOT_TOKEN = "7009289954:AAEssEXV8cZSGAKOShmFNiJMO2ldgJU5Nl4"

ADMINS = [1217732736, 6374979572]
TEST_USER_ID = 6374979572
TEST_USER_BALANCE = 1_000_000

WITHDRAW_GROUP_ID = -1002938188891  # yechib olish so'rovlari guruhiga yuboriladi

AWARD = 500        # 500 so‘m
PENALTY = 100
FIRST_MIN = 1000
NEXT_MIN = 15000

# ======== PERSISTENT DB sozlamalari ========
DB_DIR = os.getenv("DB_DIR", "/data")
try:
    os.makedirs(DB_DIR, exist_ok=True)
except Exception:
    DB_DIR = "."
DB_PATH = os.getenv("DB_PATH", os.path.join(DB_DIR, "bot.db"))
DB_BACKUP = os.getenv("DB_BACKUP", os.path.join(DB_DIR, "bot.db.bak"))
# ===========================================

TG_BACKUP = True
API_ID = 16072756           # fill with my real API ID
API_HASH = "5fc7839a0d020c256e5c901cebd21bb7" # fill with my real API HASH
TG_SESSION = "db_backup"
BACKUP_CHAT = "@dbpul"  # or "me"
DB_BACKUP_EVERY_MIN = 1
conn: sqlite3.Connection | None = None

# -------- Button labels
BTN_REF_MAIN   = "👥 Do‘st taklif qilib pul ishlash 💰"
BTN_HELP       = "ℹ️ Yordam ❓"
BTN_ADMIN      = "🛠 Admin Panel"
BTN_LINK       = "🔗 Taklif linkim 📎"
BTN_BALANCE    = "💰 Balansim 📊"
BTN_WITHDRAW   = "💸 Yechib olish 💳"
BTN_RULES      = "📜 Qoidalar & FAQ 📋"
BTN_BACK       = "◀️ Orqaga 🔙"
BTN_STATS      = "📊 Statistika"
BTN_SENDALL    = "📤 Send to all"
BTN_TOPREF     = "📈 Xarakat qilayotganlar"

MAIN_BTNS = {BTN_REF_MAIN, BTN_HELP, BTN_ADMIN}
REF_BTNS  = {BTN_LINK, BTN_BALANCE, BTN_WITHDRAW, BTN_RULES, BTN_BACK}

# ================== DB ==================
def _restore_db_if_needed():
    try:
        if (not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) < 1024) and os.path.exists(DB_BACKUP):
            shutil.copyfile(DB_BACKUP, DB_PATH)
    except Exception:
        pass

def _sqlite_connect(path: str):
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    except Exception:
        pass
    return conn

def _make_backup(conn: sqlite3.Connection, dst: str):
    try:
        bconn = sqlite3.connect(dst)
        with bconn:
            conn.backup(bconn)
        bconn.close()
    except Exception:
        try:
            shutil.copyfile(DB_PATH, dst)
        except Exception:
            pass

async def _periodic_backup_task():
    while True:
        await asyncio.sleep(1800)  # 30 min
        try:
            _make_backup(conn, DB_BACKUP)
        except Exception:
            pass

# ======== faqat yo‘q bo‘lsa yaratish ========
REQUIRED_TABLES = {"users", "referrals", "withdrawals", "pending_refs"}

def _has_required_schema(c: sqlite3.Connection) -> bool:
    rows = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {r[0] for r in rows}
    return REQUIRED_TABLES.issubset(names)

def ensure_db():
    _restore_db_if_needed()
    c = _sqlite_connect(DB_PATH)
    if not _has_required_schema(c):
        cur = c.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER DEFAULT 0,
            referrer_id INTEGER,
            has_withdrawn INTEGER DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            invited_id INTEGER,
            join_time TEXT,
            penalized INTEGER DEFAULT 0,
            done INTEGER DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS withdrawals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            card_number TEXT,
            full_name TEXT,
            created_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_refs(
            referee_id INTEGER PRIMARY KEY,
            referrer_id INTEGER,
            created_at TEXT
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ref_invited ON referrals(invited_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id)")
        c.commit()
        try:
            _make_backup(c, DB_BACKUP)
        except Exception:
            pass
    return c

async def _tg_restore_if_needed():
    if not TG_BACKUP or API_ID == 0 or not API_HASH:
        return
    try:
        need_restore = (not os.path.exists(DB_PATH)) or (os.path.getsize(DB_PATH) < 1024)
    except Exception:
        need_restore = True
    if not need_restore:
        return
    try:
        from telethon import TelegramClient
        client = TelegramClient("bot_backup_restore", API_ID, API_HASH)
        await client.start(bot_token=TOKEN)
        async for m in client.iter_messages(BACKUP_CHAT, limit=50):
            if getattr(m, "document", None) and m.file and str(m.file.name).endswith(".db.gz"):
                tmp_gz = DB_BACKUP + ".restore.gz"
                await client.download_media(m, tmp_gz)
                with gzip.open(tmp_gz, "rb") as src, open(DB_PATH, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                os.remove(tmp_gz)
                break
        await client.disconnect()
        logging.info("DB restore: done")
    except Exception as e:
        logging.exception("DB restore failed: %s", e)

async def _tg_periodic_backup():
    if not TG_BACKUP or API_ID == 0 or not API_HASH:
        return
    from telethon import TelegramClient
    while True:
        await asyncio.sleep(DB_BACKUP_EVERY_MIN * 60)
        try:
            if not os.path.exists(DB_PATH):
                continue
            tmp_gz = DB_BACKUP + ".gz"
            with gzip.open(tmp_gz, "wb") as gz, open(DB_PATH, "rb") as src:
                shutil.copyfileobj(src, gz)
            client = TelegramClient("bot_backup_loop", API_ID, API_HASH)
            await client.start(bot_token=TOKEN)
            ts = datetime.datetime.utcnow().isoformat(timespec="seconds")
            await client.send_file(BACKUP_CHAT, tmp_gz, caption=f"bot.db.gz • {ts}")
            await client.disconnect()
            os.remove(tmp_gz)
            logging.info("DB backup sent")
        except Exception as e:
            logging.exception("DB backup failed: %s", e)

# ================== BOT ==================
bot = Bot(token=TOKEN)
helper_bot = Bot(token=HELPER_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = None

def esc(s: str | None) -> str:
    return "" if not s else s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def mention(uid: int, username: str | None, fb: str = "Profil") -> str:
    return f"@{username}" if username else f"<a href='tg://user?id={uid}'>{fb}</a>"

# ================== PENALTY MONITOR (24h) ==================
async def check_pendings():
    while True:
        await asyncio.sleep(600)
        now = datetime.datetime.utcnow()
        rows = conn.execute(
            "SELECT id, referrer_id, invited_id, join_time FROM referrals WHERE penalized=0 AND done=0"
        ).fetchall()
        for rid, ref_id, inv_id, jt in rows:
            try:
                join_t = datetime.datetime.fromisoformat(jt)
            except Exception:
                join_t = now
            hours = (now - join_t).total_seconds() / 3600
            if hours > 24:
                conn.execute("UPDATE referrals SET done=1 WHERE id=?", (rid,))
                conn.commit()
                continue

            in_all = True
            for ch in MANDATORY_CHANNELS:
                try:
                    m = await bot.get_chat_member(ch, inv_id)
                    if m.status not in ["member", "administrator", "creator"]:
                        in_all = False
                        break
                except Exception:
                    in_all = False
                    break

            if not in_all:
                conn.execute("""
                    UPDATE users
                    SET balance = CASE WHEN balance>=? THEN balance-? ELSE 0 END
                    WHERE id=?
                """, (PENALTY, PENALTY, ref_id))
                conn.execute("UPDATE referrals SET penalized=1 WHERE id=?", (rid,))
                conn.commit()
                u = conn.execute("SELECT username FROM users WHERE id=?", (inv_id,)).fetchone()
                uname = u[0] if u else None
                try:
                    await bot.send_message(
                        ref_id,
                        "⚠️ {usr} majburiy kanallardan biridan 24 soat ichida chiqib ketdi.\n"
                        f"−{PENALTY} so‘m balansingizdan ayirildi."
                        .format(usr=mention(inv_id, uname)),
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass

# ================== GATE (KANALLAR + MAJBURIY BOT) ==================
async def has_started_helper(user_id: int) -> bool:
    try:
        await helper_bot.send_chat_action(user_id, "typing")
        return True
    except Exception:
        return False

def gate_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🤖 Majburiy botni ochish", url=f"https://t.me/{HELPER_BOT_USERNAME.lstrip('@')}?start=start")],
    ]
    for ch in MANDATORY_CHANNELS:
        rows.append([InlineKeyboardButton(text=f"📢 Kanalga a’zo bo‘lish: {ch}", url=f"https://t.me/{ch.lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="gate_check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def gate_ok(user_id: int) -> bool:
    in_all_channels = True
    for ch in MANDATORY_CHANNELS:
        try:
            cm = await bot.get_chat_member(ch, user_id)
            if cm.status not in ["member", "administrator", "creator"]:
                in_all_channels = False
                break
        except Exception:
            in_all_channels = False
            break
    started_helper = await has_started_helper(user_id)
    return in_all_channels and started_helper

# ================== KEYBOARDS ==================
def kb_main(is_admin: bool):
    b = ReplyKeyboardBuilder()
    b.row(KeyboardButton(text=BTN_REF_MAIN), KeyboardButton(text=BTN_HELP))
    if is_admin:
        b.row(KeyboardButton(text=BTN_ADMIN))
    return b.as_markup(resize_keyboard=True)

def kb_ref():
    b = ReplyKeyboardBuilder()
    b.row(KeyboardButton(text=BTN_LINK), KeyboardButton(text=BTN_BALANCE))
    b.row(KeyboardButton(text=BTN_WITHDRAW))
    b.row(KeyboardButton(text=BTN_RULES))
    b.row(KeyboardButton(text=BTN_BACK))
    return b.as_markup(resize_keyboard=True)

def kb_admin():
    b = ReplyKeyboardBuilder()
    b.row(KeyboardButton(text=BTN_STATS))
    b.row(KeyboardButton(text=BTN_SENDALL))
    b.row(KeyboardButton(text=BTN_TOPREF))
    b.row(KeyboardButton(text=BTN_BACK))
    return b.as_markup(resize_keyboard=True)

def kb_share(link: str):
    enc = urllib.parse.quote(link)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Ulashish", url=f"https://t.me/share/url?url={enc}")]
    ])

def kb_wd_confirm():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="wd_ok")],
        [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="wd_cancel")]
    ])

# ================== STATES ==================
class AdminSend(StatesGroup):
    waiting = State()
    confirm = State()

class WD(StatesGroup):
    amount = State()
    card = State()
    name = State()
    confirm = State()

# ================== STARTUP ==================
async def on_startup():
    global BOT_USERNAME, conn
    me = await bot.get_me()
    BOT_USERNAME = me.username
    await _tg_restore_if_needed()
    conn = ensure_db()
    asyncio.create_task(_periodic_backup_task())
    asyncio.create_task(check_pendings())
    asyncio.create_task(_tg_periodic_backup())

# ================== HELPERS ==================
def parse_ref_arg(arg: str | None) -> int | None:
    if not arg:
        return None
    a = arg.strip()
    if a.startswith("ref_"):
        a = a.split("ref_", 1)[1]
    if a.isdigit():
        try:
            return int(a)
        except Exception:
            return None
    return None

def upsert_pending_ref(referee_id: int, referrer_id: int | None):
    if not referrer_id or referrer_id == referee_id:
        return
    conn.execute(
        "INSERT INTO pending_refs(referee_id, referrer_id, created_at) VALUES(?,?,?) "
        "ON CONFLICT(referee_id) DO UPDATE SET referrer_id=excluded.referrer_id, created_at=excluded.created_at",
        (referee_id, referrer_id, datetime.datetime.utcnow().isoformat())
    )
    conn.commit()

def pop_pending_ref(referee_id: int) -> int | None:
    row = conn.execute("SELECT referrer_id FROM pending_refs WHERE referee_id=?", (referee_id,)).fetchone()
    if not row:
        return None
    ref = row[0]
    conn.execute("DELETE FROM pending_refs WHERE referee_id=?", (referee_id,))
    conn.commit()
    return ref

# --- WELCOME (👋 Salom olib tashlandi, majburiyga ikki kanal qo‘shildi)
WELCOME = (
    "💸 Bu Zarafshon Pul Boti. Do‘stlaringizni taklif qiling va mukofot oling.\n"
    "✅ Hammasi haqqoniy. To‘lovlar karta orqali amalga oshiriladi.\n"
    "🟢 Birinchi marta 1 000 so‘m yigsangiz yechib olishingiz mumkin.\n"
    "📣 Taklif havolangizni yaqinlaringiz va guruhlarga yuboring.\n"
    "🤔 Nega pul beramiz? Kanal auditoriyasi kengayadi, biz foyda olamiz, siz esa mukofot olasiz.\n\n"
    "🔐 Majburiy: {helper} da /start, {chan1} va {chan2} ga a’zo bo‘lish.\n\n"
    "👇 Pastdagi tugmalardan foydalaning."
)

# =============== UNIVERSAL: menyu tugmasi bosilganda FSM tozalash ===============
async def ensure_gate_and_clear_state(message: types.Message, state: FSMContext) -> bool:
    await state.clear()
    if not await gate_ok(message.from_user.id):
        await message.reply(
            "🔐 <b>Kirishdan oldin</b>\n"
            f"1) 🤖 <b>Majburiy bot</b>: {HELPER_BOT_USERNAME} ni ochib <b>/start</b> bosing.\n"
            f"2) 📢 <b>Kanal</b>: {CHANNEL} <b>va</b> {SECOND_CHANNEL} ga a’zo bo‘ling.\n"
            "3) So‘ng <b>✅ Tekshirish</b> tugmasini bosing.",
            reply_markup=gate_keyboard(),
            parse_mode="HTML"
        )
        return False
    return True

# ================== HANDLERS ==================
@dp.message(Command("start"))
async def start(m: types.Message, state: FSMContext):
    await state.clear()
    parts = (m.text or "").split(maxsplit=1)
    ref_from_link = parse_ref_arg(parts[1] if len(parts) > 1 else None)
    if ref_from_link:
        upsert_pending_ref(m.from_user.id, ref_from_link)

    if not await gate_ok(m.from_user.id):
        await m.reply(
            "🔐 <b>Kirishdan oldin</b>\n"
            f"1) 🤖 <b>Majburiy bot</b>: {HELPER_BOT_USERNAME} ni ochib <b>/start</b> bosing.\n"
            f"2) 📢 <b>Kanal</b>: {CHANNEL} <b>va</b> {SECOND_CHANNEL} ga a’zo bo‘ling.\n"
            "3) So‘ng <b>✅ Tekshirish</b> tugmasini bosing.",
            reply_markup=gate_keyboard(),
            parse_mode="HTML"
        )
        return

    # Ism bilan salomlashish (faqat bitta salom)
    full_name = (m.from_user.full_name or "Do‘st")
    greet = f"Assalomu alaykum, {full_name}! 👋\n\n"

    user_row = conn.execute("SELECT id FROM users WHERE id=?", (m.from_user.id,)).fetchone()
    final_ref = pop_pending_ref(m.from_user.id) or ref_from_link

    if not user_row:
        if final_ref and final_ref != m.from_user.id:
            ref_exists = conn.execute("SELECT id FROM users WHERE id=?", (final_ref,)).fetchone()
            if ref_exists:
                conn.execute("UPDATE users SET balance=balance+? WHERE id=?", (AWARD, final_ref))
                conn.execute(
                    "INSERT INTO referrals(referrer_id, invited_id, join_time) VALUES(?,?,?)",
                    (final_ref, m.from_user.id, datetime.datetime.utcnow().isoformat())
                )
                try:
                    await bot.send_message(
                        final_ref,
                        "🆕 <b>Yangi referral</b>\n"
                        f"🧑‍🤝‍🧑 Yangi a’zo: {mention(m.from_user.id, m.from_user.username)} (ID: <code>{m.from_user.id}</code>)\n"
                        f"💰 Balansga +{AWARD} so‘m qo‘shildi.",
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass
        conn.execute(
            "INSERT INTO users(id, username, balance, referrer_id, has_withdrawn) VALUES(?,?,?,?,0)",
            (m.from_user.id, m.from_user.username, 0, final_ref)
        )
        conn.commit()
    else:
        conn.execute("UPDATE users SET username=? WHERE id=?", (m.from_user.username, m.from_user.id))
        conn.commit()

    text = greet + WELCOME.format(helper=HELPER_BOT_USERNAME, chan1=CHANNEL, chan2=SECOND_CHANNEL)
    await m.reply(text, reply_markup=kb_main(m.from_user.id in ADMINS), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "gate_check")
async def gate_recheck(c: types.CallbackQuery):
    if not await gate_ok(c.from_user.id):
        await c.answer("Hali shartlar bajarilmadi.", show_alert=True)
        return

    user_row = conn.execute("SELECT id FROM users WHERE id=?", (c.from_user.id,)).fetchone()
    final_ref = pop_pending_ref(c.from_user.id)

    if not user_row:
        if final_ref and final_ref != c.from_user.id:
            ref_exists = conn.execute("SELECT id FROM users WHERE id=?", (final_ref,)).fetchone()
            if ref_exists:
                conn.execute("UPDATE users SET balance=balance+? WHERE id=?", (AWARD, final_ref))
                conn.execute(
                    "INSERT INTO referrals(referrer_id, invited_id, join_time) VALUES(?,?,?)",
                    (final_ref, c.from_user.id, datetime.datetime.utcnow().isoformat())
                )
                try:
                    await bot.send_message(
                        final_ref,
                        "🆕 <b>Yangi referral</b>\n"
                        f"🧑‍🤝‍🧑 Yangi a’zo: {mention(c.from_user.id, c.from_user.username)} (ID: <code>{c.from_user.id}</code>)\n"
                        f"💰 Balansga +{AWARD} so‘m qo‘shildi.",
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass
        conn.execute(
            "INSERT INTO users(id, username, balance, referrer_id, has_withdrawn) VALUES(?,?,?,?,0)",
            (c.from_user.id, c.from_user.username, 0, final_ref)
        )
        conn.commit()

    try:
        await c.message.edit_text("✅ Tekshirildi. Davom etishingiz mumkin.")
    except Exception:
        pass
    await bot.send_message(c.from_user.id, "Menyu", reply_markup=kb_main(c.from_user.id in ADMINS))

# -------- Referral bo‘limi
@dp.message(StateFilter("*"), F.text == BTN_REF_MAIN)
async def ref_menu(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    await m.reply(
        "👥 Do‘stlarni taklif qilib pul ishlang.\n\n"
        "Qanday ishlaydi:\n"
        f"1) “{BTN_LINK}” — o‘zingizning havolangizni oling.\n"
        "2) Uni do‘stlarga yuboring.\n"
        f"3) Har bir yangi a’zo uchun balansingizga {AWARD} so‘m yoziladi.\n\n"
        "Quyidagi tugmalardan foydalaning.",
        reply_markup=kb_ref()
    )

@dp.message(StateFilter("*"), F.text == BTN_HELP)
async def help_menu(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    await m.reply(
        "ℹ️ <b>Yordam markazi</b>\n\n"
        "📌 <b>Botdan foydalanish</b>\n"
        f"• <b>{BTN_LINK}</b> — havolangizni oling va ulashing.\n"
        f"• <b>{BTN_BALANCE}</b> — jamg‘arma holatini ko‘ring.\n"
        f"• <b>{BTN_WITHDRAW}</b> — limitga yetgach kartaga so‘rov yuboring.\n\n"
        "✅ <b>Shartlar</b>\n"
        f"• {HELPER_BOT_USERNAME} da <b>/start</b> bosilgan bo‘lishi shart.\n"
        f"• {CHANNEL} <b>va</b> {SECOND_CHANNEL} ga a’zo bo‘lish shart.\n"
        "• 24 soat ichida taklif qilingan foydalanuvchi kanaldan chiqsa, <b>−100 so‘m</b> jarima.\n\n"
        "💰 <b>Mukofotlar manbai</b>\n"
        f"• To‘lovlar {CHANNEL} va {SECOND_CHANNEL} homiy budjetlaridan qoplanadi.\n\n"
        "📞 <b>Aloqa</b>\n"
        "• Telefon: <b>93 311 15 29</b>\n"
        "• Telegram: <b>@Behruz_shokirov</b>",
        parse_mode="HTML",
        reply_markup=kb_main(m.from_user.id in ADMINS)
    )

@dp.message(StateFilter("*"), F.text == BTN_LINK)
async def my_link(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    link = f"https://t.me/{BOT_USERNAME}?start={m.from_user.id}"
    await m.reply(
        "🔗 Sizning shaxsiy taklif havolangiz:\n"
        f"{link}\n\n"
        f"Ushbu havola orqali kirgan har bir do‘st uchun {AWARD} so‘m olasiz.\n"
        "📤 Quyidagi tugma orqali tez ulashing.",
        reply_markup=kb_share(link)
    )

@dp.message(StateFilter("*"), F.text == BTN_BALANCE)
async def my_balance(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    if m.from_user.id == TEST_USER_ID:
        await m.reply(f"💰 Balans: {TEST_USER_BALANCE:,} so‘m (test) ♾️".replace(",", " "))
        return
    row = conn.execute("SELECT balance, has_withdrawn FROM users WHERE id=?", (m.from_user.id,)).fetchone()
    bal = row[0] if row else 0
    has_w = row[1] if row else 0
    need = FIRST_MIN if has_w == 0 else NEXT_MIN
    status = "✅ Hozir yechishingiz mumkin." if bal >= need else "⏳ Hali yetarli emas."
    await m.reply(f"💰 Balans: {bal} so‘m\n🎯 Minimal yechish: {need} so‘m\n{status}")

@dp.message(StateFilter("*"), F.text == BTN_WITHDRAW)
async def wd_start(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    if m.from_user.id == TEST_USER_ID:
        await state.set_state(WD.amount)
        await m.reply(f"♾️ Test balans: {TEST_USER_BALANCE:,} so‘m.\n💸 Qancha yechamiz? Summani raqam bilan yuboring.")
        return
    row = conn.execute("SELECT balance, has_withdrawn FROM users WHERE id=?", (m.from_user.id,)).fetchone()
    bal = row[0] if row else 0
    has_w = row[1] if row else 0
    need = FIRST_MIN if has_w == 0 else NEXT_MIN
    if bal < need:
        await m.reply(f"⚠️ Minimal yechib olish: {need} so‘m.\n💰 Sizda hozir: {bal} so‘m.")
        return
    await state.set_state(WD.amount)
    await m.reply(f"💸 Qancha yechamiz? (≤ {bal})\n🔢 Summani raqam bilan yuboring.")

@dp.message(StateFilter("*"), F.text == BTN_RULES)
async def rules(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    await m.reply(
        "📜 Qoidalar\n"
        f"• Har bir haqiqiy taklif uchun {AWARD} so‘m beriladi.\n"
        f"• Birinchi yechib olish: {FIRST_MIN} so‘m. Keyingi yechishlar: {NEXT_MIN} so‘mdan.\n"
        "• Faqat real foydalanuvchilar hisoblanadi.\n"
        f"• Kanalga a’zo bo‘lish majburiy: {CHANNEL} va {SECOND_CHANNEL}.\n"
        f"• 24 soat ichida siz taklif qilgan foydalanuvchi kanaldan chiqsa, balansingizdan {PENALTY} so‘m jarima ayriladi.\n\n"
        "❓ FAQ\n"
        "• Mukofot qachon yoziladi? Do‘st kanalga a’zo bo‘lganda.\n"
        "• Nega pul beriladi? Kanal auditoriyasi oshgani uchun.\n"
        "• To‘lov manbai: homiy budjeti (majburiy kanallar)."
    )

@dp.message(StateFilter("*"), F.text == BTN_BACK)
async def back_btn(m: types.Message, state: FSMContext):
    await state.clear()
    await m.reply("🏠 Asosiy menyu:", reply_markup=kb_main(m.from_user.id in ADMINS))

# -------- Withdraw FSM
def _is_any_menu(text: str | None) -> bool:
    if not text: return False
    t = text.strip()
    return t in MAIN_BTNS or t in REF_BTNS

@dp.message(WD.amount)
async def wd_amount(m: types.Message, state: FSMContext):
    if _is_any_menu(m.text):
        await state.clear()
        await m.answer("❌ Yechib olish jarayoni bekor qilindi.")
        return
    digits = "".join(ch for ch in (m.text or "") if ch.isdigit())
    if not digits:
        await m.reply("❗ Summani faqat raqam bilan yuboring."); return
    amt = int(digits)
    if m.from_user.id != TEST_USER_ID:
        row = conn.execute("SELECT balance, has_withdrawn FROM users WHERE id=?", (m.from_user.id,)).fetchone()
        bal = row[0] if row else 0
        has_w = row[1] if row else 0
        need = FIRST_MIN if has_w == 0 else NEXT_MIN
        if amt < need or amt > bal:
            await m.reply(f"❗ Noto‘g‘ri summa. Minimal {need}, maksimal {bal}."); return
    await state.update_data(amount=amt)
    await state.set_state(WD.card)
    await m.reply("💳 Karta raqamini yuboring (16 xonali, faqat raqam).")

@dp.message(WD.card)
async def wd_card(m: types.Message, state: FSMContext):
    if _is_any_menu(m.text):
        await state.clear()
        await m.answer("❌ Yechib olish jarayoni bekor qilindi.")
        return
    digits = "".join(ch for ch in (m.text or "") if ch.isdigit())
    if len(digits) != 16:
        await m.reply("❗ 16 xonali karta raqamini yuboring."); return
    await state.update_data(card=digits)
    await state.set_state(WD.name)
    await m.reply("👤 Karta egasining <b>Ism Familiyasi</b>ni yuboring:", parse_mode="HTML")

@dp.message(WD.name)
async def wd_name(m: types.Message, state: FSMContext):
    if _is_any_menu(m.text):
        await state.clear()
        await m.answer("❌ Yechib olish jarayoni bekor qilindi.")
        return
    fio = (m.text or "").strip()
    if len(fio) < 3:
        await m.reply("❗ Ism Familiyani to‘liq yuboring."); return
    data = await state.get_data()
    amt = int(data["amount"])
    card = data["card"]
    masked = f"{card[:4]}****{card[4:8]}****{card[-4:]}"
    await state.update_data(name=fio)
    await state.set_state(WD.confirm)
    await m.reply(
        "🧾 <b>So‘rov</b>\n"
        f"• 💰 Summа: <b>{amt}</b>\n"
        f"• 💳 Karta: <b>{masked}</b>\n"
        f"• 👤 F.I.Sh: <b>{esc(fio)}</b>\n\n"
        "Tasdiqlaysizmi?",
        parse_mode="HTML",
        reply_markup=kb_wd_confirm()
    )

@dp.callback_query(lambda q: q.data in ["wd_ok", "wd_cancel"])
async def wd_confirm(c: types.CallbackQuery, state: FSMContext):
    if c.data == "wd_cancel":
        await state.clear()
        try: await c.message.edit_text("❌ Bekor qilindi.")
        except Exception: pass
        await c.answer()
        return

    data = await state.get_data()
    amt = int(data.get("amount", 0))
    card = data.get("card", "")
    fio = data.get("name", "")
    user_id = c.from_user.id

    if user_id != TEST_USER_ID:
        row = conn.execute("SELECT balance, has_withdrawn FROM users WHERE id=?", (user_id,)).fetchone()
        bal = row[0] if row else 0
        if amt <= 0 or amt > bal or len(card) != 16 or not fio:
            await state.clear()
            try: await c.message.edit_text("⚠️ Ma’lumotlar eskirgan. Qaytadan yuboring.")
            except Exception: pass
            await c.answer(); return
        conn.execute("UPDATE users SET balance=balance-? WHERE id=?", (amt, user_id))
        if (row[1] if row else 0) == 0:
            conn.execute("UPDATE users SET has_withdrawn=1 WHERE id=?", (user_id,))
        conn.commit()

    now_uz = (datetime.datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO withdrawals(user_id,amount,card_number,full_name,created_at) VALUES(?,?,?,?,?)",
        (user_id, amt, card, fio, now_uz)
    )
    conn.commit()

    invited_rows = conn.execute("""
        SELECT r.invited_id, u.username
        FROM referrals r
        LEFT JOIN users u ON u.id = r.invited_id
        WHERE r.referrer_id=?
        ORDER BY r.id DESC
    """, (user_id,)).fetchall()
    inv_count = len(invited_rows)
    mentions = [mention(inv_id, uname) for inv_id, uname in invited_rows[:30]]
    more = inv_count - len(mentions)
    invited_block = "\n👥 Taklif qilganlari: —"
    if inv_count > 0:
        invited_block = (
            f"\n👥 Taklif qilganlari: <b>{inv_count} ta</b>\n"
            + ("• " + ", ".join(mentions))
            + (f"\n… va yana <b>{more}</b> ta" if more > 0 else "")
        )

    uname_row = conn.execute("SELECT username FROM users WHERE id=?", (user_id,)).fetchone()
    uname = uname_row[0] if uname_row else c.from_user.username
    masked = f"{card[:4]}****{card[4:8]}****{card[-4:]}"
    try:
        await bot.send_message(
            WITHDRAW_GROUP_ID,
            "💸 <b>Yangi yechib olish</b>\n"
            f"👤 Foydalanuvchi: {mention(user_id, uname)} (ID: <code>{user_id}</code>)\n"
            f"💰 Summа: <b>{amt}</b>\n"
            f"💳 Karta: <code>{masked}</code>\n"
            f"👤 F.I.Sh: <b>{esc(fio)}</b>\n"
            f"🕒 {now_uz}"
            f"{invited_block}",
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception:
        pass

    await state.clear()
    try:
        await c.message.edit_text(
            "✅ So‘rov qabul qilindi. Operator ko‘rib chiqadi.\n\n"
            "📞 Aloqa: 93 311 15 29\n"
            "👤 Telegram: @Behruz_shokirov"
        )
    except Exception:
        pass
    await c.answer()

# -------- Admin panel
@dp.message(StateFilter("*"), F.text == BTN_ADMIN)
async def admin_panel(m: types.Message, state: FSMContext):
    await state.clear()
    if m.from_user.id not in ADMINS:
        return
    await m.reply("🛠 Admin Panel:", reply_markup=kb_admin())

@dp.message(StateFilter("*"), F.text == BTN_STATS)
async def stats(m: types.Message, state: FSMContext):
    await state.clear()
    if m.from_user.id not in ADMINS:
        return
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    wd_users = conn.execute("SELECT COUNT(DISTINCT user_id) FROM withdrawals").fetchone()[0]
    bal_sum = conn.execute("SELECT COALESCE(SUM(balance),0) FROM users WHERE id!=?", (TEST_USER_ID,)).fetchone()[0]
    await m.reply(
        "📊 Statistika\n"
        f"👥 Foydalanuvchilar: {total_users}\n"
        f"👤 Yechgan foydalanuvchilar: {wd_users}\n"
        f"💼 Jami balanslar yig‘indisi: {bal_sum} so‘m"
    )

@dp.message(StateFilter("*"), F.text == BTN_SENDALL)
async def bcast_start(m: types.Message, state: FSMContext):
    await state.clear()
    if m.from_user.id not in ADMINS:
        return
    await state.set_state(AdminSend.waiting)
    await m.reply("📤 Barchaga yuboriladigan xabarni yuboring (matn/rasm/video/fayl).")

@dp.message(AdminSend.waiting)
async def bcast_capture(m: types.Message, state: FSMContext):
    await state.update_data(mid=m.message_id, chat=m.chat.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Tasdiqlash", callback_data="send_ok")],
        [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="send_no")]
    ])
    await m.reply("❓ Ushbu xabar barchaga yuborilsinmi?", reply_markup=kb)

@dp.callback_query(lambda q: q.data in ["send_ok", "send_no"])
async def bcast_confirm(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in ADMINS:
        await c.answer(); return
    if c.data == "send_no":
        await state.clear()
        try: await c.message.edit_text("❌ Yuborish bekor qilindi.")
        except Exception: pass
        await c.answer(); return

    data = await state.get_data()
    mid = data.get("mid")
    chat = data.get("chat")
    users = [r[0] for r in conn.execute("SELECT id FROM users").fetchall()]
    sent = 0; fail = 0
    for uid in users:
        try:
            await bot.copy_message(uid, chat, mid)
            sent += 1
            await asyncio.sleep(0.03)
        except Exception:
            fail += 1
    await state.clear()
    try:
        await c.message.edit_text(f"✅ Xabar barchaga yuborildi. Yuborildi: {sent}/{len(users)} | ⚠️ Yetmadi: {fail}")
    except Exception:
        pass
    await c.answer()

@dp.message(StateFilter("*"), F.text == BTN_TOPREF)
async def active_referrers(m: types.Message, state: FSMContext):
    await state.clear()
    if m.from_user.id not in ADMINS:
        return
    rows = conn.execute("""
        SELECT referrer_id, COUNT(*) AS cnt
        FROM referrals
        GROUP BY referrer_id
        HAVING cnt > 0
        ORDER BY cnt DESC
    """).fetchall()
    if not rows:
        await m.reply("📉 Hozircha faol referrerlar yo‘q.")
        return
    text = "📈 Faol referrerlar:\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for ref_id, cnt in rows:
        u = conn.execute("SELECT username FROM users WHERE id=?", (ref_id,)).fetchone()
        uname = u[0] if u else None
        if uname:
            text += f"• @{uname}: {cnt} ta do‘st\n"
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"@{uname}", url=f"https://t.me/{uname}")])
        else:
            clickable_id = mention(ref_id, None, fb=f"ID {ref_id}")
            text += f"• {clickable_id}: {cnt} ta do‘st\n"
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"ID {ref_id}", url=f"tg://user?id={ref_id}")])
    await m.reply(text, reply_markup=kb, parse_mode="HTML")

# ================== RUN ==================
async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())