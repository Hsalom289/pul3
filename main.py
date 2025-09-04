# -*- coding: utf-8 -*-
# Aiogram 3.x
# pip install aiogram supabase

import asyncio
import logging
import datetime
from datetime import timedelta
import urllib.parse
from typing import Optional, Dict, List, Tuple

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

from supabase import create_client, Client  # <-- SUPABASE

logging.basicConfig(level=logging.INFO)

# ================== CONFIG ==================
TOKEN = "8436934185:AAGWATHLhZ0B04TDi79OoULLDAMJ4w78-Ik"
CHANNEL = "@uyzar_elonlar"
SECOND_CHANNEL = "@zarafshon_kanal"
MANDATORY_CHANNELS = [CHANNEL, SECOND_CHANNEL]

HELPER_BOT_USERNAME = "@uyzarbot"
HELPER_BOT_TOKEN = "7009289954:AAEssEXV8cZSGAKOShmFNiJMO2ldgJU5Nl4"

ADMINS = [1217732736, 6374979572]
TEST_USER_ID = 6374979572
TEST_USER_BALANCE = 1_000_000

WITHDRAW_GROUP_ID = -1002938188891

AWARD = 500
PENALTY = 100

# Bosqichli yechib olish limitlari
WITHDRAW_TIERS = [1000, 5000, 8000, 11000, 15000]  # 1..5; keyin ham 15000

# ---------- SUPABASE ----------
SUPABASE_URL = "https://bnuleyjyjdwvkzwcyalf.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJudWxleWp5amR3dmt6d2N5YWxmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDkzMDE5ODMsImV4cCI6MjAyNDg3Nzk4M30.UG-EKAXRYDDZANFwOKhM_0daycSutmC3DBqf-SLy3SU"
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Jadval nomlari
TBL_USERS = "userpul"
TBL_REFS = "referralpul"
TBL_WITHDRAWS = "withdrawpul"
TBL_PENDING = "pendingpul"

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

# ================== BOT ==================
bot = Bot(token=TOKEN)
helper_bot = Bot(token=HELPER_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = None

def esc(s: Optional[str]) -> str:
    return "" if not s else s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def mention(uid: int, username: Optional[str], fb: str = "Profil") -> str:
    return f"@{username}" if username else f"<a href='tg://user?id={uid}'>{fb}</a>"

# ================== SUPABASE DB ADAPTER ==================
class DB:
    # ---- USERS ----
    @staticmethod
    def get_user(uid: int) -> Optional[Dict]:
        resp = sb.table(TBL_USERS).select("*").eq("id", uid).limit(1).execute()
        return resp.data[0] if resp.data else None

    @staticmethod
    def insert_user(uid: int, username: Optional[str], referrer_id: Optional[int]):
        sb.table(TBL_USERS).upsert({
            "id": uid,
            "username": username,
            "balance": 0,
            "referrer_id": referrer_id,
            "has_withdrawn": False
        }, on_conflict="id").execute()

    @staticmethod
    def set_referrer_if_empty(uid: int, referrer_id: int):
        u = DB.get_user(uid)
        if u and (u.get("referrer_id") is None):
            sb.table(TBL_USERS).update({"referrer_id": referrer_id}).eq("id", uid).execute()

    @staticmethod
    def update_username(uid: int, username: Optional[str]):
        sb.table(TBL_USERS).update({"username": username}).eq("id", uid).execute()

    @staticmethod
    def add_balance(uid: int, amount: int):
        u = DB.get_user(uid)
        cur = u.get("balance", 0) if u else 0
        sb.table(TBL_USERS).update({"balance": cur + amount}).eq("id", uid).execute()

    @staticmethod
    def sub_balance_floor(uid: int, amount: int):
        u = DB.get_user(uid)
        cur = u.get("balance", 0) if u else 0
        newv = cur - amount
        if newv < 0: newv = 0
        sb.table(TBL_USERS).update({"balance": newv}).eq("id", uid).execute()

    @staticmethod
    def get_withdraw_count(uid: int) -> int:
        r = sb.table(TBL_WITHDRAWS).select("id", count="exact").eq("user_id", uid).execute()
        return (r.count if r.count is not None else len(r.data or [])) or 0

    @staticmethod
    def next_withdraw_min(uid: int) -> int:
        c = DB.get_withdraw_count(uid)
        return WITHDRAW_TIERS[c] if c < len(WITHDRAW_TIERS) else WITHDRAW_TIERS[-1]

    # ---- REFERRALS ----
    @staticmethod
    def insert_referral(referrer_id: int, invited_id: int, join_time_iso: str):
        sb.table(TBL_REFS).insert({
            "referrer_id": referrer_id,
            "invited_id": invited_id,
            "join_time": join_time_iso,
            "penalized": False,
            "done": False
        }).execute()

    @staticmethod
    def has_referral(referrer_id: int, invited_id: int) -> bool:
        r = sb.table(TBL_REFS).select("id").eq("referrer_id", referrer_id).eq("invited_id", invited_id).limit(1).execute()
        return bool(r.data)

    @staticmethod
    def open_referrals() -> List[Dict]:
        resp = sb.table(TBL_REFS).select("*").eq("penalized", False).eq("done", False).execute()
        return resp.data or []

    @staticmethod
    def mark_referral_done(rid: int):
        sb.table(TBL_REFS).update({"done": True}).eq("id", rid).execute()

    @staticmethod
    def mark_referral_penalized(rid: int):
        sb.table(TBL_REFS).update({"penalized": True}).eq("id", rid).execute()

    @staticmethod
    def get_invited_by(referrer_id: int) -> List[Tuple[int, Optional[str]]]:
        r = sb.table(TBL_REFS).select("invited_id").eq("referrer_id", referrer_id).order("id", desc=True).execute()
        ids = [row["invited_id"] for row in (r.data or [])]
        if not ids:
            return []
        u = sb.table(TBL_USERS).select("id,username").in_("id", ids).execute()
        umap = {row["id"]: row.get("username") for row in (u.data or [])}
        return [(i, umap.get(i)) for i in ids]

    # ---- PENDING ----
    @staticmethod
    def upsert_pending(referee_id: int, referrer_id: int, created_at_iso: str):
        sb.table(TBL_PENDING).upsert({
            "referee_id": referee_id,
            "referrer_id": referrer_id,
            "created_at": created_at_iso
        }, on_conflict="referee_id").execute()

    @staticmethod
    def pop_pending(referee_id: int) -> Optional[int]:
        r = sb.table(TBL_PENDING).select("referrer_id").eq("referee_id", referee_id).limit(1).execute()
        if not r.data:
            return None
        ref = r.data[0]["referrer_id"]
        sb.table(TBL_PENDING).delete().eq("referee_id", referee_id).execute()
        return ref

    # ---- WITHDRAWALS ----
    @staticmethod
    def insert_withdrawal(uid: int, amount: int, card_number: str, full_name: str, created_at_iso: str):
        sb.table(TBL_WITHDRAWS).insert({
            "user_id": uid,
            "amount": amount,
            "card_number": card_number,
            "full_name": full_name,
            "created_at": created_at_iso
        }).execute()

    # ---- STATS ----
    @staticmethod
    def count_users() -> int:
        r = sb.table(TBL_USERS).select("id", count="exact").execute()
        return (r.count if r.count is not None else len(r.data or [])) or 0

    @staticmethod
    def count_withdraw_users() -> int:
        r = sb.table(TBL_WITHDRAWS).select("user_id").execute()
        users = {row["user_id"] for row in (r.data or [])}
        return len(users)

    @staticmethod
    def sum_balances(exclude_id: Optional[int] = None) -> int:
        q = sb.table(TBL_USERS).select("id,balance")
        if exclude_id is not None:
            q = q.neq("id", exclude_id)
        r = q.execute()
        return sum((row.get("balance", 0) or 0) for row in (r.data or []))

# ================== FAST MEMBERSHIP CHECK ==================
async def has_started_helper(user_id: int) -> bool:
    try:
        await helper_bot.send_chat_action(user_id, "typing")
        return True
    except Exception:
        return False

async def channels_status(user_id: int) -> Dict[str, bool]:
    async def one(ch: str):
        try:
            cm = await bot.get_chat_member(ch, user_id)
            return cm.status in ("member", "administrator", "creator")
        except Exception:
            return False
    tasks = [one(ch) for ch in MANDATORY_CHANNELS]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    return {ch: ok for ch, ok in zip(MANDATORY_CHANNELS, results)}

async def gate_ok(user_id: int) -> bool:
    chs = await channels_status(user_id)
    helper_ok = await has_started_helper(user_id)
    return all(chs.values()) and helper_ok

def gate_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🤖 Majburiy botni ochish", url=f"https://t.me/{HELPER_BOT_USERNAME.lstrip('@')}?start=start")],
    ]
    for ch in MANDATORY_CHANNELS:
        rows.append([InlineKeyboardButton(text=f"📢 Kanalga a’zo bo‘lish: {ch}", url=f"https://t.me/{ch.lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="gate_check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

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
    global BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    asyncio.create_task(check_pendings())

# ================== HELPERS ==================
def parse_ref_arg(arg: Optional[str]) -> Optional[int]:
    if not arg:
        return None
    a = arg.strip()
    if a.startswith("ref_"):
        a = a.split("ref_", 1)[1]
    if a.isdigit():
        try: return int(a)
        except Exception: return None
    return None

def upsert_pending_ref(referee_id: int, referrer_id: Optional[int]):
    if not referrer_id or referrer_id == referee_id:
        return
    DB.upsert_pending(referee_id, referrer_id, datetime.datetime.utcnow().isoformat())

def pop_pending_ref(referee_id: int) -> Optional[int]:
    return DB.pop_pending(referee_id)

WELCOME = (
    "Assalomu alaykum, {name}! 👋\n\n"
    "💸 Bu Zarafshon Pul Boti. Do‘stlaringizni taklif qiling va mukofot oling.\n"
    "✅ Hammasi haqqoniy. To‘lovlar karta orqali amalga oshiriladi.\n"
    "🟢 Birinchi marta 1 000 so‘m yigsangiz yechib olishingiz mumkin.\n"
    "📣 Taklif havolangizni yaqinlaringiz va guruhlarga yuboring.\n"
    "🤔 Nega pul beramiz? Kanal auditoriyasi kengayadi, biz foyda olamiz, siz esa mukofot olasiz.\n\n"
    "🔐 Majburiy: {helper} da /start, {chan1} va {chan2} ga a’zo bo‘lish.\n\n"
    "👇 Pastdagi tugmalardan foydalaning."
)

async def ensure_gate_and_clear_state(message: types.Message, state: FSMContext) -> bool:
    await state.clear()
    if not await gate_ok(message.from_user.id):
        chs = await channels_status(message.from_user.id)
        missing = [ch for ch, ok in chs.items() if not ok]
        miss_txt = ("❗ Hali a’zo emassiz: " + ", ".join(missing)) if missing else ""
        await message.reply(
            "🔐 <b>Kirishdan oldin</b>\n"
            f"1) 🤖 <b>Majburiy bot</b>: {HELPER_BOT_USERNAME} ni ochib <b>/start</b> bosing.\n"
            f"2) 📢 <b>Kanal</b>: {CHANNEL} <b>va</b> {SECOND_CHANNEL} ga a’zo bo‘ling.\n"
            "3) So‘ng <b>✅ Tekshirish</b> tugmasini bosing.\n\n" + miss_txt,
            reply_markup=gate_keyboard(),
            parse_mode="HTML"
        )
        return False
    return True

# === Award helper (faqat bir marta) ===
async def award_referral_once(invited_id: int, referrer_id: int) -> bool:
    """Gate OK bo‘lganda chaqiriladi. Oldin berilmagan bo‘lsa +ref va +AWARD qiladi."""
    if referrer_id == invited_id:
        return False
    if not DB.get_user(referrer_id) or not DB.get_user(invited_id):
        return False
    if DB.has_referral(referrer_id, invited_id):
        return False
    # invited referrer_id boshqa bo‘lsa — hurmat qilamiz, yo‘q bo‘lsa o‘rnatamiz
    DB.set_referrer_if_empty(invited_id, referrer_id)
    DB.add_balance(referrer_id, AWARD)
    DB.insert_referral(referrer_id, invited_id, datetime.datetime.utcnow().isoformat())
    # xabar taklifchiga
    try:
        u = DB.get_user(invited_id)
        await bot.send_message(
            referrer_id,
            "🆕 <b>Yangi referral</b>\n"
            f"🧑‍🤝‍🧑 Yangi a’zo: {mention(invited_id, (u or {}).get('username'))} (ID: <code>{invited_id}</code>)\n"
            f"💰 Balansga +{AWARD} so‘m qo‘shildi.",
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception:
        pass
    return True

# ================== HANDLERS ==================
@dp.message(Command("start"))
async def start(m: types.Message, state: FSMContext):
    await state.clear()
    parts = (m.text or "").split(maxsplit=1)
    ref_from_link = parse_ref_arg(parts[1] if len(parts) > 1 else None)
    if ref_from_link:
        upsert_pending_ref(m.from_user.id, ref_from_link)

    # foydalanuvchini darhol DBga yozamiz / yangilaymiz
    if not DB.get_user(m.from_user.id):
        DB.insert_user(m.from_user.id, m.from_user.username, None)
    else:
        DB.update_username(m.from_user.id, m.from_user.username)

    if not await gate_ok(m.from_user.id):
        await m.reply(
            "🔐 <b>Kirishdan oldin</b>\n"
            f"1) 🤖 <b>Majburiy bot</b>: {HELPER_BOT_USERNAME} ni ochib <b>/start</b> bosing.\n"
            f"2) 📢 <b>Kanal</b>: {CHANNEL} <b>va</b> {SECOND_CHANNEL} ga a’zo bo‘ling.\n"
            "3) So‘ng <b>✅ Tekshirish</b> tugmasini bosing.",
            reply_markup=gate_keyboard(),
            parse_mode="HTML"
        )
    else:
        # Gate OK → referalni faqat bir marta kreditlash
        final_ref = pop_pending_ref(m.from_user.id) or ref_from_link
        if final_ref:
            await award_referral_once(m.from_user.id, final_ref)

    text = WELCOME.format(
        name=esc(m.from_user.full_name or ""),
        helper=HELPER_BOT_USERNAME, chan1=CHANNEL, chan2=SECOND_CHANNEL
    )
    await m.reply(text, reply_markup=kb_main(m.from_user.id in ADMINS), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "gate_check")
async def gate_recheck(c: types.CallbackQuery):
    if not await gate_ok(c.from_user.id):
        await c.answer("Hali shartlar bajarilmadi.", show_alert=True)
        return

    # Gate OK → referalni faqat bir marta kreditlash
    if not DB.get_user(c.from_user.id):
        DB.insert_user(c.from_user.id, c.from_user.username, None)
    else:
        DB.update_username(c.from_user.id, c.from_user.username)
    final_ref = pop_pending_ref(c.from_user.id)
    if final_ref:
        await award_referral_once(c.from_user.id, final_ref)

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
        "• 24 soat ichida taklif qilingan foydalanuvchi kanallardan chiqsa, <b>−100 so‘m</b> jarima.\n\n"
        "💵 Mukofotlar <b>@uyzar_elonlar</b> va <b>@zarafshon_kanal</b> budjetidan to‘lanadi.",
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
    u = DB.get_user(m.from_user.id)
    bal = (u or {}).get("balance", 0)
    need = DB.next_withdraw_min(m.from_user.id)
    status = "✅ Hozir yechishingiz mumkin." if bal >= need else "⏳ Hali yetarli emas."
    await m.reply(f"💰 Balans: {bal} so‘m\n🎯 Minimal yechish: {need} so‘m\n{status}")

# -------- Withdraw FSM
class WD(StatesGroup):
    amount = State()
    card = State()
    name = State()
    confirm = State()

def _is_any_menu(text: Optional[str]) -> bool:
    if not text: return False
    t = text.strip()
    return t in MAIN_BTNS or t in REF_BTNS

@dp.message(StateFilter("*"), F.text == BTN_WITHDRAW)
async def wd_start(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    if m.from_user.id == TEST_USER_ID:
        await state.set_state(WD.amount)
        await m.reply(f"♾️ Test balans: {TEST_USER_BALANCE:,} so‘m.\n💸 Qancha yechamiz? Summani raqam bilan yuboring.".replace(",", " "))
        return
    need = DB.next_withdraw_min(m.from_user.id)
    bal = (DB.get_user(m.from_user.id) or {}).get("balance", 0)
    if bal < need:
        await m.reply(f"⚠️ Minimal yechib olish: {need} so‘m.\n💰 Sizda hozir: {bal} so‘m.")
        return
    await state.set_state(WD.amount)
    await m.reply(f"💸 Qancha yechamiz? (≤ {bal})\n🔢 Summani raqam bilan yuboring.")

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
        need = DB.next_withdraw_min(m.from_user.id)
        bal = (DB.get_user(m.from_user.id) or {}).get("balance", 0)
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
        need = DB.next_withdraw_min(user_id)
        bal = (DB.get_user(user_id) or {}).get("balance", 0)
        if amt < need or amt > bal or len(card) != 16 or not fio:
            await state.clear()
            try: await c.message.edit_text("⚠️ Ma’lumotlar eskirgan yoki limit mos emas. Qaytadan yuboring.")
            except Exception: pass
            await c.answer(); return
        DB.sub_balance_floor(user_id, amt)
        now_uz_iso = (datetime.datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S")
        DB.insert_withdrawal(user_id, amt, card, fio, now_uz_iso)

    invited_rows = DB.get_invited_by(user_id)
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

    u = DB.get_user(user_id)
    uname = (u or {}).get("username", c.from_user.username)
    masked = f"{card[:4]}****{card[4:8]}****{card[-4:]}"
    try:
        await bot.send_message(
            WITHDRAW_GROUP_ID,
            "💸 <b>Yangi yechib olish</b>\n"
            f"👤 Foydalanuvchi: {mention(user_id, uname)} (ID: <code>{user_id}</code>)\n"
            f"💰 Summа: <b>{amt}</b>\n"
            f"💳 Karta: <code>{masked}</code>\n"
            f"👤 F.I.Sh: <b>{esc(fio)}</b>\n"
            f"🕒 {(datetime.datetime.utcnow() + timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')}"
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

# -------- Qoidalar (bosqichli limitlar qo‘shildi)
@dp.message(StateFilter("*"), F.text == BTN_RULES)
async def rules(m: types.Message, state: FSMContext):
    if not await ensure_gate_and_clear_state(m, state): return
    tiers_text = " → ".join(str(x) for x in WITHDRAW_TIERS)
    await m.reply(
        "📜 Qoidalar\n"
        f"• Har bir haqiqiy taklif uchun {AWARD} so‘m beriladi.\n"
        f"• Yechib olish bosqichlari: {tiers_text} (keyingi safarlar ham 15000 so‘mdan).\n"
        "• Faqat real foydalanuvchilar hisoblanadi.\n"
        "• Kanallarga a’zo bo‘lish majburiy: @uyzar_elonlar va @zarafshon_kanal.\n"
        f"• 24 soat ichida siz taklif qilgan foydalanuvchi kanallardan chiqsa, balansingizdan {PENALTY} so‘m jarima ayriladi.\n\n"
        "❓ FAQ\n"
        "• Mukofot qachon yoziladi? Do‘st kanallarga a’zo bo‘lganda.\n"
        "• Nega pul beriladi? Kanal auditoriyasi oshgani uchun.\n"
        "• Mukofotlar @uyzar_elonlar va @zarafshon_kanal budjetidan to‘lanadi.",
        parse_mode="HTML"
    )

@dp.message(StateFilter("*"), F.text == BTN_BACK)
async def back_btn(m: types.Message, state: FSMContext):
    await state.clear()
    await m.reply("🏠 Asosiy menyu:", reply_markup=kb_main(m.from_user.id in ADMINS))

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
    total_users = DB.count_users()
    wd_users = DB.count_withdraw_users()
    bal_sum = DB.sum_balances(exclude_id=TEST_USER_ID)
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
    r = sb.table(TBL_USERS).select("id").execute()
    users = [row["id"] for row in (r.data or [])]
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
    r = sb.table(TBL_REFS).select("referrer_id").execute()
    counts: Dict[int, int] = {}
    for row in (r.data or []):
        rid = row["referrer_id"]
        counts[rid] = counts.get(rid, 0) + 1
    if not counts:
        await m.reply("📉 Hozircha faol referrerlar yo‘q.")
        return
    sorted_rows = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    ids = [rid for rid, _ in sorted_rows]
    u = sb.table(TBL_USERS).select("id,username").in_("id", ids).execute()
    umap = {row["id"]: row.get("username") for row in (u.data or [])}

    text = "📈 Faol referrerlar:\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for rid, cnt in sorted_rows:
        uname = umap.get(rid)
        # Matnda hamisha mention() — username bo‘lmasa lichka linki
        text += f"• {mention(rid, uname)}: {cnt} ta do‘st\n"
        # Tugma: username bo‘lsa t.me, bo‘lmasa tg://user
        if uname:
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"@{uname}", url=f"https://t.me/{uname}")])
        else:
            kb.inline_keyboard.append([InlineKeyboardButton(text=f"ID {rid}", url=f"tg://user?id={rid}")])
    await m.reply(text, reply_markup=kb, parse_mode="HTML")

# ================== PENALTY MONITOR (24h) ==================
async def check_pendings():
    while True:
        await asyncio.sleep(60)  # 10 daqiqa
        now = datetime.datetime.utcnow()
        rows = DB.open_referrals()
        for ref in rows:
            rid = ref["id"]
            ref_id = ref["referrer_id"]
            inv_id = ref["invited_id"]
            jt = ref.get("join_time")
            try:
                join_t = datetime.datetime.fromisoformat(jt.replace("Z","")) if isinstance(jt, str) else now
            except Exception:
                join_t = now
            hours = (now - join_t.replace(tzinfo=None)).total_seconds() / 3600 if isinstance(join_t, datetime.datetime) else 0
            if hours > 24:
                DB.mark_referral_done(rid)
                continue

            chs = await channels_status(inv_id)
            in_all = all(chs.values())

            if not in_all:
                DB.sub_balance_floor(ref_id, PENALTY)
                DB.mark_referral_penalized(rid)
                u = DB.get_user(inv_id)
                uname = u.get("username") if u else None
                missing = [ch for ch, ok in chs.items() if not ok]
                miss_txt = ", ".join(missing) if missing else "majburiy kanal"
                try:
                    await bot.send_message(
                        ref_id,
                        ("⚠️ {usr} quyidagi kanal(lar)dan 24 soat ichida chiqib ketdi: "
                         f"<b>{miss_txt}</b>\n−{PENALTY} so‘m balansingizdan ayirildi.")
                        .format(usr=mention(inv_id, uname)),
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass

# ================== RUN ==================
async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
