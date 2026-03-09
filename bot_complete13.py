import asyncio
import logging
import sqlite3
import aiohttp
import json
from datetime import date
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# ════════════════════════════════════════════════════════
# НАСТРОЙКИ
# ════════════════════════════════════════════════════════
BOT_TOKEN        = "8609916854:AAHv5_-piLwrUktqiUnJ_YPZT1amXrHkeAA"
YOO_KASSA_TOKEN  = "390540012:LIVE:90402"
SUPPORT_USER     = "TT_Vieta"
SHOP_BANK        = "StarShopBank"        # user account (не бот!) для приёма NFT
ADMIN_ID         = 7830401684
ADMIN_IDS        = [7830401684, 7147395276]
ADMIN_WALLET_TON = "UQD-flkcU1_5HoAYjdO_hn4Mediv9vjFSV7bG6V5vrBTObrd"
CHANNEL_ID       = -1003794867266
CHANNEL_URL      = "https://t.me/MegaBuyStarShop"
RUB_PER_STAR     = 1.38
MIN_WITHDRAW     = 50
COMMISSION       = 0.03
WITHDRAW_RUB     = 30
MINI_APP_URL     = "https://jabrailkeligov2010-source.github.io/star-shop-app/"

# ════════════════════════════════════════════════════════
# USERBOT-НАСТРОЙКИ
# @StarShopBank — обычный TG-аккаунт. Userbot работает от его имени
# через Telethon и ловит входящие подарки автоматически.
#
# КАК НАСТРОИТЬ:
#   1. pip install telethon
#   2. Запусти один раз: python userbot_setup.py  (создаст shopbank.session)
#   3. Поставь USERBOT_ENABLED = True
#   4. Заполни USERBOT_API_ID и USERBOT_API_HASH с my.telegram.org
# ════════════════════════════════════════════════════════
USERBOT_ENABLED  = True
USERBOT_SESSION  = "shopbank"
USERBOT_API_ID   = 35259374        # ← my.telegram.org → App configuration → api_id
USERBOT_API_HASH = "2ccd52afcc03f089aab036c4b41d361c"       # ← my.telegram.org → App configuration → api_hash

# ════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO)
logging.info("=" * 50)
logging.info("✅ ВЕРСИЯ ФАЙЛА: НОВАЯ v6 — /mywallet работает")
logging.info("=" * 50)
bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher()

# ════════════════════════════════════════════════════════
# КУРС TON
# ════════════════════════════════════════════════════════
async def get_ton_rate() -> float:
    """Курс TON/RUB — фиксированный 105 ₽"""
    return 105.0


async def parse_nft_link(link: str) -> dict | None:
    """Парсит ссылку t.me/nft/NAME-NUM и возвращает данные NFT"""
    import re as _re
    # Извлекаем slug: SnakeBox-88712
    m = _re.search(r't\.me/nft/([A-Za-z0-9_-]+)', link)
    if not m:
        return None
    slug = m.group(1)
    url  = f"https://t.me/nft/{slug}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10),
                             headers={"User-Agent": "Mozilla/5.0"}) as r:
                html = await r.text()
    except Exception as e:
        logging.error(f"parse_nft_link fetch error: {e}")
        return None

    # og:image
    img_m = _re.search(r'og:image.*?content="([^"]+)"', html)
    if not img_m:
        img_m = _re.search(r'content="([^"]+)".*?og:image', html)
    image_url = img_m.group(1) if img_m else ""

    # Название и номер из title: "Snake Box #88712"
    title_m = _re.search(r'<title>([^<]+)</title>', html)
    title = title_m.group(1).strip() if title_m else slug
    # Убираем лишнее типа "Telegram: Collectible Gift"
    title = _re.sub(r'Telegram[^|]*\|?\s*', '', title).strip()

    # Парсим "Snake Box #88712" -> name="Snake Box", num="#88712"
    num_m = _re.search(r'(#\d+)', title)
    nft_num  = num_m.group(1) if num_m else ""
    nft_name = title.replace(nft_num, "").strip() if nft_num else title

    # Model из таблицы на странице
    model_m = _re.search(r'Model\s*</td>\s*<td[^>]*>([^<]+)', html)
    model = model_m.group(1).strip() if model_m else ""

    # Редкость по проценту в Model: < 1% = legendary, < 5% = epic, < 15% = rare, иначе common
    pct_m = _re.search(r'(\d+(?:\.\d+)?)\s*%', model) if model else None
    if pct_m:
        pct = float(pct_m.group(1))
        if pct < 1:      rarity = "legendary"
        elif pct < 5:    rarity = "epic"
        elif pct < 15:   rarity = "rare"
        else:            rarity = "common"
    else:
        rarity = "common"

    # Emoji по названию коллекции
    EMOJI_MAP = {
        "snake": "🐍", "box": "📦", "duck": "🦆", "bear": "🐻",
        "cat": "🐱", "dog": "🐶", "dragon": "🐉", "star": "⭐",
        "heart": "❤️", "diamond": "💎", "skull": "💀", "ghost": "👻",
        "robot": "🤖", "alien": "👽", "fire": "🔥", "ice": "❄️",
        "moon": "🌙", "sun": "☀️", "crown": "👑", "gem": "💎",
    }
    emoji = "🎁"
    name_lower = nft_name.lower()
    for key, em in EMOJI_MAP.items():
        if key in name_lower:
            emoji = em
            break

    return {
        "slug":      slug,
        "nft_name":  nft_name or slug,
        "nft_num":   nft_num  or f"#{slug.split('-')[-1]}",
        "emoji":     emoji,
        "rarity":    rarity,
        "image_url": image_url,
        "model":     model,
    }

async def rub_to_ton(rub: float) -> float:
    rate = await get_ton_rate()
    return round(rub / rate, 4)

async def ton_to_rub(ton: float) -> float:
    rate = await get_ton_rate()
    return round(ton * rate, 2)

# ════════════════════════════════════════════════════════
# БД
# ════════════════════════════════════════════════════════
DB = "starshop.db"

def _db():
    return sqlite3.connect(DB)

def init_db():
    with _db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id      INTEGER PRIMARY KEY,
            username     TEXT,
            referrer_id  INTEGER,
            balance_rub  REAL    DEFAULT 0,
            ref_stars    REAL    DEFAULT 0,
            last_bonus   TEXT    DEFAULT NULL,
            ton_wallet   TEXT    DEFAULT NULL,
            balance_ton  REAL    DEFAULT 0
        )""")
        migrations = [
            ("balance_rub", "REAL", "0"),
            ("ref_stars",   "REAL", "0"),
            ("last_bonus",  "TEXT", "NULL"),
            ("ton_wallet",  "TEXT", "NULL"),
            ("balance_ton", "REAL", "0"),
        ]
        for col, dtype, dflt in migrations:
            try:
                c.execute(f"ALTER TABLE users ADD COLUMN {col} {dtype} DEFAULT {dflt}")
            except Exception:
                pass

        c.execute("""CREATE TABLE IF NOT EXISTS vault (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id     INTEGER NOT NULL,
            nft_name     TEXT    NOT NULL,
            nft_number   TEXT    NOT NULL,
            emoji        TEXT    DEFAULT '🎁',
            rarity       TEXT    DEFAULT 'common',
            source       TEXT    DEFAULT 'bought',
            status       TEXT    DEFAULT 'stored',
            added_at     TEXT    DEFAULT (datetime('now')),
            image_url    TEXT    DEFAULT '',
            slug         TEXT    DEFAULT ''
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS listings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            vault_id     INTEGER NOT NULL,
            seller_id    INTEGER NOT NULL,
            seller_uname TEXT,
            price_rub    INTEGER NOT NULL,
            status       TEXT    DEFAULT 'active',
            created_at   TEXT    DEFAULT (datetime('now')),
            sold_at      TEXT,
            buyer_id     INTEGER,
            buyer_uname  TEXT
        )""")

        c.execute("""CREATE TABLE IF NOT EXISTS transactions (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER NOT NULL,
            type     TEXT    NOT NULL,
            amount   REAL    NOT NULL,
            comment  TEXT,
            ts       TEXT    DEFAULT (datetime('now'))
        )""")

        # Очередь ожидающих NFT (для userbot)
        # Миграция: добавляем новые поля если их нет
        try:
            c.execute("ALTER TABLE vault ADD COLUMN image_url TEXT DEFAULT ''")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE vault ADD COLUMN slug TEXT DEFAULT ''")
        except Exception:
            pass
        c.execute("""CREATE TABLE IF NOT EXISTS nft_pending (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id    INTEGER NOT NULL,
            seller_uname TEXT,
            status       TEXT    DEFAULT 'waiting',
            created_at   TEXT    DEFAULT (datetime('now'))
        )""")

# ════════════════════════════════════════════════════════
# HELPERS — пользователи
# ════════════════════════════════════════════════════════
def ensure_user(uid, uname=None, ref_id=None):
    with _db() as c:
        row = c.execute("SELECT user_id FROM users WHERE user_id=?", (uid,)).fetchone()
        if not row:
            c.execute(
                "INSERT INTO users (user_id,username,referrer_id) VALUES (?,?,?)",
                (uid, uname, ref_id if ref_id and ref_id != uid else None)
            )

def get_user(uid):
    with _db() as c:
        return c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

# ── Рублёвый баланс ──
def get_balance(uid) -> float:
    u = get_user(uid)
    return float(u[3]) if u and u[3] else 0.0

def add_balance(uid, amount: float):
    with _db() as c:
        c.execute("UPDATE users SET balance_rub=balance_rub+? WHERE user_id=?", (amount, uid))
        c.execute("INSERT INTO transactions (user_id,type,amount,comment) VALUES (?,?,?,?)",
                  (uid, "credit_rub", amount, "пополнение ₽"))

def deduct_balance(uid, amount: float) -> bool:
    with _db() as c:
        row = c.execute("SELECT balance_rub FROM users WHERE user_id=?", (uid,)).fetchone()
        bal = float(row[0]) if row and row[0] else 0.0
        if bal < amount:
            return False
        c.execute("UPDATE users SET balance_rub=balance_rub-? WHERE user_id=?", (amount, uid))
        c.execute("INSERT INTO transactions (user_id,type,amount,comment) VALUES (?,?,?,?)",
                  (uid, "debit_rub", amount, "списание ₽"))
        return True

# ── TON-баланс ──
def get_balance_ton(uid) -> float:
    u = get_user(uid)
    if not u or len(u) < 8:
        return 0.0
    return float(u[7]) if u[7] else 0.0

def add_balance_ton(uid, amount: float):
    with _db() as c:
        c.execute("UPDATE users SET balance_ton=balance_ton+? WHERE user_id=?", (amount, uid))
        c.execute("INSERT INTO transactions (user_id,type,amount,comment) VALUES (?,?,?,?)",
                  (uid, "credit_ton", amount, "пополнение TON"))

def deduct_balance_ton(uid, amount: float) -> bool:
    with _db() as c:
        row = c.execute("SELECT balance_ton FROM users WHERE user_id=?", (uid,)).fetchone()
        bal = float(row[0]) if row and row[0] else 0.0
        if bal < amount:
            return False
        c.execute("UPDATE users SET balance_ton=balance_ton-? WHERE user_id=?", (amount, uid))
        c.execute("INSERT INTO transactions (user_id,type,amount,comment) VALUES (?,?,?,?)",
                  (uid, "debit_ton", amount, "списание TON"))
        return True

def get_ton_wallet(uid) -> str:
    u = get_user(uid)
    return (u[6] or "").strip() if u else ""

def set_ton_wallet(uid, wallet: str):
    with _db() as c:
        c.execute("UPDATE users SET ton_wallet=? WHERE user_id=?", (wallet, uid))

def get_ref_stars(uid) -> float:
    u = get_user(uid)
    return float(u[4]) if u and u[4] else 0.0

def add_ref_stars(uid, amount: float):
    with _db() as c:
        c.execute("UPDATE users SET ref_stars=ref_stars+? WHERE user_id=?", (amount, uid))

def reset_ref_stars(uid):
    with _db() as c:
        c.execute("UPDATE users SET ref_stars=0 WHERE user_id=?", (uid,))

def get_referrer(uid):
    u = get_user(uid)
    return u[2] if u else None

def get_last_bonus(uid):
    u = get_user(uid)
    return u[5] if u else None

def set_last_bonus(uid, val):
    with _db() as c:
        c.execute("UPDATE users SET last_bonus=? WHERE user_id=?", (val, uid))

# ════════════════════════════════════════════════════════
# HELPERS — vault, listings
# ════════════════════════════════════════════════════════
def add_to_vault(owner_id, nft_name, nft_number, emoji="🎁", rarity="common", source="bought", image_url="", slug="") -> int:
    with _db() as c:
        cur = c.execute(
            "INSERT INTO vault (owner_id,nft_name,nft_number,emoji,rarity,source,image_url,slug) VALUES (?,?,?,?,?,?,?,?)",
            (owner_id, nft_name, nft_number, emoji, rarity, source, image_url, slug)
        )
        vid = cur.lastrowid
        logging.info(f"add_to_vault: owner={owner_id} name={nft_name!r} num={nft_number!r} vid={vid}")
        return vid

def get_vault(uid):
    with _db() as c:
        return c.execute("SELECT * FROM vault WHERE owner_id=? ORDER BY added_at DESC", (uid,)).fetchall()

def get_vault_item(vid):
    with _db() as c:
        return c.execute("SELECT * FROM vault WHERE id=?", (vid,)).fetchone()

def update_vault_status(vid, status: str):
    with _db() as c:
        c.execute("UPDATE vault SET status=? WHERE id=?", (status, vid))

def add_listing(vault_id, seller_id, seller_uname, price_rub) -> int:
    with _db() as c:
        cur = c.execute(
            "INSERT INTO listings (vault_id,seller_id,seller_uname,price_rub) VALUES (?,?,?,?)",
            (vault_id, seller_id, seller_uname, price_rub)
        )
        return cur.lastrowid

def get_listing(lid):
    with _db() as c:
        return c.execute("SELECT * FROM listings WHERE id=?", (lid,)).fetchone()

def get_active_listings():
    with _db() as c:
        return c.execute(
            """SELECT l.*, v.nft_name, v.nft_number, v.emoji, v.rarity
               FROM listings l JOIN vault v ON l.vault_id=v.id
               WHERE l.status='active' ORDER BY l.created_at DESC"""
        ).fetchall()

def mark_listing_sold(lid, buyer_id, buyer_uname):
    with _db() as c:
        c.execute(
            "UPDATE listings SET status='sold',buyer_id=?,buyer_uname=?,sold_at=datetime('now') WHERE id=?",
            (buyer_id, buyer_uname, lid)
        )

# ── NFT-очередь для userbot ──
def add_nft_pending(seller_id: int, seller_uname: str) -> int:
    with _db() as c:
        cur = c.execute(
            "INSERT INTO nft_pending (seller_id, seller_uname) VALUES (?,?)",
            (seller_id, seller_uname)
        )
        return cur.lastrowid

def get_pending_by_username(username: str):
    with _db() as c:
        return c.execute(
            """SELECT p.* FROM nft_pending p
               JOIN users u ON p.seller_id=u.user_id
               WHERE (u.username=? OR p.seller_uname=?) AND p.status='waiting'
               ORDER BY p.created_at LIMIT 1""",
            (username, username)
        ).fetchone()

def mark_pending_matched(pending_id: int):
    with _db() as c:
        c.execute("UPDATE nft_pending SET status='matched' WHERE id=?", (pending_id,))

# ════════════════════════════════════════════════════════
# FSM STATES
# ════════════════════════════════════════════════════════
class BuyFlow(StatesGroup):
    waiting_friend = State()
    waiting_amount = State()

class DepositFlow(StatesGroup):
    waiting_amount = State()

class ConvertFlow(StatesGroup):
    waiting_amount = State()

# ════════════════════════════════════════════════════════
# УВЕДОМЛЕНИЯ
# ════════════════════════════════════════════════════════
async def notify_admins(text: str, kb=None):
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(
                aid, text, parse_mode="HTML",
                reply_markup=kb.as_markup() if kb else None
            )
        except Exception as e:
            logging.error(f"notify_admin {aid}: {e}")

async def notify_ref(buyer_id: int, stars: int):
    ref_id = get_referrer(buyer_id)
    if not ref_id:
        return
    bonus = round(stars * 0.02, 2)
    add_ref_stars(ref_id, bonus)
    total = get_ref_stars(ref_id)
    try:
        await bot.send_message(
            ref_id,
            f"🎉 Реферал купил <b>{stars} ⭐</b> — тебе <b>+{bonus:.2f} ⭐</b>!\n"
            f"💫 Реферальный баланс: <b>{total:.2f} ⭐</b>\n\n"
            f"Открой магазин чтобы увидеть обновлённый баланс.",
            parse_mode="HTML"
        )
    except Exception:
        passs

def require_wallet(uid: int) -> bool:
    return bool(get_ton_wallet(uid))

# ════════════════════════════════════════════════════════
# ПОДПИСКА
# ════════════════════════════════════════════════════════
async def check_sub(uid: int) -> bool:
    try:
        m = await bot.get_chat_member(CHANNEL_ID, uid)
        return m.status in ("member", "administrator", "creator")
    except Exception:
        return False

def sub_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Подписаться", url=CHANNEL_URL)
    kb.button(text="✅ Проверить",   callback_data="check_sub")
    kb.adjust(1)
    return kb.as_markup()

@dp.callback_query(F.data == "check_sub")
async def cb_check_sub(cb: types.CallbackQuery, state: FSMContext):
    if await check_sub(cb.from_user.id):
        await cb.message.delete()
        await send_menu(cb.message, state)
    else:
        await cb.message.delete()
        await send_menu(cb.message, state)

# ════════════════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ
# ════════════════════════════════════════════════════════
async def send_menu(message: types.Message, state: FSMContext):
    await state.clear()
    uid    = message.chat.id
    bal_r  = get_balance(uid)
    bal_t  = get_balance_ton(uid)
    wallet = get_ton_wallet(uid)
    refs   = get_ref_stars(uid)

    # Данные передаём через startapp параметр (компактно — только важное)
    import base64, json as _json
    user_data = {
        "b":  round(bal_r, 2),      # balance rub
        "bt": round(bal_t, 4),      # balance ton
        "w":  wallet or "",          # wallet
        "r":  round(refs, 2),        # ref stars
    }
    # Листинги и хранилище — компактно
    with _db() as c:
        listings_raw = c.execute(
            """SELECT l.id, v.nft_name, v.nft_number, v.emoji, v.rarity,
                      l.price_rub, l.seller_id, u.username
               FROM listings l
               JOIN vault v ON l.vault_id=v.id
               LEFT JOIN users u ON l.seller_id=u.user_id
               WHERE l.status='active'
               ORDER BY l.created_at DESC LIMIT 30"""
        ).fetchall()
        vault_raw = c.execute(
            "SELECT id,nft_name,nft_number,emoji,rarity,status,COALESCE(image_url,''),COALESCE(slug,'') FROM vault WHERE owner_id=? ORDER BY added_at DESC LIMIT 30",
            (uid,)
        ).fetchall()
        my_listings_raw = c.execute(
            """SELECT l.id, v.nft_name, v.nft_number, v.emoji, l.price_rub, l.status
               FROM listings l JOIN vault v ON l.vault_id=v.id
               WHERE l.seller_id=? ORDER BY l.created_at DESC LIMIT 20""",
            (uid,)
        ).fetchall()

    user_data["l"] = [[r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7] or ""] for r in listings_raw]
    user_data["v"] = [[r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]] for r in vault_raw]
    user_data["m"] = [[r[0],r[1],r[2],r[3],r[4],r[5]] for r in my_listings_raw]

    # Base64 кодирование — компактнее чем URL encoding
    encoded = base64.urlsafe_b64encode(
        _json.dumps(user_data, ensure_ascii=False, separators=(',',':')).encode()
    ).decode().rstrip('=')

    # Передаём через startapp (Telegram поддерживает до 4096 символов)
    if len(encoded) > 3000:
        # Обрезаем до 10 NFT и 10 листингов если данных слишком много
        user_data2 = {"b": user_data["b"], "bt": user_data["bt"], "w": user_data["w"], "r": user_data["r"],
                      "v": user_data["v"][:10], "l": user_data["l"][:10], "m": user_data["m"][:5]}
        encoded = base64.urlsafe_b64encode(
            _json.dumps(user_data2, ensure_ascii=False, separators=(',',':')).encode()
        ).decode().rstrip('=')

    app_url = f"{MINI_APP_URL}#startapp={encoded}"
    logging.info(f"send_menu uid={uid} encoded_len={len(encoded)} vault={len(vault_raw)} listings={len(listings_raw)}")

    kb = InlineKeyboardBuilder()
    kb.button(text="🌐 Открыть магазин", web_app=types.WebAppInfo(url=app_url))
    kb.button(text="⭐ Купить звёзды",   callback_data="buy_stars_menu")
    kb.button(text="⭐ Заработать",      callback_data="ref_menu")
    kb.button(text="👨‍💻 Поддержка",       url=f"https://t.me/{SUPPORT_USER}")
    kb.button(text="📝 Отзывы",          url="https://t.me/+T9eTb0u_eso3NTAy")
    kb.adjust(1, 2, 1, 1)

    wallet_line = (
        f"👛 <code>{wallet[:8]}...{wallet[-4:]}</code>"
        if wallet else
        "👛 <b>Кошелёк не привязан</b> — привяжи в магазине"
    )

    await message.answer(
        f"⭐ <b>Star Shop</b>\n\n"
        f"💰 Рублёвый баланс: <b>{bal_r:.2f} ₽</b>\n"
        f"💎 TON-баланс: <b>{bal_t:.4f} TON</b>\n"
        f"{wallet_line}\n\n"
        f"<i>Пополнение, конвертация и NFT — в магазине.</i>",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )

@dp.message(Command("start"))
async def cmd_start(msg: types.Message, state: FSMContext, command: CommandObject = None):
    args = command.args if command and command.args else ""
    uid   = msg.from_user.id
    uname = msg.from_user.username or str(uid)

    # Команда из Mini App: /start app_BASE64
    if args.startswith("app_"):
        ensure_user(uid, uname)
        try:
            import base64 as _b64
            raw = args[4:]  # убираем "app_"
            # восстанавливаем padding
            pad = 4 - len(raw) % 4
            if pad != 4:
                raw += "=" * pad
            decoded = _b64.b64decode(raw.replace("-", "+").replace("_", "/")).decode()
            data = json.loads(decoded)
            action = data.get("action", "")
            logging.info(f"MiniApp deeplink uid={uid} action={action!r}")
            # Переиспользуем логику on_miniapp
            fake_msg = msg  # используем то же сообщение
            await _handle_miniapp_action(fake_msg, state, data, uid, uname)
        except Exception as e:
            logging.error(f"deeplink parse error: {e}", exc_info=True)
            await send_menu(msg, state)
        return

    ref_id = int(args) if args.isdigit() else None
    ensure_user(uid, uname, ref_id)
    if not await check_sub(uid):
        return await msg.answer(
            "🛑 <b>Подпишись на канал!</b>",
            reply_markup=sub_kb(), parse_mode="HTML"
        )
    await send_menu(msg, state)

@dp.message(Command("mywallet"))
async def cmd_mywallet(msg: types.Message):
    """Диагностика — показывает что реально сохранено в БД"""
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)
    wallet = get_ton_wallet(uid)
    bal_r  = get_balance(uid)
    bal_t  = get_balance_ton(uid)
    if wallet:
        await msg.answer(
            f"✅ <b>Кошелёк в базе:</b>\n<code>{wallet}</code>\n\n"
            f"💰 Рублёвый: {bal_r:.2f} ₽\n"
            f"💎 TON: {bal_t:.4f} TON",
            parse_mode="HTML"
        )
    else:
        await msg.answer(
            "❌ <b>Кошелёк НЕ сохранён в базе.</b>\n\n"
            "Открой магазин → Профиль → введи адрес → нажми «Сохранить»",
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await send_menu(cb.message, state)
    await cb.answer()

# ════════════════════════════════════════════════════════
# ПОПОЛНЕНИЕ БАЛАНСА
# ════════════════════════════════════════════════════════
@dp.callback_query(F.data == "deposit_menu")
async def cb_deposit_menu(cb: types.CallbackQuery):
    uid   = cb.from_user.id
    bal_r = get_balance(uid)
    bal_t = get_balance_ton(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Пополнить ₽ — картой (ЮKassa)", callback_data="deposit_card")
    kb.button(text="💎 Пополнить TON — Tonkeeper",      callback_data="deposit_ton")
    kb.button(text="🔙 Назад",                          callback_data="menu")
    kb.adjust(1)
    await cb.message.answer(
        f"💰 <b>Пополнение баланса</b>\n\n"
        f"💰 Рублёвый баланс: <b>{bal_r:.2f} ₽</b>\n"
        f"💎 TON-баланс: <b>{bal_t:.4f} TON</b>\n\n"
        f"Оба баланса можно тратить независимо.\n"
        f"Хочешь перевести один в другой? Используй «🔄 Конвертировать».",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "deposit_card")
async def cb_deposit_card(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("💳 Введи сумму пополнения в рублях (минимум 100 ₽):")
    await state.set_state(DepositFlow.waiting_amount)
    await state.update_data(method="card")
    await cb.answer()

@dp.callback_query(F.data == "deposit_ton")
async def cb_deposit_ton(cb: types.CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    if not require_wallet(uid):
        return await cb.answer("❌ Сначала привяжи TON кошелёк в магазине!", show_alert=True)
    await cb.message.answer("💎 Введи сумму TON для пополнения (минимум 0.5 TON):")
    await state.set_state(DepositFlow.waiting_amount)
    await state.update_data(method="ton")
    await cb.answer()

@dp.message(DepositFlow.waiting_amount)
async def msg_deposit_amount(msg: types.Message, state: FSMContext):
    d      = await state.get_data()
    method = d.get("method", "card")
    text   = msg.text.strip().replace(",", ".")
    uid    = msg.from_user.id

    if method == "card":
        if not text.isdigit():
            return await msg.answer("❌ Введи целое число рублей!")
        amount = int(text)
        if amount < 100:
            return await msg.answer("❌ Минимум 100 ₽!")
        await bot.send_invoice(
            msg.chat.id,
            title="Пополнение баланса",
            description=f"Пополнение рублёвого баланса на {amount} ₽",
            provider_token=YOO_KASSA_TOKEN,
            currency="rub",
            prices=[types.LabeledPrice(label=f"{amount} ₽", amount=amount * 100)],
            payload=f"deposit_rub:{amount}:{uid}"
        )
    else:
        try:
            amount_ton = float(text)
        except ValueError:
            return await msg.answer("❌ Введи сумму числом, например: 1.5")
        if amount_ton < 0.5:
            return await msg.answer("❌ Минимум 0.5 TON!")
        nanos = int(amount_ton * 1e9)
        turl  = f"ton://transfer/{ADMIN_WALLET_TON}?amount={nanos}"
        kb = InlineKeyboardBuilder()
        kb.button(text=f"📲 Перевести {amount_ton} TON", url=turl)
        kb.button(text="✅ Я перевёл",                   callback_data=f"dep_ton_check:{amount_ton}:{uid}")
        kb.adjust(1)
        await msg.answer(
            f"💎 <b>Пополнение TON-баланса</b>\n\n"
            f"К переводу: <b>{amount_ton} TON</b>\n\n"
            f"1. Нажми «Перевести» → оплати в Tonkeeper\n"
            f"2. Нажми «Я перевёл» — администратор проверит и начислит",
            reply_markup=kb.as_markup(), parse_mode="HTML"
        )
    await state.clear()

@dp.callback_query(F.data.startswith("dep_ton_check:"))
async def cb_dep_ton_check(cb: types.CallbackQuery):
    parts      = cb.data.split(":")
    amount_ton = float(parts[1])
    uid        = int(parts[2])
    uname      = cb.from_user.username or str(uid)
    ka = InlineKeyboardBuilder()
    ka.button(text=f"✅ Начислить {amount_ton} TON", callback_data=f"dep_ton_approve:{amount_ton}:{uid}")
    ka.button(text="❌ Отклонить",                   callback_data=f"dep_reject:{uid}")
    ka.adjust(1)
    await notify_admins(
        f"💎 <b>ПОПОЛНЕНИЕ TON-БАЛАНСА</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 @{uname} (ID:{uid})\n"
        f"💎 Сумма: {amount_ton} TON\n\n"
        f"📌 Проверь кошелёк и начисли баланс!",
        kb=ka
    )
    await cb.message.answer("📨 Заявка отправлена! Ожидай начисления.")
    await cb.answer()

@dp.callback_query(F.data.startswith("dep_ton_approve:"))
async def cb_dep_ton_approve(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    parts      = cb.data.split(":")
    amount_ton = float(parts[1])
    uid        = int(parts[2])
    add_balance_ton(uid, amount_ton)
    try:
        await bot.send_message(
            uid,
            f"✅ <b>TON-баланс пополнен!</b>\n"
            f"+{amount_ton} TON\n"
            f"Текущий: <b>{get_balance_ton(uid):.4f} TON</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + f"\n\n✅ Начислено {amount_ton} TON", parse_mode="HTML")
    await cb.answer("✅")

@dp.callback_query(F.data.startswith("dep_reject:"))
async def cb_dep_reject(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    try:
        await bot.send_message(uid, "❌ Пополнение отклонено. Обратись в поддержку.")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено", parse_mode="HTML")
    await cb.answer("❌")

# ════════════════════════════════════════════════════════
# КОНВЕРТАЦИЯ БАЛАНСА
# ════════════════════════════════════════════════════════
@dp.callback_query(F.data == "convert_menu")
async def cb_convert_menu(cb: types.CallbackQuery):
    uid   = cb.from_user.id
    bal_r = get_balance(uid)
    bal_t = get_balance_ton(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 ₽ → TON (конвертировать рубли в TON)", callback_data="convert_rub_to_ton")
    kb.button(text="💎 TON → ₽ (конвертировать TON в рубли)", callback_data="convert_ton_to_rub")
    kb.button(text="🔙 Назад", callback_data="menu")
    kb.adjust(1)
    await cb.message.answer(
        f"🔄 <b>Конвертация баланса</b>\n\n"
        f"💰 Рублёвый баланс: <b>{bal_r:.2f} ₽</b>\n"
        f"💎 TON-баланс: <b>{bal_t:.4f} TON</b>\n\n"
        f"Конвертация происходит по текущему курсу TON.\n"
        f"<i>Без дополнительных комиссий.</i>",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "convert_rub_to_ton")
async def cb_convert_rub_to_ton(cb: types.CallbackQuery, state: FSMContext):
    uid   = cb.from_user.id
    bal_r = get_balance(uid)
    rate  = await get_ton_rate()
    await cb.message.answer(
        f"💰→💎 <b>Рубли → TON</b>\n\n"
        f"Доступно: <b>{bal_r:.2f} ₽</b>\n"
        f"Курс: <b>1 TON = {rate:.0f} ₽</b>\n\n"
        f"Введи сумму в рублях (минимум 100 ₽):",
        parse_mode="HTML"
    )
    await state.set_state(ConvertFlow.waiting_amount)
    await state.update_data(direction="rub_to_ton")
    await cb.answer()

@dp.callback_query(F.data == "convert_ton_to_rub")
async def cb_convert_ton_to_rub(cb: types.CallbackQuery, state: FSMContext):
    uid   = cb.from_user.id
    bal_t = get_balance_ton(uid)
    rate  = await get_ton_rate()
    await cb.message.answer(
        f"💎→💰 <b>TON → Рубли</b>\n\n"
        f"Доступно: <b>{bal_t:.4f} TON</b>\n"
        f"Курс: <b>1 TON = {rate:.0f} ₽</b>\n\n"
        f"Введи сумму в TON (минимум 0.1 TON):",
        parse_mode="HTML"
    )
    await state.set_state(ConvertFlow.waiting_amount)
    await state.update_data(direction="ton_to_rub")
    await cb.answer()

@dp.message(ConvertFlow.waiting_amount)
async def msg_convert_amount(msg: types.Message, state: FSMContext):
    d         = await state.get_data()
    direction = d.get("direction", "rub_to_ton")
    uid       = msg.from_user.id
    text      = msg.text.strip().replace(",", ".")

    if direction == "rub_to_ton":
        if not text.isdigit():
            return await msg.answer("❌ Введи целое число рублей!")
        amount_rub = int(text)
        if amount_rub < 100:
            return await msg.answer("❌ Минимум 100 ₽!")
        if get_balance(uid) < amount_rub:
            return await msg.answer(f"❌ Недостаточно на рублёвом балансе! Доступно: {get_balance(uid):.2f} ₽")
        ton = await rub_to_ton(amount_rub)
        kb = InlineKeyboardBuilder()
        kb.button(text=f"✅ Конвертировать {amount_rub} ₽ → {ton} TON",
                  callback_data=f"do_convert:rub_to_ton:{amount_rub}:{ton}")
        kb.button(text="❌ Отмена", callback_data="convert_menu")
        kb.adjust(1)
        await msg.answer(
            f"🔄 <b>Подтверди конвертацию</b>\n\n"
            f"💰 {amount_rub} ₽  →  💎 {ton} TON",
            reply_markup=kb.as_markup(), parse_mode="HTML"
        )
    else:
        try:
            amount_ton = float(text)
        except ValueError:
            return await msg.answer("❌ Введи число, например: 0.5")
        if amount_ton < 0.1:
            return await msg.answer("❌ Минимум 0.1 TON!")
        if get_balance_ton(uid) < amount_ton:
            return await msg.answer(f"❌ Недостаточно TON-баланса! Доступно: {get_balance_ton(uid):.4f} TON")
        rub = await ton_to_rub(amount_ton)
        kb = InlineKeyboardBuilder()
        kb.button(text=f"✅ Конвертировать {amount_ton} TON → {rub} ₽",
                  callback_data=f"do_convert:ton_to_rub:{amount_ton}:{rub}")
        kb.button(text="❌ Отмена", callback_data="convert_menu")
        kb.adjust(1)
        await msg.answer(
            f"🔄 <b>Подтверди конвертацию</b>\n\n"
            f"💎 {amount_ton} TON  →  💰 {rub} ₽",
            reply_markup=kb.as_markup(), parse_mode="HTML"
        )
    await state.clear()

@dp.callback_query(F.data.startswith("do_convert:"))
async def cb_do_convert(cb: types.CallbackQuery):
    parts     = cb.data.split(":")
    direction = parts[1]
    uid       = cb.from_user.id

    if direction == "rub_to_ton":
        amount_rub = float(parts[2])
        ton        = float(parts[3])
        if not deduct_balance(uid, amount_rub):
            return await cb.answer("❌ Недостаточно рублей!", show_alert=True)
        add_balance_ton(uid, ton)
        await cb.message.edit_text(
            f"✅ <b>Конвертировано!</b>\n"
            f"💰 -{amount_rub} ₽  →  💎 +{ton} TON\n\n"
            f"Рублёвый баланс: {get_balance(uid):.2f} ₽\n"
            f"TON-баланс: {get_balance_ton(uid):.4f} TON",
            parse_mode="HTML"
        )
    else:
        amount_ton = float(parts[2])
        rub        = float(parts[3])
        if not deduct_balance_ton(uid, amount_ton):
            return await cb.answer("❌ Недостаточно TON!", show_alert=True)
        add_balance(uid, rub)
        await cb.message.edit_text(
            f"✅ <b>Конвертировано!</b>\n"
            f"💎 -{amount_ton} TON  →  💰 +{rub} ₽\n\n"
            f"Рублёвый баланс: {get_balance(uid):.2f} ₽\n"
            f"TON-баланс: {get_balance_ton(uid):.4f} TON",
            parse_mode="HTML"
        )
    await cb.answer("✅")

# ════════════════════════════════════════════════════════
# ПОКУПКА ЗВЁЗД
# ════════════════════════════════════════════════════════
@dp.callback_query(F.data == "buy_stars_menu")
async def cb_buy_stars_menu(cb: types.CallbackQuery, state: FSMContext):
    uid = cb.from_user.id
    bal_r = get_balance(uid)
    bal_t = get_balance_ton(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Себе",   callback_data="bst_self")
    kb.button(text="🎁 Другу",  callback_data="bst_friend")
    kb.button(text="🔙 Назад",  callback_data="menu")
    kb.adjust(2, 1)
    await cb.message.answer(
        f"⭐ <b>Купить звёзды</b>\n\n"
        f"💰 Рублёвый баланс: <b>{bal_r:.2f} ₽</b>\n"
        f"💎 TON-баланс: <b>{bal_t:.4f} TON</b>\n"
        f"💫 Цена: <b>{RUB_PER_STAR} ₽ / ⭐</b>\n\n"
        f"Кому отправить звёзды?",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await cb.answer()

def amounts_kb(tag: str):
    kb = InlineKeyboardBuilder()
    for a in [50, 100, 200, 500, 1000]:
        rub = int(a * RUB_PER_STAR)
        kb.button(text=f"⭐{a} — {rub}₽", callback_data=f"sa|{tag}|{a}")
    kb.button(text="✏️ Своя сумма", callback_data=f"sa_custom|{tag}")
    kb.button(text="🔙 Назад",      callback_data="buy_stars_menu")
    kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()

@dp.callback_query(F.data == "bst_self")
async def cb_bst_self(cb: types.CallbackQuery, state: FSMContext):
    if not cb.from_user.username:
        return await cb.answer("❌ Нужен @username в Telegram!", show_alert=True)
    await state.update_data(target=cb.from_user.username, pay_from="direct")
    await cb.message.answer("⭐ Выбери количество:", reply_markup=amounts_kb("self"))
    await cb.answer()

@dp.callback_query(F.data == "bst_friend")
async def cb_bst_friend(cb: types.CallbackQuery, state: FSMContext):
    await state.update_data(pay_from="direct")
    await cb.message.answer("👤 Введи @username друга (без @):")
    await state.set_state(BuyFlow.waiting_friend)
    await cb.answer()

@dp.message(BuyFlow.waiting_friend)
async def msg_friend(msg: types.Message, state: FSMContext):
    t = msg.text.replace("@", "").strip()
    if not t:
        return await msg.answer("❌ Введи username!")
    await state.update_data(target=t)
    await msg.answer(f"✅ Для @{t}. Выбери количество:", reply_markup=amounts_kb("friend"))
    await state.set_state(None)

@dp.callback_query(F.data.startswith("sa|"))
async def cb_sa(cb: types.CallbackQuery, state: FSMContext):
    _, tag, a = cb.data.split("|")
    amount = int(a)
    d = await state.get_data()
    await state.update_data(amount=amount)
    await show_buy_payment(cb.message, state, amount, d.get("target", cb.from_user.username or ""), d.get("pay_from", "direct"))
    await cb.answer()

@dp.callback_query(F.data.startswith("sa_custom|"))
async def cb_sa_custom(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("✏️ Введи количество звёзд (50–10000):")
    await state.set_state(BuyFlow.waiting_amount)
    await cb.answer()

@dp.message(BuyFlow.waiting_amount)
async def msg_buy_amount(msg: types.Message, state: FSMContext):
    if not msg.text.strip().isdigit():
        return await msg.answer("❌ Только цифры!")
    a = int(msg.text.strip())
    if a < 50 or a > 10000:
        return await msg.answer("❌ От 50 до 10000!")
    d = await state.get_data()
    await state.update_data(amount=a)
    await show_buy_payment(msg, state, a, d.get("target", ""), d.get("pay_from", "direct"))

async def show_buy_payment(message, state, amount: int, target: str, pay_from: str):
    rub   = int(amount * RUB_PER_STAR)
    uid   = message.chat.id if hasattr(message, "chat") else message.from_user.id
    ton   = await rub_to_ton(rub)
    bal_r = get_balance(uid)
    bal_t = get_balance_ton(uid)
    await state.update_data(amount=amount, target=target, rub=rub, ton=ton)

    kb = InlineKeyboardBuilder()
    if pay_from == "balance_rub":
        if bal_r >= rub:
            kb.button(text=f"💰 Списать {rub} ₽ с баланса", callback_data="pay_bal_rub_stars")
        else:
            kb.button(text=f"❌ Мало ₽ ({bal_r:.0f}/{rub})", callback_data="_")
    elif pay_from == "balance_ton":
        if bal_t >= ton:
            kb.button(text=f"💎 Списать {ton} TON с баланса", callback_data="pay_bal_ton_stars")
        else:
            kb.button(text=f"❌ Мало TON ({bal_t:.4f}/{ton})", callback_data="_")
    else:
        kb.button(text=f"💳 Картой — {rub} ₽",      callback_data="pay_card_stars")
        kb.button(text=f"💎 Tonkeeper — {ton} TON",  callback_data="pay_ton_stars")
        kb.button(text=f"💰 С рублёвого баланса",    callback_data="pay_bal_rub_stars")
        kb.button(text=f"💎 С TON-баланса",          callback_data="pay_bal_ton_stars")
    kb.button(text="🔙 Назад", callback_data="buy_stars_menu")
    kb.adjust(1)

    await message.answer(
        f"📦 <b>{amount} ⭐</b> для @{target}\n"
        f"💰 {rub} ₽  ·  💎 {ton} TON",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )

@dp.callback_query(F.data == "pay_card_stars")
async def cb_pay_card_stars(cb: types.CallbackQuery, state: FSMContext):
    d   = await state.get_data()
    uid = cb.from_user.id
    await bot.send_invoice(
        cb.message.chat.id,
        title=f"{d.get('amount','?')} ⭐ Stars",
        description=f"Покупка {d.get('amount','?')} звёзд для @{d.get('target','')}",
        provider_token=YOO_KASSA_TOKEN,
        currency="rub",
        prices=[types.LabeledPrice(label="Stars", amount=d['rub'] * 100)],
        payload=f"stars:{d['amount']}:{uid}:{d.get('target','')}"
    )
    await cb.answer()

@dp.callback_query(F.data == "pay_ton_stars")
async def cb_pay_ton_stars(cb: types.CallbackQuery, state: FSMContext):
    d   = await state.get_data()
    uid = cb.from_user.id
    wallet = get_ton_wallet(uid)
    if not wallet:
        return await cb.answer("❌ Привяжи TON кошелёк в магазине (вкладка Профиль)!", show_alert=True)
    nanos = int(d['ton'] * 1e9)
    turl  = f"ton://transfer/{ADMIN_WALLET_TON}?amount={nanos}"
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Открыть Tonkeeper", url=turl)
    kb.button(text="✅ Я перевёл",         callback_data="check_ton_stars")
    kb.adjust(1)
    await cb.message.answer(
        f"👛 <b>Оплата TON</b>\n\n"
        f"⭐ {d.get('amount','?')} для @{d.get('target','')}\n"
        f"💎 {d['ton']} TON",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "check_ton_stars")
async def cb_check_ton_stars(cb: types.CallbackQuery, state: FSMContext):
    d     = await state.get_data()
    uid   = cb.from_user.id
    uname = cb.from_user.username or str(uid)
    await notify_admins(
        f"💎 <b>ЗАКАЗ ЗВЁЗД — TONKEEPER</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Покупатель: @{uname} (ID:{uid})\n"
        f"🎯 Получатель: @{d.get('target', uname)}\n"
        f"⭐ Количество: {d.get('amount','?')}\n"
        f"💎 Сумма: {d.get('ton','?')} TON\n\n"
        f"📌 Проверь кошелёк и начисли через Fragment!"
    )
    await cb.message.answer("📨 Заявка отправлена! Администратор проверит и начислит звёзды.")
    await notify_ref(uid, d.get("amount", 0))
    await state.clear()
    await cb.answer()

@dp.callback_query(F.data == "pay_bal_rub_stars")
async def cb_pay_bal_rub_stars(cb: types.CallbackQuery, state: FSMContext):
    d   = await state.get_data()
    uid = cb.from_user.id
    rub = d.get("rub", 0)
    if not deduct_balance(uid, rub):
        return await cb.answer(f"❌ Недостаточно на рублёвом балансе! Нужно {rub} ₽", show_alert=True)
    uname = cb.from_user.username or str(uid)
    await notify_admins(
        f"💳 <b>ЗАКАЗ ЗВЁЗД — БАЛАНС ₽</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Покупатель: @{uname} (ID:{uid})\n"
        f"🎯 Получатель: @{d.get('target', uname)}\n"
        f"⭐ Количество: {d.get('amount','?')}\n"
        f"💵 Списано: {rub} ₽\n\n"
        f"📌 Начисли через Fragment!"
    )
    await cb.message.answer(f"✅ <b>{d.get('amount','?')} ⭐</b> придут @{d.get('target','')} в ближайшее время!", parse_mode="HTML")
    await notify_ref(uid, d.get("amount", 0))
    await state.clear()
    await cb.answer()

@dp.callback_query(F.data == "pay_bal_ton_stars")
async def cb_pay_bal_ton_stars(cb: types.CallbackQuery, state: FSMContext):
    d   = await state.get_data()
    uid = cb.from_user.id
    ton = d.get("ton", 0)
    if not deduct_balance_ton(uid, ton):
        return await cb.answer(f"❌ Недостаточно на TON-балансе! Нужно {ton} TON", show_alert=True)
    uname = cb.from_user.username or str(uid)
    await notify_admins(
        f"💎 <b>ЗАКАЗ ЗВЁЗД — БАЛАНС TON</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Покупатель: @{uname} (ID:{uid})\n"
        f"🎯 Получатель: @{d.get('target', uname)}\n"
        f"⭐ Количество: {d.get('amount','?')}\n"
        f"💎 Списано: {ton} TON\n\n"
        f"📌 Начисли через Fragment!"
    )
    await cb.message.answer(f"✅ <b>{d.get('amount','?')} ⭐</b> придут @{d.get('target','')} в ближайшее время!", parse_mode="HTML")
    await notify_ref(uid, d.get("amount", 0))
    await state.clear()
    await cb.answer()

# ════════════════════════════════════════════════════════
# ПРЕЧЕКАУТ + УСПЕШНАЯ ОПЛАТА (ЮKassa)
# ════════════════════════════════════════════════════════
@dp.pre_checkout_query()
async def pre_checkout(q: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)

@dp.message(F.successful_payment)
async def on_paid(msg: types.Message, state: FSMContext):
    payload = msg.successful_payment.invoice_payload
    parts   = payload.split(":")

    if parts[0] == "deposit_rub":
        amount = int(parts[1])
        uid    = int(parts[2])
        add_balance(uid, amount)
        await msg.answer(
            f"✅ <b>Рублёвый баланс пополнен на {amount} ₽!</b>\n"
            f"Текущий: <b>{get_balance(uid):.2f} ₽</b>",
            parse_mode="HTML"
        )
        return

    if parts[0] == "stars":
        stars  = int(parts[1])
        uid    = int(parts[2])
        target = parts[3] if len(parts) > 3 else msg.from_user.username or "?"
        uname  = msg.from_user.username or str(uid)
        await notify_admins(
            f"💳 <b>ЗАКАЗ ЗВЁЗД — КАРТА</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"👤 Покупатель: @{uname} (ID:{uid})\n"
            f"🎯 Получатель: @{target}\n"
            f"⭐ Количество: {stars}\n"
            f"💵 Оплачено: {int(stars * RUB_PER_STAR)} ₽\n\n"
            f"📌 Начисли через Fragment!"
        )
        await msg.answer(f"✅ <b>{stars} ⭐</b> придут @{target} в течение 15 минут.", parse_mode="HTML")
        await notify_ref(uid, stars)
        await state.clear()
        return

    if parts[0] == "nft":
        lid = int(parts[1])
        uid = int(parts[2])
        await _nft_bought(msg, lid, uid)
        return

    if parts[0] == "withdraw_nft":
        vid = int(parts[1])
        uid = int(parts[2])
        await _nft_withdraw_paid(msg, vid, uid)
        return

# ════════════════════════════════════════════════════════
# MINI APP — обработчик (и через sendData, и через deeplink)
# ════════════════════════════════════════════════════════
# Карта восстановления сжатых ключей
_KEY_EXPAND = {'a':'action','n':'amount','t':'target','p':'payment','l':'listing_id',
               'v':'vault_id','pr':'price','d':'direction','w':'wallet'}

_PAY_EXPAND = {'c':'card','r':'balance_rub','b':'balance_ton','k':'tonkeeper'}

def _expand_payload(data: dict) -> dict:
    out = {}
    for k, v in data.items():
        key = _KEY_EXPAND.get(k, k)
        # Разворачиваем сокращённые коды payment
        if key == 'payment':
            v = _PAY_EXPAND.get(v, v)
        out[key] = v
    return out

async def _handle_miniapp_action(msg: types.Message, state: FSMContext, data: dict, uid: int, uname: str):
    data   = _expand_payload(data)
    action = data.get("action", "")
    logging.info(f"MiniApp uid={uid} action={action!r}")
    try:

        # ── Привязать кошелёк ──
        if action == "set_wallet":
            wallet = str(data.get("wallet", "")).strip()
            if wallet == "1":
                # Кошелёк слишком длинный для deeplink — просим прислать вручную
                await msg.answer(
                    "👛 <b>Привязка TON кошелька</b>\n\n"
                    "Отправь адрес кошелька следующим сообщением\n"
                    "<i>(начинается с UQ... или EQ...)</i>",
                    parse_mode="HTML"
                )
                await state.set_state("waiting_wallet")
                await state.update_data(action="set_wallet")
            elif wallet == "":
                set_ton_wallet(uid, "")
                await msg.answer("🗑 Кошелёк удалён.")
            else:
                set_ton_wallet(uid, wallet)
                await msg.answer(f"✅ <b>TON кошелёк привязан!</b>\n<code>{wallet}</code>", parse_mode="HTML")
            return

        # ── Открыть пополнение баланса ──
        if action == "open_deposit":
            method = data.get("method", "card")
            if method == "ton" and not require_wallet(uid):
                return await msg.answer("❌ Сначала привяжи TON кошелёк!")
            await msg.answer(
                "💳 Введи сумму в рублях (мин. 100 ₽):"
                if method == "card" else
                "💎 Введи сумму TON (мин. 0.5 TON):"
            )
            await state.set_state(DepositFlow.waiting_amount)
            await state.update_data(method=method)
            return

        # ── Конвертация через Mini App ──
        if action == "convert":
            direction = data.get("direction", "rub_to_ton")
            amount    = float(data.get("amount", 0))
            if direction == "rub_to_ton":
                ton = await rub_to_ton(amount)
                if not deduct_balance(uid, amount):
                    return await msg.answer(f"❌ Недостаточно ₽! Нужно {amount}")
                add_balance_ton(uid, ton)
                await msg.answer(
                    f"✅ Конвертировано: {amount} ₽ → {ton} TON\n"
                    f"TON-баланс: {get_balance_ton(uid):.4f} TON",
                    parse_mode="HTML"
                )
            else:
                rub = await ton_to_rub(amount)
                if not deduct_balance_ton(uid, amount):
                    return await msg.answer(f"❌ Недостаточно TON! Нужно {amount}")
                add_balance(uid, rub)
                await msg.answer(
                    f"✅ Конвертировано: {amount} TON → {rub} ₽\n"
                    f"Рублёвый баланс: {get_balance(uid):.2f} ₽",
                    parse_mode="HTML"
                )
            return

        # Только NFT-продажа и вывод NFT требуют кошелька безусловно
        needs_wallet = action in ("withdraw_nft", "list_nft", "delist_nft")  # nft_sell_notify не требует кошелька
        # Покупка через Tonkeeper/TON-баланс тоже требует кошелька
        if action in ("buy", "buy_nft"):
            payment = data.get("payment", "card")
            needs_wallet = payment in ("tonkeeper", "balance_ton")

        if needs_wallet and not require_wallet(uid):
            return await msg.answer(
                "❌ <b>Привяжи TON кошелёк</b> для этого действия.\n"
                "Открой магазин → Профиль → TON кошелёк",
                parse_mode="HTML"
            )

        # ── Купить звёзды ──
        if action == "buy":
            amount  = int(data.get("amount", 0))
            target  = str(data.get("target") or uname).strip().lstrip("@") or uname
            payment = data.get("payment", "card")
            logging.info(f"buy: amount={amount} target={target!r} payment={payment!r} raw_data={data}")
            if amount < 50:
                return await msg.answer("❌ Минимум 50 звёзд!")
            rub = int(amount * RUB_PER_STAR)
            ton = await rub_to_ton(rub)
            await state.update_data(amount=amount, target=target, rub=rub, ton=ton)

            if payment == "card":
                await bot.send_invoice(
                    msg.chat.id,
                    title=f"{amount} ⭐ Stars",
                    description=f"Покупка {amount} звёзд для @{target}",
                    provider_token=YOO_KASSA_TOKEN,
                    currency="rub",
                    prices=[types.LabeledPrice(label="Stars", amount=rub * 100)],
                    payload=f"stars:{amount}:{uid}:{target}"
                )
            elif payment == "balance_rub":
                if not deduct_balance(uid, rub):
                    return await msg.answer(f"❌ Недостаточно ₽! Нужно {rub} ₽")
                await notify_admins(
                    f"💳 <b>ЗАКАЗ ЗВЁЗД — БАЛАНС ₽</b>\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"👤 Покупатель: @{uname} (ID:{uid})\n"
                    f"🎯 Получатель: @{target}\n"
                    f"⭐ Количество: {amount}\n"
                    f"💵 Списано: {rub} ₽\n\n"
                    f"📌 Начисли через Fragment!"
                )
                await msg.answer(f"✅ <b>{amount} ⭐</b> придут @{target} в ближайшее время!", parse_mode="HTML")
                await notify_ref(uid, amount)
            elif payment == "balance_ton":
                if not deduct_balance_ton(uid, ton):
                    return await msg.answer(f"❌ Недостаточно TON! Нужно {ton} TON")
                await notify_admins(
                    f"💎 <b>ЗАКАЗ ЗВЁЗД — БАЛАНС TON</b>\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"👤 Покупатель: @{uname} (ID:{uid})\n"
                    f"🎯 Получатель: @{target}\n"
                    f"⭐ Количество: {amount}\n"
                    f"💎 Списано: {ton} TON\n\n"
                    f"📌 Начисли через Fragment!"
                )
                await msg.answer(f"✅ <b>{amount} ⭐</b> придут @{target} в ближайшее время!", parse_mode="HTML")
                await notify_ref(uid, amount)
            elif payment == "tonkeeper":
                nanos = int(ton * 1e9)
                turl  = f"ton://transfer/{ADMIN_WALLET_TON}?amount={nanos}"
                kb = InlineKeyboardBuilder()
                kb.button(text="📲 Открыть Tonkeeper", url=turl)
                kb.button(text="✅ Я перевёл",         callback_data="check_ton_stars")
                kb.adjust(1)
                await msg.answer(
                    f"👛 <b>Оплата TON</b>\n⭐ {amount} для @{target}  ·  💎 {ton} TON",
                    reply_markup=kb.as_markup(), parse_mode="HTML"
                )

        # ── Купить NFT ──
        elif action == "buy_nft":
            lid     = int(data.get("listing_id", 0))
            payment = data.get("payment", "card")
            listing = get_listing(lid)
            if not listing or listing[4] != "active":
                return await msg.answer("❌ NFT уже недоступен!")
            if listing[2] == uid:
                return await msg.answer("❌ Нельзя купить свой NFT!")
            price = listing[3]
            ton   = await rub_to_ton(price)
            await state.update_data(nft_lid=lid, rub=price, ton=ton)

            if payment == "card":
                await bot.send_invoice(
                    msg.chat.id,
                    title="NFT из маркета",
                    description="Покупка NFT — после оплаты появится в хранилище",
                    provider_token=YOO_KASSA_TOKEN,
                    currency="rub",
                    prices=[types.LabeledPrice(label="NFT", amount=price * 100)],
                    payload=f"nft:{lid}:{uid}"
                )
            elif payment == "balance_rub":
                if not deduct_balance(uid, price):
                    return await msg.answer(f"❌ Недостаточно ₽! Нужно {price} ₽")
                await _nft_bought(msg, lid, uid)
            elif payment == "balance_ton":
                if not deduct_balance_ton(uid, ton):
                    return await msg.answer(f"❌ Недостаточно TON! Нужно {ton} TON")
                await _nft_bought(msg, lid, uid)
            elif payment == "tonkeeper":
                nanos = int(ton * 1e9)
                turl  = f"ton://transfer/{ADMIN_WALLET_TON}?amount={nanos}"
                kb = InlineKeyboardBuilder()
                kb.button(text=f"📲 Перевести {ton} TON", url=turl)
                kb.button(text="✅ Я перевёл",             callback_data=f"nft_ton_check:{lid}")
                kb.adjust(1)
                await msg.answer(
                    f"👛 <b>Оплата NFT через Tonkeeper</b>\n\n"
                    f"💰 {price} ₽  ·  💎 {ton} TON",
                    reply_markup=kb.as_markup(), parse_mode="HTML"
                )

        # ── Выставить NFT на продажу ──
        elif action == "list_nft":
            vid   = int(data.get("vault_id", 0))
            price = int(data.get("price", 0))
            if price < 100:
                return await msg.answer("❌ Минимальная цена 100 ₽!")
            item = get_vault_item(vid)
            if not item or item[1] != uid:
                return await msg.answer("❌ NFT не найден!")
            if item[7] != "stored":
                return await msg.answer("❌ NFT уже выставлен или снят!")
            add_listing(vid, uid, uname, price)
            update_vault_status(vid, "listed")
            comm   = round(price * COMMISSION)
            payout = price - comm
            await msg.answer(
                f"✅ <b>NFT выставлен!</b>\n\n"
                f"{item[4]} {item[2]} {item[3]}\n"
                f"💰 {price} ₽  →  получишь <b>{payout} ₽</b> (−{comm} ₽ комиссия)",
                parse_mode="HTML"
            )

        # ── Снять с продажи ──
        elif action == "delist_nft":
            lid = int(data.get("listing_id", 0))
            lst = get_listing(lid)
            if not lst or lst[2] != uid:
                return await msg.answer("❌ Не найден!")
            with _db() as c:
                c.execute("UPDATE listings SET status='cancelled' WHERE id=?", (lid,))
                c.execute("UPDATE vault SET status='stored' WHERE id=?", (lst[1],))
            await msg.answer("✅ NFT снят с продажи.")

        # ── Вывести NFT в профиль ──
        elif action == "withdraw_nft":
            vid     = int(data.get("vault_id", 0))
            payment = data.get("payment", "card")
            item    = get_vault_item(vid)
            if not item or item[1] != uid:
                return await msg.answer("❌ NFT не найден!")
            if item[7] != "stored":
                return await msg.answer("❌ NFT недоступен для вывода!")
            ton = await rub_to_ton(WITHDRAW_RUB)
            await state.update_data(withdraw_vid=vid, rub=WITHDRAW_RUB, ton=ton)

            if payment == "card":
                await bot.send_invoice(
                    msg.chat.id,
                    title="Вывод NFT в профиль",
                    description=f"Перевод {item[2]} {item[3]} в твой профиль",
                    provider_token=YOO_KASSA_TOKEN,
                    currency="rub",
                    prices=[types.LabeledPrice(label="Вывод NFT", amount=WITHDRAW_RUB * 100)],
                    payload=f"withdraw_nft:{vid}:{uid}"
                )
            elif payment == "balance_rub":
                if not deduct_balance(uid, WITHDRAW_RUB):
                    return await msg.answer(f"❌ Недостаточно ₽! Нужно {WITHDRAW_RUB} ₽")
                await _nft_withdraw_paid(msg, vid, uid)
            elif payment == "balance_ton":
                if not deduct_balance_ton(uid, ton):
                    return await msg.answer(f"❌ Недостаточно TON! Нужно {ton} TON")
                await _nft_withdraw_paid(msg, vid, uid)
            elif payment == "tonkeeper":
                nanos = int(ton * 1e9)
                turl  = f"ton://transfer/{ADMIN_WALLET_TON}?amount={nanos}"
                kb = InlineKeyboardBuilder()
                kb.button(text=f"📲 Перевести {ton} TON", url=turl)
                kb.button(text="✅ Я перевёл",             callback_data=f"withdraw_ton_check:{vid}")
                kb.adjust(1)
                await msg.answer(
                    f"👛 <b>Вывод NFT через Tonkeeper</b>\n\n"
                    f"{item[4]} <b>{item[2]} {item[3]}</b>\n"
                    f"💎 {ton} TON",
                    reply_markup=kb.as_markup(), parse_mode="HTML"
                )

        # ── Продажа NFT (пользователь нажал "Я передал") ──
        elif action == "nft_sell_notify":
            add_nft_pending(uid, uname)
            ka = InlineKeyboardBuilder()
            ka.button(text="✅ Добавить в хранилище", callback_data=f"admin_nft_add:{uid}")
            ka.button(text="❌ Не получили",          callback_data=f"admin_nft_reject:{uid}")
            ka.adjust(1)
            await notify_admins(
                f"📦 <b>NFT СДАН НА ХРАНЕНИЕ</b>\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"👤 Продавец: @{uname} (ID:{uid})\n\n"
                f"Проверь @{SHOP_BANK} — должен прийти подарок.\n"
                f"{'⚡ Userbot активен — добавится автоматически.' if USERBOT_ENABLED else '⚠ Userbot выключен — добавь вручную.'}",
                kb=ka
            )
            await msg.answer(
                f"✅ <b>Уведомление отправлено!</b>\n\n"
                f"Администратор проверит <b>@{SHOP_BANK}</b> и добавит NFT в хранилище.\n"
                f"Обычно до 30 минут.",
                parse_mode="HTML"
            )

        # ── Ежедневный бонус ──
        elif action == "daily_bonus":
            if not uname or uname == str(uid):
                return await msg.answer("❌ Нужен @username!")
            today = str(date.today())
            if get_last_bonus(uid) == today:
                return await msg.answer("⏳ Бонус уже получен сегодня!")
            bot_me = await bot.get_me()
            link   = f"https://t.me/{bot_me.username}?start={uid}"
            set_last_bonus(uid, today)
            ka = InlineKeyboardBuilder()
            ka.button(text="✅ +1 ⭐",    callback_data=f"bonus_ok:{uid}")
            ka.button(text="❌ Отклонить", callback_data=f"bonus_no:{uid}")
            ka.adjust(2)
            await notify_admins(
            f"🎁 <b>ЗАПРОС БОНУСА +1 ⭐</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"👤 @{uname} (ID:{uid})\n"
            f"🔗 <code>{link}</code>\n\n"
            f"Проверь bio и подтверди!",
            kb=ka
        )
            await msg.answer(f"📨 Заявка! Убедись что в bio: <code>{link}</code>", parse_mode="HTML")

        # ── Вывод реф. звёзд ──
        elif action == "ref_withdraw":
            ref_stars = get_ref_stars(uid)
            if ref_stars < MIN_WITHDRAW:
                return await msg.answer(f"⏳ Нужно ещё <b>{MIN_WITHDRAW - ref_stars:.2f} ⭐</b>", parse_mode="HTML")
            await notify_admins(
            f"💸 <b>ВЫВОД РЕФЕРАЛЬНЫХ ЗВЁЗД</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"👤 @{uname} (ID:{uid})\n"
            f"⭐ Сумма: {ref_stars:.2f} ⭐\n\n"
            f"📌 Начисли через Fragment!"
        )
            reset_ref_stars(uid)
            await msg.answer(f"✅ Заявка на <b>{ref_stars:.2f} ⭐</b> отправлена!", parse_mode="HTML")

        else:
            logging.warning(f"Unknown MiniApp action: {action!r}")

    except json.JSONDecodeError:
        await msg.answer("❌ Ошибка данных от приложения")
    except Exception as e:
        logging.error(f"MiniApp error: {e}", exc_info=True)
        await msg.answer(f"❌ Ошибка: {e}")


@dp.message(F.web_app_data)
async def on_miniapp(msg: types.Message, state: FSMContext):
    logging.info(f"📲 web_app_data получен: {msg.web_app_data.data[:200]}")
    try:
        data  = json.loads(msg.web_app_data.data)
        uid   = msg.from_user.id
        uname = msg.from_user.username or str(uid)
        await _handle_miniapp_action(msg, state, data, uid, uname)
    except Exception as e:
        logging.error(f"on_miniapp error: {e}", exc_info=True)

# ════════════════════════════════════════════════════════
# ВВОД КОШЕЛЬКА (после deeplink set_wallet)
# ════════════════════════════════════════════════════════
@dp.message(F.text, ~F.from_user.id.in_(set(ADMIN_IDS)))
async def handle_wallet_input(msg: types.Message, state: FSMContext):
    current = await state.get_state()
    if current != "waiting_wallet":
        return
    w = msg.text.strip()
    if not (w.startswith("UQ") or w.startswith("EQ")) or len(w) < 40:
        return await msg.answer("❌ Неверный адрес. Должен начинаться с UQ... или EQ... и быть длиннее 40 символов.")
    uid = msg.from_user.id
    set_ton_wallet(uid, w)
    await state.clear()
    await msg.answer(f"✅ <b>TON кошелёк привязан!</b>\n<code>{w}</code>\n\nВернись в магазин.", parse_mode="HTML")


# ════════════════════════════════════════════════════════
# ПРОДАЖА NFT — /sell
# ════════════════════════════════════════════════════════
@dp.message(Command("sell"))
async def cmd_sell(msg: types.Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)
    if not require_wallet(uid):
        return await msg.answer(
            "❌ <b>Привяжи TON кошелёк</b> прежде чем продавать!\n"
            "Открой магазин → Профиль → TON кошелёк",
            parse_mode="HTML"
        )
    kb = InlineKeyboardBuilder()
    kb.button(text=f"✅ Я передал NFT @{SHOP_BANK}", callback_data="nft_sent_confirm")
    kb.button(text="❌ Отмена",                        callback_data="menu")
    kb.adjust(1)
    await msg.answer(
        f"💼 <b>Продажа NFT</b>\n\n"
        f"<b>1.</b> Telegram → Профиль → Подарки\n"
        f"<b>2.</b> Выбери NFT → «Передать» → найди <b>@{SHOP_BANK}</b>\n"
        f"<b>3.</b> Нажми кнопку ниже\n\n"
        f"После проверки NFT появится в хранилище.\n"
        f"<i>Комиссия платформы: 3%</i>",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )

@dp.callback_query(F.data == "nft_sent_confirm")
async def cb_nft_sent_confirm(cb: types.CallbackQuery):
    uid   = cb.from_user.id
    uname = cb.from_user.username or str(uid)
    add_nft_pending(uid, uname)
    ka = InlineKeyboardBuilder()
    ka.button(text="✅ Добавить в хранилище", callback_data=f"admin_nft_add:{uid}")
    ka.button(text="❌ Не получили",          callback_data=f"admin_nft_reject:{uid}")
    ka.adjust(1)
    await notify_admins(
        f"📦 <b>NFT СДАН НА ХРАНЕНИЕ</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 Продавец: @{uname} (ID:{uid})\n\n"
        f"Проверь @{SHOP_BANK} — должен прийти подарок.\n"
        f"{'⚡ Userbot активен — добавится автоматически.' if USERBOT_ENABLED else '⚠ Userbot выключен — добавь вручную.'}",
        kb=ka
    )
    await cb.message.edit_text(
        f"✅ <b>Уведомление отправлено!</b>\n\n"
        f"Проверим @{SHOP_BANK} и добавим NFT в хранилище.\n"
        f"Обычно до 30 минут.",
        parse_mode="HTML"
    )
    await cb.answer()

# ════════════════════════════════════════════════════════
# ADMIN — добавление NFT вручную
# ════════════════════════════════════════════════════════
@dp.callback_query(F.data.startswith("admin_nft_add:"))
async def cb_admin_nft_add(cb: types.CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    await state.update_data(nft_target_uid=uid)
    await cb.message.answer(
        f"🔗 Отправь ссылку на NFT для пользователя <b>{uid}</b>\n\n"
        f"Формат: <code>https://t.me/nft/SnakeBox-88712</code>\n\n"
        f"Бот автоматически получит название, картинку и редкость.",
        parse_mode="HTML"
    )
    await state.set_state("admin_nft_input")
    await cb.answer()

@dp.message(F.text, F.from_user.id.in_(set(ADMIN_IDS)))
async def admin_nft_input(msg: types.Message, state: FSMContext):
    current = await state.get_state()
    # Обработка ввода кошелька для админов
    if current == "waiting_wallet":
        w = msg.text.strip()
        if not (w.startswith("UQ") or w.startswith("EQ")) or len(w) < 40:
            return await msg.answer("❌ Неверный адрес. Должен начинаться с UQ... или EQ...")
        set_ton_wallet(msg.from_user.id, w)
        await state.clear()
        return await msg.answer(f"✅ <b>TON кошелёк привязан!</b>\n<code>{w}</code>\n\nВернись в магазин.", parse_mode="HTML")
    if current != "admin_nft_input":
        return
    d   = await state.get_data()
    uid = d.get("nft_target_uid")
    if not uid:
        return

    link = msg.text.strip()
    wait = await msg.answer("⏳ Получаю данные NFT...")

    nft_data = await parse_nft_link(link)
    if not nft_data:
        await wait.delete()
        return await msg.answer(
            "❌ Не удалось получить данные.\n"
            "Проверь ссылку: <code>https://t.me/nft/SnakeBox-88712</code>",
            parse_mode="HTML"
        )

    nft_name  = nft_data["nft_name"]
    nft_num   = nft_data["nft_num"]
    emoji     = nft_data["emoji"]
    rarity    = nft_data["rarity"]
    image_url = nft_data["image_url"]
    slug      = nft_data["slug"]
    model     = nft_data.get("model", "")

    vid = add_to_vault(uid, nft_name, nft_num, emoji, rarity,
                       source="sold_in", image_url=image_url, slug=slug)

    await wait.delete()
    # Уведомляем пользователя
    try:
        txt = (
            f"✅ <b>NFT добавлен в хранилище!</b>\n\n"
            f"{emoji} <b>{nft_name} {nft_num}</b>\n"
            f"Редкость: {rarity}\n"
            + (f"Модель: {model}\n" if model else "")
            + f"\nОткрой магазин → Хранилище"
        )
        await bot.send_message(uid, txt, parse_mode="HTML")
    except Exception as e:
        logging.error(f"notify user nft added: {e}")
    # Подтверждение админу
    await msg.answer(
        f"✅ <b>NFT добавлен!</b>\n"
        f"{emoji} {nft_name} {nft_num}\n"
        f"Редкость: {rarity}\n"
        f"Картинка: {'✅' if image_url else '❌ не найдена'}",
        parse_mode="HTML"
    )
    await state.clear()


@dp.callback_query(F.data.startswith("admin_nft_reject:"))
async def cb_admin_nft_reject(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    try:
        await bot.send_message(
            uid,
            f"❌ <b>NFT не найден</b>\n\n"
            f"Проверь что правильно передал @{SHOP_BANK}.\n"
            f"Вопросы: @{SUPPORT_USER}",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено", parse_mode="HTML")
    await cb.answer("❌")

# ════════════════════════════════════════════════════════
# ПОКУПКА NFT — после оплаты
# ════════════════════════════════════════════════════════
async def _nft_bought(msg_or_ctx, lid: int, buyer_id: int):
    listing = get_listing(lid)
    if not listing:
        return
    buyer_uname = str(buyer_id)
    chat_id     = buyer_id
    if hasattr(msg_or_ctx, "from_user"):
        buyer_uname = msg_or_ctx.from_user.username or str(buyer_id)
        chat_id     = msg_or_ctx.chat.id

    item      = get_vault_item(listing[1])
    seller_id = listing[2]
    price     = listing[3]
    comm      = round(price * COMMISSION)
    payout    = price - comm

    mark_listing_sold(lid, buyer_id, buyer_uname)
    update_vault_status(listing[1], "sold_out")

    nft_name = item[2] if item else "NFT"
    nft_num  = item[3] if item else "#????"
    emoji    = item[4] if item else "🎁"
    rarity   = item[5] if item else "common"
    add_to_vault(buyer_id, nft_name, nft_num, emoji, rarity, source="bought")

    try:
        await bot.send_message(
            chat_id,
            f"🎉 <b>NFT у тебя в хранилище!</b>\n\n"
            f"{emoji} <b>{nft_name} {nft_num}</b>\n"
            f"Редкость: {rarity}\n\n"
            f"Открой «Хранилище» → нажми «Вывести в профиль».\n"
            f"<i>Вывод: {WITHDRAW_RUB} ₽</i>",
            parse_mode="HTML"
        )
    except Exception:
        pass

    seller_wallet = get_ton_wallet(seller_id)
    try:
        await bot.send_message(
            seller_id,
            f"💰 <b>Твой NFT продан!</b>\n\n"
            f"{emoji} {nft_name} {nft_num}\n"
            f"💵 {price} ₽  →  тебе <b>{payout} ₽</b> (−{comm} ₽)\n\n"
            f"Деньги придут на TON кошелёк:\n<code>{seller_wallet}</code>",
            parse_mode="HTML"
        )
    except Exception:
        pass

    ka = InlineKeyboardBuilder()
    ka.button(text="✅ Деньги продавцу отправлены", callback_data=f"nft_payout_done:{lid}:{seller_id}")
    ka.adjust(1)
    await notify_admins(
        f"💰 <b>NFT ПРОДАН #{lid}</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{emoji} {nft_name} {nft_num}\n"
        f"🛒 Покупатель: @{buyer_uname} (ID:{buyer_id})\n"
        f"👤 Продавец: ID:{seller_id}\n\n"
        f"💵 Сумма: {price} ₽\n"
        f"💸 Выплатить: <b>{payout} ₽</b>\n\n"
        f"👛 TON кошелёк продавца:\n<code>{seller_wallet}</code>",
        kb=ka
    )

@dp.callback_query(F.data.startswith("nft_ton_check:"))
async def cb_nft_ton_check(cb: types.CallbackQuery, state: FSMContext):
    lid   = int(cb.data.split(":")[1])
    uid   = cb.from_user.id
    uname = cb.from_user.username or str(uid)
    d     = await state.get_data()
    ton   = d.get("ton", "?")
    ka = InlineKeyboardBuilder()
    ka.button(text="✅ Подтвердить", callback_data=f"nft_ton_approve:{lid}:{uid}")
    ka.button(text="❌ Отклонить",  callback_data=f"nft_ton_reject:{uid}")
    ka.adjust(1)
    await notify_admins(
        f"👛 <b>NFT — TONKEEPER</b>\n@{uname} (ID:{uid})\nЛистинг #{lid}  ·  {ton} TON\nПроверь кошелёк!",
        kb=ka
    )
    await cb.message.answer("📨 Ожидай — администратор проверит перевод!")
    await cb.answer()

@dp.callback_query(F.data.startswith("nft_ton_approve:"))
async def cb_nft_ton_approve(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    parts    = cb.data.split(":")
    lid      = int(parts[1])
    buyer_id = int(parts[2])
    await _nft_bought(cb.message, lid, buyer_id)
    await cb.message.edit_text(cb.message.text + "\n\n✅ Подтверждено", parse_mode="HTML")
    await cb.answer("✅")

@dp.callback_query(F.data.startswith("nft_ton_reject:"))
async def cb_nft_ton_reject(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    try:
        await bot.send_message(uid, f"❌ Перевод не найден. @{SUPPORT_USER}")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено", parse_mode="HTML")
    await cb.answer("❌")

@dp.callback_query(F.data.startswith("nft_payout_done:"))
async def cb_nft_payout_done(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    seller_id = int(cb.data.split(":")[2])
    try:
        await bot.send_message(
            seller_id,
            f"✅ <b>Деньги отправлены!</b>\nПроверь TON кошелёк.\nВопросы: @{SUPPORT_USER}",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n✅ <b>ВЫПЛАЧЕНО</b>", parse_mode="HTML")
    await cb.answer("✅")

# ════════════════════════════════════════════════════════
# ВЫВОД NFT В ПРОФИЛЬ
# ════════════════════════════════════════════════════════
async def _nft_withdraw_paid(msg_or_ctx, vid: int, uid: int):
    item = get_vault_item(vid)
    if not item:
        return
    uname   = ""
    chat_id = uid
    if hasattr(msg_or_ctx, "from_user"):
        uname   = msg_or_ctx.from_user.username or str(uid)
        chat_id = msg_or_ctx.chat.id

    update_vault_status(vid, "withdrawn")
    try:
        await bot.send_message(
            chat_id,
            f"✅ <b>Заявка на вывод принята!</b>\n\n"
            f"{item[4]} <b>{item[2]} {item[3]}</b>\n\n"
            f"Администратор переведёт NFT в профиль в течение <b>24 часов</b>.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    ka = InlineKeyboardBuilder()
    ka.button(text="✅ NFT переведён в профиль", callback_data=f"withdraw_done:{vid}:{uid}")
    ka.adjust(1)
    await notify_admins(
        f"📤 <b>ВЫВОД NFT В ПРОФИЛЬ</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 @{uname} (ID:{uid})\n"
        f"🎁 NFT: {item[4]} {item[2]} {item[3]}\n\n"
        f"📌 Переведи: @{SHOP_BANK} → Профиль → Подарки → Передать → @{uname}",
        kb=ka
    )

@dp.callback_query(F.data.startswith("withdraw_ton_check:"))
async def cb_withdraw_ton_check(cb: types.CallbackQuery):
    vid   = int(cb.data.split(":")[1])
    uid   = cb.from_user.id
    uname = cb.from_user.username or str(uid)
    ka = InlineKeyboardBuilder()
    ka.button(text="✅ Подтвердить", callback_data=f"withdraw_ton_approve:{vid}:{uid}")
    ka.button(text="❌ Отклонить",  callback_data=f"withdraw_ton_reject:{uid}")
    ka.adjust(1)
    await notify_admins(
        f"👛 <b>ВЫВОД NFT — TON</b>\n@{uname} (ID:{uid})\nvault_id={vid}\nПроверь TON!",
        kb=ka
    )
    await cb.message.answer("📨 Ожидай проверки перевода!")
    await cb.answer()

@dp.callback_query(F.data.startswith("withdraw_ton_approve:"))
async def cb_withdraw_ton_approve(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    parts = cb.data.split(":")
    vid   = int(parts[1])
    uid   = int(parts[2])
    await _nft_withdraw_paid(cb.message, vid, uid)
    await cb.message.edit_text(cb.message.text + "\n\n✅ Подтверждено", parse_mode="HTML")
    await cb.answer("✅")

@dp.callback_query(F.data.startswith("withdraw_ton_reject:"))
async def cb_withdraw_ton_reject(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    try:
        await bot.send_message(uid, f"❌ TON перевод не найден. @{SUPPORT_USER}")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено", parse_mode="HTML")
    await cb.answer("❌")

@dp.callback_query(F.data.startswith("withdraw_done:"))
async def cb_withdraw_done(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[2])
    try:
        await bot.send_message(uid, f"🎉 <b>NFT переведён в профиль!</b>\nПрофиль → Подарки.", parse_mode="HTML")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n✅ <b>ВЫПОЛНЕНО</b>", parse_mode="HTML")
    await cb.answer("✅")

# ════════════════════════════════════════════════════════
# РЕФЕРАЛЫ / БОНУС
# ════════════════════════════════════════════════════════
@dp.callback_query(F.data == "ref_menu")
async def cb_ref_menu(cb: types.CallbackQuery):
    uid       = cb.from_user.id
    bot_me    = await bot.get_me()
    link      = f"https://t.me/{bot_me.username}?start={uid}"
    ref_stars = get_ref_stars(uid)
    today     = str(date.today())
    can       = get_last_bonus(uid) != today
    kb = InlineKeyboardBuilder()
    if can:
        kb.button(text="🎯 +1 ⭐ сегодня", callback_data="claim_bonus")
    if ref_stars >= MIN_WITHDRAW:
        kb.button(text=f"💸 Вывести {ref_stars:.2f} ⭐", callback_data="withdraw_ref")
    else:
        kb.button(text=f"⏳ Нужно {MIN_WITHDRAW - ref_stars:.2f} ⭐", callback_data="_")
    kb.button(text="🔙 Назад", callback_data="menu")
    kb.adjust(1)
    await cb.message.answer(
        f"⭐ <b>Заработать звёзды</b>\n\n"
        f"Баланс: <b>{ref_stars:.2f} ⭐</b>  (вывод от {MIN_WITHDRAW} ⭐)\n"
        f"2% с каждой покупки реферала\n\n"
        f"🔗 Твоя ссылка:\n<code>{link}</code>\n\n"
        f"📝 +1 ⭐ в день — поставь ссылку в bio",
        reply_markup=kb.as_markup(), parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "claim_bonus")
async def cb_claim_bonus(cb: types.CallbackQuery):
    uid   = cb.from_user.id
    uname = cb.from_user.username
    if not uname:
        return await cb.answer("❌ Нужен @username!", show_alert=True)
    if get_last_bonus(uid) == str(date.today()):
        return await cb.answer("⏳ Уже получен!", show_alert=True)
    bot_me = await bot.get_me()
    link   = f"https://t.me/{bot_me.username}?start={uid}"
    set_last_bonus(uid, str(date.today()))
    ka = InlineKeyboardBuilder()
    ka.button(text="✅ +1 ⭐", callback_data=f"bonus_ok:{uid}")
    ka.button(text="❌",       callback_data=f"bonus_no:{uid}")
    ka.adjust(2)
    await notify_admins(
        f"🎁 <b>ЗАПРОС БОНУСА +1 ⭐</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 @{uname} (ID:{uid})\n"
        f"🔗 <code>{link}</code>\n\n"
        f"Проверь bio и подтверди!",
        kb=ka
    )
    await cb.message.answer(f"📨 Заявка отправлена! Добавь в bio:\n<code>{link}</code>", parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data.startswith("bonus_ok:"))
async def cb_bonus_ok(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    add_ref_stars(uid, 1)
    try:
        await bot.send_message(uid, f"🎉 <b>+1 ⭐</b> начислена! Баланс: {get_ref_stars(uid):.2f} ⭐", parse_mode="HTML")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n✅ Начислено", parse_mode="HTML")
    await cb.answer("✅")

@dp.callback_query(F.data.startswith("bonus_no:"))
async def cb_bonus_no(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    uid = int(cb.data.split(":")[1])
    set_last_bonus(uid, None)
    try:
        await bot.send_message(uid, "❌ Бонус отклонён — ссылка не найдена в bio.")
    except Exception:
        pass
    await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонено", parse_mode="HTML")
    await cb.answer("❌")

@dp.callback_query(F.data == "withdraw_ref")
async def cb_withdraw_ref(cb: types.CallbackQuery):
    uid       = cb.from_user.id
    ref_stars = get_ref_stars(uid)
    if ref_stars < MIN_WITHDRAW:
        return await cb.answer(f"❌ Нужно {MIN_WITHDRAW} ⭐!", show_alert=True)
    uname = cb.from_user.username or str(uid)
    await notify_admins(
        f"💸 <b>ВЫВОД РЕФЕРАЛЬНЫХ ЗВЁЗД</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 @{uname} (ID:{uid})\n"
        f"⭐ Сумма: {ref_stars:.2f} ⭐\n\n"
        f"📌 Начисли через Fragment!"
    )
    reset_ref_stars(uid)
    await cb.message.answer(f"✅ Заявка на {ref_stars:.2f} ⭐ отправлена!")
    await cb.answer()

@dp.callback_query(F.data == "_")
async def cb_noop(cb: types.CallbackQuery):
    await cb.answer()

# ════════════════════════════════════════════════════════
# КОМАНДЫ АДМИНИСТРАТОРА
# ════════════════════════════════════════════════════════
@dp.message(Command("send"))
async def cmd_send(msg: types.Message, command: CommandObject = None):
    if msg.from_user.id not in ADMIN_IDS:
        return
    if not command or not command.args or len(command.args.split(maxsplit=1)) < 2:
        return await msg.answer("Формат: /send [ID] [текст]")
    uid_str, text = command.args.split(maxsplit=1)
    if not uid_str.isdigit():
        return await msg.answer("❌ ID — число!")
    try:
        await bot.send_message(int(uid_str), f"💬 <b>Поддержка:</b>\n\n{text}", parse_mode="HTML")
        await msg.answer("✅ Отправлено")
    except Exception as e:
        await msg.answer(f"❌ {e}")

@dp.message(Command("addbal"))
async def cmd_addbal(msg: types.Message, command: CommandObject = None):
    if msg.from_user.id not in ADMIN_IDS:
        return
    # /addbal [ID] [сумма] [rub|ton]
    if not command or not command.args or len(command.args.split()) < 2:
        return await msg.answer("Формат: /addbal [ID] [сумма] [rub|ton]")
    parts = command.args.split()
    if not parts[0].isdigit():
        return await msg.answer("❌ ID — число!")
    uid      = int(parts[0])
    try:
        amount = float(parts[1])
    except ValueError:
        return await msg.answer("❌ Сумма — число!")
    currency = parts[2].lower() if len(parts) > 2 else "rub"
    if currency == "ton":
        add_balance_ton(uid, amount)
        await msg.answer(f"✅ +{amount} TON → ID {uid}\nTON-баланс: {get_balance_ton(uid):.4f} TON")
    else:
        add_balance(uid, amount)
        await msg.answer(f"✅ +{amount} ₽ → ID {uid}\nРублёвый: {get_balance(uid):.2f} ₽")

@dp.message(Command("bal"))
async def cmd_bal(msg: types.Message, command: CommandObject = None):
    if msg.from_user.id not in ADMIN_IDS:
        return
    if not command or not command.args or not command.args.strip().isdigit():
        return await msg.answer("Формат: /bal [ID]")
    uid = int(command.args.strip())
    await msg.answer(
        f"Баланс ID {uid}:\n"
        f"💰 {get_balance(uid):.2f} ₽\n"
        f"💎 {get_balance_ton(uid):.4f} TON"
    )

@dp.message(Command("listings"))
async def cmd_listings(msg: types.Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    rows = get_active_listings()
    if not rows:
        return await msg.answer("📭 Нет активных листингов")
    text = f"📋 <b>Маркет ({len(rows)}):</b>\n\n"
    for r in rows[:20]:
        text += f"#{r[0]} {r[10]} {r[11]} · {r[4]}₽ — @{r[3]}\n"
    await msg.answer(text, parse_mode="HTML")

# ════════════════════════════════════════════════════════
# USERBOT — Telethon на аккаунте @StarShopBank
#
# Поскольку @StarShopBank — обычный user account (не бот),
# мы запускаем userbot который ловит входящие подарки.
#
# КАК ПОДКЛЮЧИТЬ:
#   1. pip install telethon
#   2. Запусти userbot_setup.py (ниже) — он создаст shopbank.session
#   3. Поставь USERBOT_ENABLED = True, заполни API_ID и API_HASH
#
# ФАЙЛ userbot_setup.py — запустить один раз для авторизации:
#   from telethon.sync import TelegramClient
#   client = TelegramClient("shopbank", API_ID, API_HASH)
#   client.start()  # введёт номер телефона @StarShopBank
#   print("Сессия создана!")
# ════════════════════════════════════════════════════════
async def run_userbot():
    if not USERBOT_ENABLED:
        return
    if not USERBOT_API_ID or not USERBOT_API_HASH:
        logging.warning("Userbot: укажи USERBOT_API_ID и USERBOT_API_HASH")
        return

    try:
        from telethon import TelegramClient, events

        client = TelegramClient(USERBOT_SESSION, USERBOT_API_ID, USERBOT_API_HASH)
        await client.start()
        logging.info("⚡ Userbot @StarShopBank запущен")

        @client.on(events.NewMessage(incoming=True))
        async def on_incoming(event):
            try:
                msg = event.message

                # Telegram передаёт подарки как MessageService
                # Проверяем тип action
                is_gift    = False
                gift_name  = "NFT"
                gift_num   = "#0000"
                gift_emoji = "🎁"
                gift_rar   = "common"

                if hasattr(msg, "action") and msg.action is not None:
                    aname = type(msg.action).__name__
                    # StarGift / InputStickerSetItem — NFT подарки
                    if any(k in aname for k in ("Gift", "Nft", "StarGift")):
                        is_gift = True
                        a = msg.action
                        if hasattr(a, "title"):
                            gift_name = a.title
                        if hasattr(a, "num"):
                            gift_num = f"#{a.num}"
                        elif hasattr(a, "slug"):
                            gift_num = a.slug
                        if hasattr(a, "sticker") and hasattr(a.sticker, "emoji"):
                            gift_emoji = a.sticker.emoji or "🎁"
                        # rarity
                        if hasattr(a, "availability_remains") and hasattr(a, "availability_total"):
                            ratio = a.availability_remains / max(a.availability_total, 1)
                            if ratio < 0.01:
                                gift_rar = "legendary"
                            elif ratio < 0.05:
                                gift_rar = "epic"
                            elif ratio < 0.15:
                                gift_rar = "rare"

                if not is_gift:
                    return

                sender = await event.get_sender()
                if not sender:
                    return

                sender_uname = (getattr(sender, "username", None) or "").lower()
                logging.info(f"Userbot: подарок от @{sender_uname}")

                # Ищем продавца в очереди
                pending = None
                if sender_uname:
                    pending = get_pending_by_username(sender_uname)

                if not pending:
                    # Никто не заявлял — уведомляем админа
                    await bot.send_message(
                        ADMIN_ID,
                        f"⚠️ <b>Userbot: получен подарок от @{sender_uname}</b>\n"
                        f"Но в очереди нет совпадений.\n"
                        f"Используй /admin_nft_add если нужно добавить вручную.",
                        parse_mode="HTML"
                    )
                    return

                seller_id = pending[1]
                mark_pending_matched(pending[0])

                vid = add_to_vault(seller_id, gift_name, gift_num, gift_emoji, gift_rar, source="sold_in")

                try:
                    await bot.send_message(
                        seller_id,
                        f"⚡ <b>NFT автоматически добавлен в хранилище!</b>\n\n"
                        f"{gift_emoji} <b>{gift_name} {gift_num}</b>\n"
                        f"Редкость: {gift_rar}\n\n"
                        f"Открой магазин → Хранилище.",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

                await bot.send_message(
                    ADMIN_ID,
                    f"⚡ <b>Userbot: NFT добавлен автоматически</b>\n"
                    f"Продавец ID:{seller_id}  |  vault_id={vid}\n"
                    f"{gift_emoji} {gift_name} {gift_num}",
                    parse_mode="HTML"
                )

            except Exception as e:
                logging.error(f"Userbot handler: {e}", exc_info=True)

        await client.run_until_disconnected()

    except ImportError:
        logging.warning("Telethon не установлен: pip install telethon")
    except Exception as e:
        logging.error(f"Userbot error: {e}", exc_info=True)

# ════════════════════════════════════════════════════════
# CATCH-ALL — логируем всё необработанное (для отладки)
# ════════════════════════════════════════════════════════
@dp.message()
async def catch_all_messages(msg: types.Message):
    logging.warning(
        f"❓ Необработанное сообщение от {msg.from_user.id}: "
        f"content_type={msg.content_type} "
        f"text={repr(msg.text or '')[:100]} "
        f"web_app_data={repr(msg.web_app_data)[:200] if msg.web_app_data else 'None'}"
    )

@dp.callback_query()
async def catch_all_callbacks(cb: types.CallbackQuery):
    logging.warning(f"❓ Необработанный callback от {cb.from_user.id}: data={repr(cb.data)}")
    await cb.answer()

# ════════════════════════════════════════════════════════
# ЗАПУСК
# ════════════════════════════════════════════════════════
async def main():
    init_db()
    logging.info("🤖 Star Shop Bot запущен!")
    tasks = [dp.start_polling(bot)]
    if USERBOT_ENABLED:
        tasks.append(run_userbot())
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
