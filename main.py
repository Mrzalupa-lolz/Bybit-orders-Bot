import time
import math
import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
import aiosqlite
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


BYBIT_P2P_URL = "https://www.bybit.com/x-api/fiat/otc/item/online"


class BybitP2PClient:
    def __init__(
        self,
        token_id: str,
        currency_id: str,
        side_value: int,
        page_size: int,
        item_region: int,
        payments: List[str],
        payment_period: List[int],
        sort_type: str,
        can_trade: bool,
        va_maker: bool,
        bulk_maker: bool,
        verification_filter: int,
        cookie: Optional[str],
        concurrency: int = 8,
        timeout_sec: float = 30.0,
    ) -> None:
        self.token_id = token_id
        self.currency_id = currency_id
        self.side_value = side_value
        self.page_size = page_size
        self.item_region = item_region
        self.payments = payments
        self.payment_period = payment_period
        self.sort_type = sort_type
        self.can_trade = can_trade
        self.va_maker = va_maker
        self.bulk_maker = bulk_maker
        self.verification_filter = verification_filter
        self.cookie = cookie
        self.concurrency = concurrency
        self.timeout_sec = timeout_sec

    def _build_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "accept": "application/json",
            "accept-language": "ru-RU",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.bybit.com",
            "platform": "PC",
            "referer": "https://www.bybit.com/ru-RU/p2p",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/140.0.0.0 Safari/537.36"
            ),
        }
        if self.cookie:
            headers["cookie"] = self.cookie
        return headers

    def _build_payload(self, page: int) -> Dict[str, Any]:
        return {
            "tokenId": self.token_id,
            "currencyId": self.currency_id,
            "payment": self.payments,
            "side": str(self.side_value),
            "size": str(self.page_size),
            "page": str(page),
            "amount": "",
            "vaMaker": self.va_maker,
            "bulkMaker": self.bulk_maker,
            "canTrade": self.can_trade,
            "verificationFilter": self.verification_filter,
            "sortType": self.sort_type,
            "paymentPeriod": self.payment_period,
            "itemRegion": self.item_region,
        }

    async def _fetch_page(self, session: aiohttp.ClientSession, page: int, retries: int = 3, backoff_sec: float = 1.5) -> Dict[str, Any]:
        for attempt in range(retries):
            try:
                async with session.post(BYBIT_P2P_URL, headers=self._build_headers(), json=self._build_payload(page), timeout=self.timeout_sec) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status in (429, 502, 503, 504):
                        await asyncio.sleep(backoff_sec * (attempt + 1))
                        continue
                    await asyncio.sleep(0.5)
            except aiohttp.ClientError:
                await asyncio.sleep(backoff_sec * (attempt + 1))
                continue
        return {}

    async def fetch_all_orders(self, max_pages: Optional[int]) -> List[Dict[str, Any]]:
        connector = aiohttp.TCPConnector(ssl=False, limit=0)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            first = await self._fetch_page(session, 1)
            result = first.get("result") or {}
            items: List[Dict[str, Any]] = list(result.get("items") or [])
            if not items:
                return []
            total_count = int(result.get("count") or 0)
            if total_count <= len(items):
                return items
            total_pages = math.ceil(total_count / max(1, self.page_size))
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)
            sem = asyncio.Semaphore(self.concurrency)

            async def fetch_with_sem(p: int) -> List[Dict[str, Any]]:
                async with sem:
                    data = await self._fetch_page(session, p)
                    res = data.get("result") or {}
                    return list(res.get("items") or [])

            tasks = [fetch_with_sem(p) for p in range(2, total_pages + 1)]
            if tasks:
                pages = await asyncio.gather(*tasks, return_exceptions=False)
                for page_items in pages:
                    if page_items:
                        items.extend(page_items)
            return items


class OrderRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                  id TEXT PRIMARY KEY,
                  tokenId TEXT,
                  currencyId TEXT,
                  price TEXT,
                  minAmount TEXT,
                  maxAmount TEXT,
                  remark TEXT,
                  nickName TEXT,
                  userMaskId TEXT,
                  profileUrl TEXT,
                  fetchedAt INTEGER
                )
                """
            )
            await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_userMaskId ON orders(userMaskId)")
            await db.commit()

    async def insert_new(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not items:
            return []
        ids = [str(it.get("id", "")) for it in items if str(it.get("id", ""))]
        if not ids:
            return []
        async with aiosqlite.connect(self.db_path) as db:
            existing: set = set()
            chunk = 900
            for i in range(0, len(ids), chunk):
                part = ids[i : i + chunk]
                qmarks = ",".join(["?"] * len(part))
                async with db.execute(f"SELECT id FROM orders WHERE id IN ({qmarks})", part) as cur:
                    async for row in cur:
                        existing.add(row[0])
            new_items: List[Dict[str, Any]] = [it for it in items if str(it.get("id", "")) and str(it.get("id")) not in existing]
            if not new_items:
                return []
            sql = (
                "INSERT OR IGNORE INTO orders (id, tokenId, currencyId, price, minAmount, maxAmount, remark, nickName, userMaskId, profileUrl, fetchedAt)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            now = int(time.time())
            params: List[tuple] = []
            for it in new_items:
                order_id = str(it.get("id", ""))
                token_id = it.get("tokenId", "")
                currency_id = it.get("currencyId", "")
                price = it.get("price", "")
                min_amount = it.get("minAmount", "")
                max_amount = it.get("maxAmount", "")
                remark = (it.get("remark", "") or "").replace("\n", " ").strip()
                nick = it.get("nickName", "")
                user_mask = it.get("userMaskId", "")
                profile_url = f"https://www.bybit.com/ru-RU/p2p/profile/{user_mask}/{token_id}/{currency_id}/item" if user_mask and token_id and currency_id else ""
                params.append((order_id, token_id, currency_id, price, min_amount, max_amount, remark, nick, user_mask, profile_url, now))
            await db.executemany(sql, params)
            await db.commit()
            return [
                {
                    "id": str(it.get("id", "")),
                    "tokenId": it.get("tokenId", ""),
                    "currencyId": it.get("currencyId", ""),
                    "price": it.get("price", ""),
                    "minAmount": it.get("minAmount", ""),
                    "maxAmount": it.get("maxAmount", ""),
                    "remark": (it.get("remark", "") or "").replace("\n", " ").strip(),
                    "nickName": it.get("nickName", ""),
                    "userMaskId": it.get("userMaskId", ""),
                    "profileUrl": f"https://www.bybit.com/ru-RU/p2p/profile/{it.get('userMaskId','')}/{it.get('tokenId','')}/{it.get('currencyId','')}/item" if it.get("userMaskId") and it.get("tokenId") and it.get("currencyId") else "",
                    "quantity": it.get("quantity", ""),
                }
                for it in new_items
            ]


class OrderFilter:
    def __init__(
        self,
        max_price: Optional[float] = None,
        min_amount_limit: Optional[float] = None,
        max_amount_limit: Optional[float] = None
    ) -> None:
        self.max_price = max_price
        self.min_amount_limit = min_amount_limit
        self.max_amount_limit = max_amount_limit

    def filter_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered_orders = []
        
        for order in orders:
            # Проверяем цену
            try:
                price = float(order.get('price', 0))
                if self.max_price is not None and price > self.max_price:
                    continue
            except (ValueError, TypeError):
                continue
            
            # Проверяем минимальный лимит
            try:
                min_amount = float(order.get('minAmount', 0))
                if self.min_amount_limit is not None and min_amount < self.min_amount_limit:
                    continue
            except (ValueError, TypeError):
                continue
            
            # Проверяем максимальный лимит
            try:
                max_amount = float(order.get('maxAmount', 0))
                if self.max_amount_limit is not None and max_amount > self.max_amount_limit:
                    continue
            except (ValueError, TypeError):
                continue
            
            filtered_orders.append(order)
        
        return filtered_orders


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, concurrency: int = 5) -> None:
        self.bot = Bot(token=bot_token)
        self.chat_id = chat_id
        self.semaphore = asyncio.Semaphore(concurrency)

    async def send_orders(self, items: List[Dict[str, Any]]) -> None:
        if not items:
            return
        
        async def send_one(order: Dict[str, Any]) -> None:
            async with self.semaphore:
                try:
                    token_id = order.get("tokenId", "")
                    currency_id = order.get("currencyId", "")
                    price = order.get("price", "")
                    min_amount = order.get("minAmount", "")
                    max_amount = order.get("maxAmount", "")
                    nick = order.get("nickName", "")
                    profile_url = order.get("profileUrl", "")
                    remark = order.get("remark", "")

                    text = (
                        f"<b>Новый ордер</b>\n"
                        f"<b>Пара</b>: <b>{self._escape(token_id)}</b> → <b>{self._escape(currency_id)}</b>\n"
                        f"<b>Цена</b>: <code>{self._escape(str(price))}</code>\n"
                        f"<b>Мин</b>: <code>{self._escape(str(min_amount))}</code>  "
                        f"<b>Макс</b>: <code>{self._escape(str(max_amount))}</code>\n"
                        f"<b>Ник</b>: {self._escape(nick)}\n"
                        f"<b>Комментарий</b>: <blockquote>{self._escape(remark)}</blockquote>"
                    )

                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(
                            text="Открыть профиль",
                            url=profile_url or "https://www.bybit.com/ru-RU/p2p"
                        )]
                    ])

                    await self.bot.send_message(
                        chat_id=self.chat_id,
                        text=text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=keyboard
                    )
                except Exception as e:
                    print(f"Ошибка отправки сообщения: {e}")

        tasks = [send_one(order) for order in items]
        await asyncio.gather(*tasks)

    def _escape(self, s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    async def close(self) -> None:
        await self.bot.close()


async def main() -> None:
    # Конфигурация
    TOKEN_ID = "USDT"
    CURRENCY_ID = "RUB"
    SIDE_VALUE = 1  # 0 - покупка usdt, 1 - продажа usdt
    PAGE_SIZE = 50
    ITEM_REGION = 1
    PAYMENTS: List[str] = []
    PAYMENT_PERIOD: List[int] = []
    SORT_TYPE = "OVERALL_RANKING"
    CAN_TRADE = True
    VA_MAKER = False
    BULK_MAKER = False
    VERIFICATION_FILTER = 0
    COOKIE: Optional[str] = None
    CONCURRENCY = 8
    MAX_PAGES: Optional[int] = 10
    POLL_INTERVAL_SEC = 20
    DB_PATH = "orders.db"
    
    # Настройки Telegram
    TG_BOT_TOKEN = "BOT_TOKEN"
    TG_CHAT_ID = "-TELEGRAM_GROUP_ID"
    
    # Фильтры
    MAX_PRICE = 82.0  # Максимальная цена для уведомления
    MIN_AMOUNT_LIMIT = 0.0  # Минимальный лимит ордера
    MAX_AMOUNT_LIMIT = 5000.0  # Максимальный лимит ордера

    # Инициализация
    repo = OrderRepository(DB_PATH)
    await repo.init()
    
    client = BybitP2PClient(
        token_id=TOKEN_ID,
        currency_id=CURRENCY_ID,
        side_value=SIDE_VALUE,
        page_size=PAGE_SIZE,
        item_region=ITEM_REGION,
        payments=PAYMENTS,
        payment_period=PAYMENT_PERIOD,
        sort_type=SORT_TYPE,
        can_trade=CAN_TRADE,
        va_maker=VA_MAKER,
        bulk_maker=BULK_MAKER,
        verification_filter=VERIFICATION_FILTER,
        cookie=COOKIE,
        concurrency=max(1, CONCURRENCY),
    )
    
    notifier = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID, concurrency=5)
    order_filter = OrderFilter(MAX_PRICE, MIN_AMOUNT_LIMIT, MAX_AMOUNT_LIMIT)

    try:
        while True:
            items = await client.fetch_all_orders(MAX_PAGES)
            
            new_items = await repo.insert_new(items)
            
            if new_items:
                filtered_orders = order_filter.filter_orders(new_items)
                
                if filtered_orders:
                    await notifier.send_orders(filtered_orders)
                    print(f"Отправлено уведомлений: {len(filtered_orders)}")
            
            print(f"Новых ордеров: {len(new_items)}, Всего ордеров: {len(items)}, Время: {int(time.time())}")
            await asyncio.sleep(POLL_INTERVAL_SEC)
            
    except KeyboardInterrupt:
        print("Остановка по запросу пользователя...")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    finally:
        await notifier.close()


if __name__ == "__main__":
    asyncio.run(main())