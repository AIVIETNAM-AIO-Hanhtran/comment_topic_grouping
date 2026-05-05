"""
shopee_crawler.py
=================
Crawl review sản phẩm từ một shop Shopee.

Quy trình:
  1. Login thủ công lần đầu → lưu session (cookie + localStorage)
  2. Những lần sau load lại session → không cần login lại
  3. Lấy danh sách sản phẩm từ shop
  4. Với từng sản phẩm → gọi API review (phân trang)
  5. Lưu kết quả ra CSV + JSON, kèm checkpoint để resume khi lỗi

Yêu cầu:
  pip install playwright pandas tqdm
  playwright install chromium
"""

from __future__ import annotations

import asyncio
import json
import csv
import time
import random
import os
import logging
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright
import pandas as pd
from tqdm import tqdm

# ──────────────────────────────────────────────
# CONFIG - Adjust before starting
# ──────────────────────────────────────────────
CONFIG = {
    # ID của shop cần crawl (lấy từ URL https://shopee.vn/api/v4/shop/get_shop_detail?username=shopname)
    "shop_id": "155343961",

    # Thư mục lưu dữ liệu đầu ra
    "output_dir": "files",

    # File lưu session (cookie + localStorage)
    "session_file": "session.json",

    # File checkpoint — ghi lại product_id đã crawl xong để resume
    "checkpoint_file": "checkpoint.json",

    # Số review mỗi trang (API Shopee tối đa 20)
    "reviews_per_page": 20,

    # Delay ngẫu nhiên (giây) giữa mỗi request để tránh rate-limit
    "delay_min": 2.0,
    "delay_max": 4.5,

    # Delay thêm sau mỗi sản phẩm (giây)
    "product_delay_min": 3.0,
    "product_delay_max": 7.0,

    # Số lần retry khi gặp lỗi network / rate-limit
    "max_retries": 3,

    # Headless = True → chạy ngầm (không hiện cửa sổ browser)
    # Headless = False → hiện browser (dùng khi login lần đầu)
    "headless": False,

    # Proxy (để trống nếu không dùng)
    # Ví dụ: "http://user:pass@proxy_ip:port"
    "proxy": "",

    # Đường dẫn Chrome thật (tránh bot detection so với Chromium của Playwright)
    "chrome_path": r"C:\Program Files\Google\Chrome\Application\chrome.exe",

    # Thư mục profile Chrome riêng cho crawler (tách khỏi profile cá nhân)
    "chrome_profile_dir": "chrome_data",
}

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crawler.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════
# PHẦN 1 — QUẢN LÝ SESSION
# ══════════════════════════════════════════════

async def save_session(context, session_file: str):
    """
    Lưu toàn bộ cookie + localStorage vào file JSON.
    Gọi hàm này SAU KHI đã login thành công.
    """
    cookies = await context.cookies()
    # Lấy localStorage từ trang Shopee đang mở
    pages = context.pages
    storage = {}
    if pages:
        try:
            storage = await pages[0].evaluate("() => JSON.stringify(localStorage)")
            storage = json.loads(storage)
        except Exception:
            storage = {}

    session_data = {
        "cookies": cookies,
        "localStorage": storage,
        "saved_at": datetime.now().isoformat(),
    }
    with open(session_file, "w", encoding="utf-8") as f:
        json.dump(session_data, f, ensure_ascii=False, indent=2)
    log.info(f"Session đã lưu vào {session_file}")


async def load_session(context, session_file: str) -> bool:
    """
    Nạp lại session từ file JSON vào browser context.
    Trả về True nếu thành công, False nếu file không tồn tại.
    """
    if not Path(session_file).exists():
        log.warning("Chưa có file session. Cần login thủ công.")
        return False

    with open(session_file, "r", encoding="utf-8") as f:
        session_data = json.load(f)

    # Nạp cookie
    await context.add_cookies(session_data["cookies"])

    # Nạp localStorage (mở 1 trang trắng để inject)
    page = await context.new_page()
    await page.goto("https://shopee.vn", wait_until="domcontentloaded")
    if session_data.get("localStorage"):
        await page.evaluate(
            """(data) => {
                for (const [key, value] of Object.entries(data)) {
                    localStorage.setItem(key, value);
                }
            }""",
            session_data["localStorage"],
        )
    await page.close()
    log.info("Session đã nạp thành công")
    return True


async def manual_login(context, session_file: str):
    """
    Mở browser để người dùng login thủ công.
    Sau khi phát hiện đã login → tự động lưu session.
    """
    page = await context.new_page()
    await page.goto("https://shopee.vn/buyer/login", wait_until="networkidle")
    log.info("Login vào tài khoản Shopee trên cửa sổ browser vừa mở. Sau khi login xong, nhấn Enter ở terminal để tiếp tục")

    # Chờ confirm đã login
    input("   >>> Nhấn Enter sau khi đã login thành công: ")

    await save_session(context, session_file)
    await page.close()


async def check_session_alive(page) -> bool:
    """
    Kiểm tra session còn sống không bằng cách gọi API user info.
    Trả về True nếu đang đăng nhập, False nếu đã hết session.
    """
    try:
        resp = await page.request.get(
            "https://shopee.vn/api/v4/account/basic/get_account_info",
            headers={"x-requested-with": "XMLHttpRequest"},
        )
        data = await resp.json()
        # Shopee trả về error code 0 = thành công
        return data.get("code") == 0
    except Exception:
        return False


# ══════════════════════════════════════════════
# PHẦN 2 — LẤY DANH SÁCH SẢN PHẨM CỦA SHOP
# ══════════════════════════════════════════════

async def get_shop_products(page, shop_id: str) -> list[dict]:
    """
    Gọi API Shopee để lấy toàn bộ sản phẩm của một shop.
    Trả về list các dict {item_id, shop_id, name, url}.
    """
    products = []
    offset = 0
    limit = 30  # Số sản phẩm mỗi trang (API cho phép tối đa 100)

    log.info(f"Đang lấy danh sách sản phẩm shop {shop_id}...")

    while True:
        url = (
            f"https://shopee.vn/api/v4/shop/rcmd_items"
            f"?limit={limit}&offset={offset}&shopid={shop_id}&sort_type=1"
        )

        try:
            data = await _browser_fetch(page, url)
        except Exception as e:
            log.error(f"Lỗi lấy sản phẩm offset={offset}: {e}")
            break

        if data.get("error") and data["error"] != 0:
            log.error(f"API lỗi: {data}")
            break

        items = data.get("items") or []
        if not items:
            break  # Hết sản phẩm

        for item in items:
            products.append({
                "item_id": item.get("itemid"),
                "shop_id": item.get("shopid"),
                "name": item.get("name", ""),
                "url": f"https://shopee.vn/product/{item.get('shopid')}/{item.get('itemid')}",
            })

        log.info(f"   → Đã lấy {len(products)} sản phẩm (offset={offset})")
        offset += limit

        # Dừng nếu đã lấy hết (API trả ít hơn limit)
        if len(items) < limit:
            break

        await _random_delay(CONFIG["delay_min"], CONFIG["delay_max"])

    log.info(f"Tổng cộng {len(products)} sản phẩm")
    return products


# ══════════════════════════════════════════════
# PHẦN 3 — LẤY REVIEW CỦA TỪNG SẢN PHẨM
# ══════════════════════════════════════════════

async def get_product_reviews(page, item_id: int, shop_id: int) -> list[dict]:
    """
    Lấy TẤT CẢ review của một sản phẩm (phân trang).
    Dừng khi response trả về mảng rỗng.

    Mỗi review trả về:
      - product_name, item_id, shop_id
      - reviewer_username
      - rating (1–5 sao)
      - comment
      - reviewed_date (ISO string)
      - helpful_count (số người thấy hữu ích)
      - reply (phản hồi của shop, nếu có)
    """
    all_reviews = []
    offset = 0
    limit = CONFIG["reviews_per_page"]

    while True:
        url = (
            f"https://shopee.vn/api/v4/item/get_ratings"
            f"?filter=0&flag=1&itemid={item_id}&limit={limit}"
            f"&offset={offset}&shopid={shop_id}&type=0"
        )

        data = await _fetch_with_retry(page, url)
        if data is None:
            log.warning(f"Không lấy được review tại offset={offset}, dừng sản phẩm này.")
            break

        ratings = data.get("data", {}).get("ratings") or []
        if not ratings:
            break  # Hết review

        for r in ratings:
            # Parse ngày review từ Unix timestamp (milliseconds)
            ctime = r.get("ctime", 0)
            reviewed_date = (
                datetime.fromtimestamp(ctime).isoformat() if ctime else ""
            )

            all_reviews.append({
                "item_id": item_id,
                "shop_id": shop_id,
                "reviewer_username": r.get("author_username", ""),
                "rating": r.get("rating_star", 0),
                "comment": r.get("comment", "").strip(),
                "reviewed_date": reviewed_date,
                "helpful_count": r.get("useful", 0)
            })

        offset += limit
        log.debug(f"      offset={offset}, tổng reviews={len(all_reviews)}")

        # Delay giữa các trang review
        await _random_delay(CONFIG["delay_min"], CONFIG["delay_max"])

    return all_reviews


# ══════════════════════════════════════════════
# PHẦN 4 — CHECKPOINT (resume khi bị gián đoạn)
# ══════════════════════════════════════════════

def load_checkpoint(checkpoint_file: str) -> set:
    """Trả về set các item_id đã crawl xong."""
    if not Path(checkpoint_file).exists():
        return set()
    with open(checkpoint_file, "r") as f:
        data = json.load(f)
    return set(data.get("done_ids", []))


def save_checkpoint(checkpoint_file: str, done_ids: set):
    """Ghi checkpoint xuống file."""
    with open(checkpoint_file, "w") as f:
        json.dump({"done_ids": list(done_ids)}, f)


# ══════════════════════════════════════════════
# PHẦN 5 — LƯU DỮ LIỆU
# ══════════════════════════════════════════════

def save_batch(reviews: list[dict], output_dir: str, item_id: int):
    """
    Lưu reviews của một sản phẩm vào:
      - JSON: output/<item_id>.json
      - CSV: output/all_reviews.csv (append)
    """
    os.makedirs(output_dir, exist_ok=True)

    # Lưu JSON riêng cho từng sản phẩm
    json_path = Path(output_dir) / f"{item_id}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(reviews, f, ensure_ascii=False, indent=2)

    # Append vào CSV tổng hợp
    csv_path = Path(output_dir) / "all_reviews.csv"
    file_exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=reviews[0].keys() if reviews else [])
        if not file_exists:
            writer.writeheader()
        writer.writerows(reviews)


# ══════════════════════════════════════════════
# PHẦN 6 — TIỆN ÍCH (headers, delay, retry)
# ══════════════════════════════════════════════



async def _random_delay(min_s: float, max_s: float):
    """Delay ngẫu nhiên để mô phỏng hành vi người dùng thật."""
    delay = random.uniform(min_s, max_s)
    await asyncio.sleep(delay)


async def _browser_fetch(page, url: str) -> dict:
    """
    Gọi fetch() bên trong browser — tự động mang cookie + fingerprint thật.
    Ném exception nếu HTTP không phải 2xx.
    """
    result = await page.evaluate(
        """async (url) => {
            const resp = await fetch(url, { credentials: 'include' });
            return { status: resp.status, body: await resp.json() };
        }""",
        url,
    )
    if result["status"] == 429:
        raise RuntimeError("rate-limit")
    if result["status"] != 200:
        raise RuntimeError(f"HTTP {result['status']}")
    return result["body"]


async def _fetch_with_retry(page, url: str, retries: int = None) -> dict | None:
    """
    Gọi _browser_fetch với cơ chế retry.
    Nếu gặp rate-limit (HTTP 429) → dừng lâu hơn rồi thử lại.
    Trả về dict JSON hoặc None nếu thất bại hoàn toàn.
    """
    if retries is None:
        retries = CONFIG["max_retries"]

    for attempt in range(1, retries + 1):
        try:
            return await _browser_fetch(page, url)
        except RuntimeError as e:
            if "rate-limit" in str(e):
                wait = random.uniform(30, 60)
                log.warning(f"Rate-limit! Dừng {wait:.0f}s rồi thử lại... (lần {attempt})")
                await asyncio.sleep(wait)
            else:
                log.warning(f"{e} tại {url} (lần {attempt})")
                await _random_delay(5, 10)
        except Exception as e:
            log.error(f"Lỗi không xác định: {e} (lần {attempt})")
            await _random_delay(5, 10)

    return None  # Hết lần thử


# ══════════════════════════════════════════════
# PHẦN 7 — HÀM CHÍNH
# ══════════════════════════════════════════════

async def main():
    shop_id = CONFIG["shop_id"]
    session_file = CONFIG["session_file"]
    checkpoint_file = CONFIG["checkpoint_file"]
    output_dir = CONFIG["output_dir"]

    os.makedirs(output_dir, exist_ok=True)

    # ── Khởi tạo Playwright ──────────────────
    async with async_playwright() as pw:
        proxy_cfg = {"server": CONFIG["proxy"]} if CONFIG["proxy"] else None

        # Dùng Chrome thật + persistent profile để tránh bot detection
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=CONFIG["chrome_profile_dir"],
            executable_path=CONFIG["chrome_path"],
            headless=CONFIG["headless"],
            proxy=proxy_cfg,
            viewport={"width": 1366, "height": 768},
            locale="vi-VN",
            timezone_id="Asia/Ho_Chi_Minh",
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
            ],
        )

        # ── Quản lý session ──────────────────
        session_loaded = await load_session(context, session_file)
        if not session_loaded:
            await manual_login(context, session_file)

        # Mở trang để dùng cho các API calls
        page = await context.new_page()

        # Kiểm tra session còn sống không
        alive = await check_session_alive(page)
        if not alive:
            log.warning("Session đã hết hạn! Cần login lại.")
            await manual_login(context, session_file)

        # ── Lấy danh sách sản phẩm ──────────
        products = await get_shop_products(page, shop_id)
        if not products:
            log.error("Không lấy được sản phẩm. Kiểm tra shop_id hoặc session.")
            return

        # Lưu danh sách sản phẩm
        products_file = Path(output_dir) / "products.json"
        with open(products_file, "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        # ── Load checkpoint ──────────────────
        done_ids = load_checkpoint(checkpoint_file)
        remaining = [p for p in products if p["item_id"] not in done_ids]
        log.info(f"Tổng {len(products)} sản phẩm, còn lại {len(remaining)} cần crawl")

        # ── Crawl reviews từng sản phẩm ─────
        total_reviews = 0
        for product in tqdm(remaining, desc="Crawling reviews"):
            item_id = product["item_id"]
            name = product["name"][:50]  # Rút gọn tên để log
            log.info(f"🔍 [{item_id}] {name}")

            reviews = await get_product_reviews(page, item_id, int(shop_id))

            # Gắn tên sản phẩm vào mỗi review
            for r in reviews:
                r["product_name"] = product["name"]
                r["product_url"] = product["url"]

            if reviews:
                save_batch(reviews, output_dir, item_id)
                total_reviews += len(reviews)
                log.info(f"{len(reviews)} reviews → đã lưu (tổng: {total_reviews})")

            # Đánh dấu checkpoint
            done_ids.add(item_id)
            save_checkpoint(checkpoint_file, done_ids)

            # Delay dài hơn giữa các sản phẩm
            await _random_delay(
                CONFIG["product_delay_min"],
                CONFIG["product_delay_max"],
            )

        log.info(f"\nHoàn thành! Tổng {total_reviews} reviews từ {len(done_ids)} sản phẩm.")
        log.info(f"Dữ liệu lưu tại: {output_dir}/")

        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
