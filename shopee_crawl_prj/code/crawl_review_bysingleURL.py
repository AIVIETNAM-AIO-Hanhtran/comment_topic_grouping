from DrissionPage import ChromiumPage, ChromiumOptions
import json
import time
import re
import os
from datetime import datetime

# ── CONFIG ─────────────────────────────────────────────────────────────────
PRODUCT_URL = "https://shopee.vn/-ANKER-S%E1%BA%A1c-D%E1%BB%B1-Ph%C3%B2ng-Anker-t%E1%BB%AB-t%C3%ADnh-10kmAh-Anker-MagGo-Power-Bank-MagSafe-Qi2-(10K-Magnetic-Wireless)-A1664-i.1044352529.27064455165?extraParams=%7B%22display_model_id%22%3A400692567663%2C%22model_selection_logic%22%3A3%7D"

CHECKPOINT_FILE = "shopee_crawl_prj\checkpoint\checkpoint_1044352529.json"
REVIEWS_PER_PAGE = 20

# ── KHỞI TẠO BROWSER ───────────────────────────────────────────────────────
co = ChromiumOptions()
co.set_browser_path(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')
co.set_argument('--disable-blink-features=AutomationControlled')
co.set_argument('--no-sandbox')
co.set_argument('--start-maximized')

page = ChromiumPage(addr_or_opts=co)

# ── LOGIN ──────────────────────────────────────────────────────────────────
print("Đang truy cập Shopee...")
page.get('https://shopee.vn')
print("Đăng nhập tài khoản Shopee trên trình duyệt vừa mở.")
input(">>> Nhấn Enter sau khi đăng nhập xong: ")

# ── PARSE REVIEW TỪ PACKET ─────────────────────────────────────────────────
def parse_ratings(body):
    ratings = (body.get('data') or {}).get('ratings') or []
    result = []
    for r in ratings:
        ctime = r.get('ctime', 0)
        result.append({
            'reviewer': r.get('author_username', ''),
            'rating':   r.get('rating_star', 0),
            'comment':  (r.get('comment') or '').strip(),
            'date':     datetime.fromtimestamp(ctime).strftime('%Y-%m-%d') if ctime else '',
            'helpful':  r.get('useful', 0),
            'reply':    ((r.get('shop_reply') or {}).get('comment') or '').strip(),
        })
    return result

# ── MỞ TRANG SP, BẮT TRANG REVIEW ĐẦU TIÊN ───────────────────────────────
print("\nĐang mở trang sản phẩm...")
page.listen.start('get_ratings')
page.get(PRODUCT_URL)

all_reviews = []
shop_id = item_id = None

for packet in page.listen.steps(timeout=20):
    if 'get_ratings' not in packet.url:
        continue
    m = re.search(r'itemid=(\d+)', packet.url)
    if m: item_id = m.group(1)
    m = re.search(r'shopid=(\d+)', packet.url)
    if m: shop_id = m.group(1)

    body = packet.response.body
    if isinstance(body, dict):
        batch = parse_ratings(body)
        all_reviews.extend(batch)
        print(f"   → Trang 1: {len(batch)} reviews")
    break

page.listen.stop()

if not shop_id or not item_id:
    raise SystemExit(" Không bắt được dữ liệu. Thử scroll xuống phần reviews rồi chạy lại.")

print(f"item_id={item_id} | shop_id={shop_id}\n")

# ── CLICK QUA TỪNG TRANG REVIEW ────────────────────────────────────────────
# Scroll xuống phần reviews để nút Next hiện ra
page.scroll.to_bottom()
time.sleep(2)

page_num = 2
while True:
    # Shopee dùng class này cho nút Next, disabled thì thêm class "shopee-button-disabled"
    btn_next = page.ele('css:.shopee-icon-button--right', timeout=3)

    if not btn_next:
        print("→ Không tìm thấy nút Next.")
        break

    btn_class = btn_next.attr('class') or ''
    if 'disabled' in btn_class or btn_next.attr('disabled') is not None:
        print("→ Nút Next bị disabled — đã hết trang.")
        break

    print(f"   Đang click sang trang {page_num}...")
    page.listen.start('get_ratings')
    btn_next.click()

    batch = []
    for packet in page.listen.steps(timeout=10):
        if 'get_ratings' not in packet.url:
            continue
        body = packet.response.body
        if isinstance(body, dict):
            batch = parse_ratings(body)
        break

    page.listen.stop()

    if not batch:
        print("→ Trang này không có review — dừng.")
        break

    all_reviews.extend(batch)
    print(f"   → Trang {page_num}: {len(batch)} reviews (tổng: {len(all_reviews)})")
    page_num += 1
    time.sleep(1.5)

# ── LƯU KẾT QUẢ ───────────────────────────────────────────────────────────
# 1. Lưu file review theo item_id
output_filename = f"review_{item_id}.json"
with open(output_filename, 'w', encoding='utf-8') as f:
    json.dump(all_reviews, f, ensure_ascii=False, indent=2)

print(f"\nXong! {len(all_reviews)} reviews → {os.path.abspath(output_filename)}")

# 2. Cập nhật vào file checkpoint_multiple.json
# Kiểm tra nếu file đã tồn tại thì đọc dữ liệu cũ, nếu chưa thì tạo mới
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            checkpoint_data = json.load(f)
    except:
        checkpoint_data = {"done": []}
else:
    checkpoint_data = {"done": []}

# Thêm item_id vào danh sách nếu chưa có
if str(item_id) not in checkpoint_data["done"]:
    checkpoint_data["done"].append(str(item_id))

# Lưu lại file checkpoint
with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
    json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)

