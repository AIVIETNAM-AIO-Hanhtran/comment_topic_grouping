from DrissionPage import ChromiumPage, ChromiumOptions
import json
import time
import re
import os
import random
from datetime import datetime

# Lấy đường dẫn thư mục gốc (shopee_crawl_prj) bất kể bạn chạy từ đâu
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__)) # Thư mục 'code'
BASE_DIR = os.path.dirname(CURRENT_DIR)                 # Thư mục 'shopee_crawl_prj'

# ── CONFIG ─────────────────────────────────────────────────────────────────
URLS_FILE = os.path.join(BASE_DIR, "urls", "urls_1044352529.txt")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "checkpoint", "checkpoint_1044352529.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "reviews_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── KHỞI TẠO BROWSER ───────────────────────────────────────────────────────
co = ChromiumOptions()
co.set_browser_path(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')
co.set_argument('--disable-blink-features=AutomationControlled')
co.set_argument('--no-sandbox')
co.set_argument('--start-maximized')
co.set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36')
page = ChromiumPage(addr_or_opts=co)

# ── LOGIN ──────────────────────────────────────────────────────────────────
print("Đang truy cập Shopee...")
page.get('https://shopee.vn')
print("Đăng nhập tài khoản Shopee trên trình duyệt vừa mở.")
input(">>> Nhấn Enter sau khi đăng nhập xong: ")

# ── CÁC HÀM TIỆN ÍCH ───────────────────────────────────────────────────────
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

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f).get('done', [])
        except: return []
    return []

def save_checkpoint(item_id):
    checkpoint_data = {"done": load_checkpoint()}
    if str(item_id) not in checkpoint_data["done"]:
        checkpoint_data["done"].append(str(item_id))
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)

# ── ĐỌC FILE URL ───────────────────────────────────────────────────────────
if not os.path.exists(URLS_FILE):
    print(f"Lỗi: Không tìm thấy file {URLS_FILE}")
    exit()

with open(URLS_FILE, 'r', encoding='utf-8') as f:
    urls = [line.strip() for line in f if line.strip()]

# ── VÒNG LẶP CHÍNH ─────────────────────────────────────────────────────────
done_ids = load_checkpoint()

for url in urls:
    # Thử trích xuất item_id từ URL để check nhanh checkpoint
    match = re.search(r'i\.\d+\.(\d+)', url)
    if match and match.group(1) in done_ids:
        print(f"\n>>> Bỏ qua sản phẩm {match.group(1)} (đã có trong checkpoint)")
        continue

    print(f"\n{'='*50}")
    print(f"Đang xử lý: {url}")
    
    page.listen.start('get_ratings')
    page.get(url)

    all_reviews = []
    shop_id = item_id = None

    # Bắt trang đầu
    for packet in page.listen.steps(timeout=20):
        if 'get_ratings' not in packet.url: continue
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

    if not item_id:
        print("   Lỗi: Không bắt được item_id, thử qua sản phẩm tiếp theo.")
        continue

    # Kiểm tra lại checkpoint một lần nữa sau khi bắt được ID thật
    if str(item_id) in done_ids:
        print(f"   Sản phẩm {item_id} đã crawl trước đó, bỏ qua.")
        continue

    # Click qua từng trang
    page.scroll.to_bottom()
    time.sleep(1.5)
    page_num = 2
    
    while True:
        btn_next = page.ele('css:.shopee-icon-button--right', timeout=3)
        if not btn_next or 'disabled' in (btn_next.attr('class') or ''):
            break

        page.listen.start('get_ratings')
        btn_next.click()
        
        batch = []
        for packet in page.listen.steps(timeout=10):
            if 'get_ratings' in packet.url and isinstance(packet.response.body, dict):
                batch = parse_ratings(packet.response.body)
                break
        page.listen.stop()

        if not batch: break
        all_reviews.extend(batch)
        print(f"   → Trang {page_num}: {len(batch)} reviews (tổng: {len(all_reviews)})")
        page_num += 1
        time.sleep(1.2)

    # LƯU KẾT QUẢ VÀ CHECKPOINT
    output_filename = os.path.join(OUTPUT_DIR, f"review_{item_id}.json")
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_reviews, f, ensure_ascii=False, indent=2)

    save_checkpoint(item_id)
    done_ids.append(str(item_id)) # Cập nhật danh sách tạm để vòng lặp sau check
    print(f"Hoàn tất {item_id}, lưu vào {output_filename}")
    
    # Nghỉ ngắn giữa các sản phẩm để tránh bị nghi ngờ
    time.sleep(random.uniform(3, 7)) if 'random' in globals() else time.sleep(5)

print("\n>>> ĐÃ XỬ LÝ XONG TOÀN BỘ DANH SÁCH.")