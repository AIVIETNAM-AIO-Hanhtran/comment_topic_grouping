import pandas as pd
import json
import os
import glob

def process_reviews(folder_paths):
    all_data = []
    processed_folders = 0

    # Chuyển input thành list nếu người dùng chỉ để 1 chuỗi
    if isinstance(folder_paths, str):
        folder_paths = [p.strip().replace('"', '').replace("'", "") for p in folder_paths.split(',')]

    for path in folder_paths:
        if not path or not os.path.exists(path):
            print(f" Đường dẫn không tồn tại hoặc trống: {path}")
            continue
        
        # Tự động lấy shop_id từ tên folder cuối cùng
        shop_id = os.path.basename(os.path.normpath(path))
        
        # Tìm các file review_*.json
        file_pattern = os.path.join(path, "review_*.json")
        files = glob.glob(file_pattern)
        
        if not files:
            print(f"Không thấy file review_*.json trong folder shop: {shop_id}")
            continue

        print(f"Đang xử lý Shop ID: {shop_id} ({len(files)} files)...")
        
        for file_path in files:
            file_name = os.path.basename(file_path)
            # Lấy itemid từ tên file (review_12345.json -> 12345)
            item_id = file_name.replace("review_", "").replace(".json", "")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    if isinstance(data, dict):
                        data = [data]
                    
                    for entry in data:
                        entry['shopid'] = shop_id
                        entry['itemid'] = item_id
                        all_data.append(entry)
            except Exception as e:
                print(f"Lỗi tại file {file_name} của shop {shop_id}: {e}")
        
        processed_folders += 1

    if not all_data:
        print("Không có dữ liệu để xử lý.")
        return

    # 2. Xử lý dữ liệu
    df = pd.DataFrame(all_data)

    # Lọc comment rỗng
    df['comment'] = df['comment'].fillna("").astype(str)
    df_filtered = df[df['comment'].str.strip() != ""].copy()

    # 3. Xuất file CSV
    output_name = "shopee_crawl_prj\reviews_output.csv"
    df_filtered.to_csv(output_name, index=False, encoding='utf-8-sig')

    # 4. Hiển thị Terminal
    print("\n" + "="*50)
    print(f"XỬ LÝ HOÀN TẤT")
    print(f"Số lượng sản phẩm (unique itemid): {df_filtered['itemid'].nunique()}")
    print(f"Tổng số review có nội dung: {len(df_filtered)}")
    print("-" * 50)
    print("SỐ LƯỢNG REVIEW THEO RATING:")
    
    rating_stats = df_filtered['rating'].value_counts().sort_index(ascending=False)
    print(rating_stats.to_string())
    print("="*50)

if __name__ == "__main__":
    # LINK FOLDER
    # Nếu có nhiều folder thì ngăn cách bằng dấu phẩy
    INPUT = r"shopee_crawl_prj\reviews_data\1044352529"
    
    process_reviews(INPUT)