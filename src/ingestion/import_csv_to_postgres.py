import glob
import pandas as pd
from sqlalchemy import create_engine

def import_csvs_to_postgres():
    # 1. Kết nối đến PostgreSQL
    db_url = 'postgresql://postgres:postgres@localhost:5432/youtube_db'
    engine = create_engine(db_url)

    print("==================================================")
    print(" Bắt đầu quá trình nạp dữ liệu CSV vào PostgreSQL...")
    print("==================================================")

    # 2. Lấy tất cả các file CSV trong thư mục data/
    csv_files = glob.glob('data/youtube_master_*.csv')
    
    if not csv_files:
        print("⚠️ Không tìm thấy file CSV nào trong thư mục data/! Vui lòng kiểm tra lại đường dẫn.")
        return

    total_records = 0

    # 3. Lần lượt đọc và nạp từng file
    for file in csv_files:
        print(f"Đang nạp file: {file}...")
        df = pd.read_csv(file)
        
        # Gộp/append vào bảng youtube_videos trong PostgreSQL
        df.to_sql('youtube_videos', con=engine, if_exists='append', index=False)
        
        record_count = len(df)
        total_records += record_count
        print(f"-> Đã nạp thành công {record_count} dòng từ {file}")

    print("==================================================")
    print(f" Hoàn tất! Tổng số bản ghi đã nạp vào PostgreSQL: {total_records}")
    print("==================================================")

if __name__ == "__main__":
    import_csvs_to_postgres()