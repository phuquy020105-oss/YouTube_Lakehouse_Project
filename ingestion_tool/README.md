# YouTube Lakehouse Ingestion Engine (GUI Tool)

Công cụ thu thập, làm giàu và bóc tách dữ liệu từ YouTube Data API v3, đóng vai trò là tầng Ingestion trực tiếp vào tầng Bronze trong kiến trúc Data Lakehouse (Medallion Architecture).

---

## 1. Giới thiệu tổng quan

* **Mục đích:** Xây dựng ứng dụng máy tính thu nạp dữ liệu thô tự động từ YouTube về tầng Bronze.
* **Giao diện:** Ứng dụng Desktop viết bằng PyQt6 hỗ trợ Dark Mode, chia hai phân vùng cấu hình và theo dõi.
* **Nguyên lý vận hành:** Phân tách rõ ràng giữa việc thiết lập thông số nghiệp vụ và luồng thu thập ngầm.
* **Đầu ra:** Dữ liệu chuẩn sạch, hỗ trợ truy vết và sẵn sàng nạp cho Apache Spark hoặc DuckDB.

---

## 2. Các tính năng chính

* **Xử lý đa luồng (QThread):** Chạy tiến trình cào dữ liệu ở luồng phụ, giao diện không bị giật lag hay treo.
* **Bảo mật biến môi trường:** Tích hợp thư viện python-dotenv tự động đọc API Key từ file .env.
* **Bộ lọc ngôn ngữ tiếng Việt:** Dùng Regex quét Title, Channel Title và Description để lọc chuẩn nội dung tiếng Việt.
* **Phân trang theo tháng:** Chia nhỏ mốc thời gian truy vấn theo từng tháng để kiểm soát bộ nhớ và hạn ngạch API.
* **Bóc tách chỉ số video:** Lấy thông tin ID, thời lượng, lượt xem, lượt like, số comment, tỷ lệ tương tác.
* **Bóc tách nội dung:** Lưu mô tả rút gọn, danh sách thẻ tags và top các bình luận nổi bật kèm lượt like.
* **Làm giàu dữ liệu kênh:** Gom nhóm gọi batch 50 channel ID/lần để lấy số lượng đăng ký và tổng số video.
* **Đóng dấu Data Lineage:** Tự động sinh batch_id và thời gian ingested_at (UTC) vào từng bản ghi.
* **Định dạng đa dạng:** Xuất linh hoạt ra Parquet (tối ưu Spark), JSON Lines và CSV.

---

## 3. Cấu trúc thư mục

* **ingestion_tool/app.py:** Tệp mã nguồn chính chứa giao diện PyQt6 và worker xử lý dữ liệu.
* **ingestion_tool/requirements.txt:** Tệp danh sách các thư viện Python cần cài đặt.
* **ingestion_tool/.gitignore:** Tệp cấu hình loại trừ file .env và dữ liệu cào không đẩy lên Git.
* **ingestion_tool/README.md:** Tài liệu hướng dẫn sử dụng và thuyết minh kỹ thuật.

---

## 4. Yêu cầu hệ thống và Cài đặt

* **Phiên bản Python:** Yêu cầu máy cài sẵn Python từ phiên bản 3.10 trở lên.
* **Khóa API:** Cần có YouTube Data API v3 Key tạo từ Google Cloud Console.
* **Cài đặt thư viện:** Chạy lệnh `pip install -r requirements.txt` tại thư mục ingestion_tool.
* **Kiểm tra môi trường:** Đảm bảo cài đủ các gói PyQt6, pandas, pyarrow, google-api-python-client và python-dotenv.

---

## 5. Hướng dẫn vận hành

* **Bước 1 - Khởi chạy:** Mở Terminal tại thư mục ingestion_tool và gõ lệnh `python app.py`.
* **Bước 2 - Nhập API Key:** Điền key vào ô số 1 hoặc tạo file .env để app tự động nạp.
* **Bước 3 - Chọn thời gian:** Thiết lập ngày bắt đầu và kết thúc theo định dạng chuẩn YYYY-MM-DD.
* **Bước 4 - Chọn danh mục:** Đánh dấu chọn ít nhất một category muốn lấy dữ liệu.
* **Bước 5 - Thiết lập chỉ tiêu:** Nhập số lượng video mục tiêu cho mỗi tháng và số bình luận cần lấy trên mỗi video.
* **Bước 6 - Chọn định dạng và thư mục:** Chọn kiểu file (CSV, JSONL, Parquet hoặc All) và đường dẫn lưu trữ.
* **Bước 7 - Tiến hành:** Bấm nút BẮT ĐẦU CÀO DỮ LIỆU để chạy, theo dõi tiến trình qua log và bấm DỪNG LẠI nếu cần hủy.

---

## 6. Sơ đồ luồng dữ liệu

* **Nguồn cấp:** YouTube Data API v3 tiếp nhận các lượt gọi truy vấn search, videos, comments, channels.
* **Bộ xử lý trung tâm (app.py):** Lọc regex tiếng Việt -> Tính Engagement Rate -> Bóc tách comment -> Batch query kênh -> Gán lineage metadata.
* **Định dạng lưu trữ:** Ghi dữ liệu thành các tệp Parquet, JSON Lines hoặc CSV theo cấu trúc tên quy định.
* **Đích đến tầng Bronze:** Dữ liệu sẵn sàng nạp thẳng vào PySpark, Delta Lake hoặc DuckDB phục vụ chuyển đổi sang tầng Silver.