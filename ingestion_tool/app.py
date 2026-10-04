import calendar
from datetime import datetime, timezone
import os
import re
import sys
import time

from dotenv import load_dotenv
from googleapiclient.discovery import build
import isodate
import pandas as pd
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

# Nạp file .env ngay khi khởi động
load_dotenv()

# Regex nhận diện ký tự tiếng Việt có dấu
VN_PATTERN = re.compile(
    r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđĐ]",
    re.IGNORECASE,
)

CATEGORIES_DICT = {
    "10 - Music (Âm nhạc)": "10",
    "24 - Entertainment (Giải trí)": "24",
    "20 - Gaming (Trò chơi)": "20",
    "17 - Sports (Thể thao)": "17",
    "28 - Science & Tech": "28",
    "25 - News & Politics": "25",
    "22 - People & Blogs": "22",
    "23 - Comedy (Hài)": "23",
    "1 - Film & Animation": "1",
}


def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class CrawlerWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, config):
        super().__init__()
        self.cfg = config
        self.is_running = True
        self.youtube_client = None

    def stop(self):
        self.is_running = False

    def _is_vietnamese_content(self, title, channel_title, description=""):
        full_text = f"{title} {channel_title} {description}"
        return bool(VN_PATTERN.search(full_text))

    def _get_video_top_comments(self, video_id, max_comments=15):
        try:
            request = self.youtube_client.commentThreads().list(
                part="snippet",
                videoId=video_id,
                order="relevance",
                maxResults=max_comments,
                textFormat="plainText",
            )
            response = request.execute()
            comments = []
            for item in response.get("items", []):
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                author = top_comment.get("authorDisplayName", "Anonymous")
                text = (
                    top_comment.get("textDisplay", "")
                    .replace("\n", " ")
                    .replace("\r", " ")
                    .strip()
                )
                like_count = top_comment.get("likeCount", 0)
                comments.append(f"[{author} | {like_count} likes]: {text}")
            return " || ".join(comments)
        except Exception:
            return ""

    def _get_channel_stats(self, channel_ids):
        channel_meta = {}
        unique_cids = list(set(filter(None, channel_ids)))
        for batch in chunk_list(unique_cids, 50):
            if not self.is_running:
                break
            try:
                req = self.youtube_client.channels().list(
                    part="statistics", id=",".join(batch), maxResults=50
                )
                res = req.execute()
                for item in res.get("items", []):
                    cid = item["id"]
                    stats = item.get("statistics", {})
                    channel_meta[cid] = {
                        "subscriber_count": int(stats.get("subscriberCount", 0)),
                        "channel_video_count": int(stats.get("videoCount", 0)),
                    }
                time.sleep(0.05)
            except Exception as e:
                self.log_signal.emit(f"   [!] Lỗi lấy thông tin kênh: {e}")
        return channel_meta

    def _get_video_ids_in_month(
        self, category_id, published_after, published_before, target_count=50
    ):
        video_ids = []
        next_page_token = None
        query_hint = "Việt Nam" if category_id == "24" else "nhạc Việt"
        needed_fetch = max(target_count * 2, target_count + 60)

        while len(video_ids) < needed_fetch and self.is_running:
            fetch_limit = min(50, needed_fetch - len(video_ids))
            try:
                request = self.youtube_client.search().list(
                    part="id",
                    type="video",
                    q=query_hint,
                    videoCategoryId=category_id,
                    regionCode="VN",
                    relevanceLanguage="vi",
                    order="viewCount",
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    maxResults=fetch_limit,
                    pageToken=next_page_token,
                )
                response = request.execute()
            except Exception as e:
                self.log_signal.emit(f"Lỗi Search API (Cat {category_id}): {e}")
                break

            items = response.get("items", [])
            if not items:
                break
            for item in items:
                if item["id"]["kind"] == "youtube#video":
                    video_ids.append(item["id"]["videoId"])

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        return video_ids

    def _get_video_details(
        self, video_ids, target_count=50, top_comments_count=15
    ):
        if not video_ids:
            return []
        parsed_videos = []
        for i in range(0, len(video_ids), 50):
            if not self.is_running or len(parsed_videos) >= target_count:
                break
            batch_ids = video_ids[i : i + 50]
            try:
                request = self.youtube_client.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=",".join(batch_ids),
                )
                response = request.execute()
            except Exception as e:
                self.log_signal.emit(f"Lỗi Video Details API: {e}")
                continue

            for item in response.get("items", []):
                if len(parsed_videos) >= target_count:
                    break
                snippet = item.get("snippet", {})
                title = snippet.get("title", "")
                channel_title = snippet.get("channelTitle", "")
                description = snippet.get("description", "")

                if not self._is_vietnamese_content(
                    title, channel_title, description
                ):
                    continue

                v_id = item["id"]
                stats = item.get("statistics", {})
                details = item.get("contentDetails", {})

                tags = " | ".join(snippet.get("tags", []))
                clean_desc = description.replace("\n", " ").strip()[:500]

                duration_iso = details.get("duration", "PT0S")
                try:
                    duration_sec = int(
                        isodate.parse_duration(duration_iso).total_seconds()
                    )
                except Exception:
                    duration_sec = 0

                views = int(stats.get("viewCount", 0))
                likes = int(stats.get("likeCount", 0))
                comments = int(stats.get("commentCount", 0))
                er = (
                    round(((likes + comments) / views * 100), 2)
                    if views > 0
                    else 0.0
                )

                comments_content = self._get_video_top_comments(
                    v_id, max_comments=top_comments_count
                )

                parsed_videos.append(
                    {
                        "video_id": v_id,
                        "title": title,
                        "channel_id": snippet.get("channelId", ""),
                        "channel_title": channel_title,
                        "category_id": snippet.get("categoryId", ""),
                        "published_at": snippet.get("publishedAt", ""),
                        "description": clean_desc,
                        "tags": tags,
                        "duration_sec": duration_sec,
                        "views": views,
                        "likes": likes,
                        "comments": comments,
                        "engagement_rate": er,
                        "top_comments": comments_content,
                        "thumbnail_url": snippet.get("thumbnails", {})
                        .get("high", {})
                        .get("url", ""),
                    }
                )
        return parsed_videos

    def run(self):
        try:
            self.youtube_client = build(
                "youtube", "v3", developerKey=self.cfg["api_key"]
            )
            start_dt = datetime.strptime(self.cfg["start_date"], "%Y-%m-%d")
            end_dt = datetime.strptime(self.cfg["end_date"], "%Y-%m-%d")

            # Khởi tạo Metadata Lineage cho đợt Ingestion này
            run_timestamp = datetime.now(timezone.utc)
            batch_id = f"BATCH_{run_timestamp.strftime('%Y%m%d_%H%M%S')}"

            current_year = start_dt.year
            current_month = start_dt.month
            total_months = (
                (end_dt.year - start_dt.year) * 12
                + (end_dt.month - start_dt.month)
                + 1
            )
            processed_months = 0

            os.makedirs(self.cfg["out_dir"], exist_ok=True)
            self.log_signal.emit("=== KHỞI CHẠY BRONZE INGESTION PIPELINE ===")
            self.log_signal.emit(f"Data Lineage Batch ID: {batch_id}")
            self.log_signal.emit(f"Ingested Time (UTC): {run_timestamp.isoformat()}")

            while self.is_running:
                loop_dt = datetime(current_year, current_month, 1)
                if loop_dt > datetime(end_dt.year, end_dt.month, 1):
                    break

                _, last_day = calendar.monthrange(current_year, current_month)
                day_from = (
                    start_dt.day
                    if (
                        current_year == start_dt.year
                        and current_month == start_dt.month
                    )
                    else 1
                )
                day_to = (
                    end_dt.day
                    if (
                        current_year == end_dt.year
                        and current_month == end_dt.month
                    )
                    else last_day
                )

                pub_after = f"{current_year}-{current_month:02d}-{day_from:02d}T00:00:00Z"
                pub_before = f"{current_year}-{current_month:02d}-{day_to:02d}T23:59:59Z"

                self.log_signal.emit(
                    f"\n>>> Chu kỳ tháng: {current_month:02d}/{current_year} ({pub_after[:10]} -> {pub_before[:10]})"
                )

                for cat_id in self.cfg["categories"]:
                    if not self.is_running:
                        break

                    self.log_signal.emit(f"-> Quét danh mục ID: {cat_id}...")
                    ids = self._get_video_ids_in_month(
                        cat_id,
                        pub_after,
                        pub_before,
                        target_count=self.cfg["target_videos"],
                    )
                    self.log_signal.emit(
                        f"   + Tìm thấy {len(ids)} video thô. Đang lấy chi tiết video & comments..."
                    )

                    data = self._get_video_details(
                        ids,
                        target_count=self.cfg["target_videos"],
                        top_comments_count=self.cfg["target_cmts"],
                    )

                    if data:
                        df = pd.DataFrame(data)

                        # Bổ sung thông tin kênh
                        self.log_signal.emit(
                            f"   + Làm giàu thông tin ({len(df['channel_id'].unique())} kênh)..."
                        )
                        c_meta = self._get_channel_stats(df["channel_id"].tolist())
                        df["subscriber_count"] = df["channel_id"].map(
                            lambda cid: c_meta.get(cid, {}).get("subscriber_count", 0)
                        )
                        df["channel_video_count"] = df["channel_id"].map(
                            lambda cid: c_meta.get(cid, {}).get("channel_video_count", 0)
                        )

                        # Gán Metadata tầng Bronze (Data Lineage & Traceability)
                        df["ingested_at"] = run_timestamp.isoformat()
                        df["batch_id"] = batch_id

                        base = f"youtube_category_{cat_id}_{current_year}_{current_month:02d}"
                        fmt = self.cfg["format"]

                        if "CSV" in fmt or "All" in fmt:
                            p = os.path.join(self.cfg["out_dir"], f"{base}.csv")
                            df.to_csv(p, index=False, encoding="utf-8-sig")
                            self.log_signal.emit(
                                f"   [Đã lưu CSV] {os.path.basename(p)} ({len(df)} dòng)"
                            )

                        if "JSON" in fmt or "All" in fmt:
                            p = os.path.join(self.cfg["out_dir"], f"{base}.jsonl")
                            df.to_json(
                                p,
                                orient="records",
                                lines=True,
                                force_ascii=False,
                            )
                            self.log_signal.emit(
                                f"   [Đã lưu JSON] {os.path.basename(p)} ({len(df)} records)"
                            )

                        if "Parquet" in fmt or "All" in fmt:
                            p = os.path.join(self.cfg["out_dir"], f"{base}.parquet")
                            df.to_parquet(p, index=False, engine="pyarrow")
                            self.log_signal.emit(
                                f"   [Đã lưu Parquet] {os.path.basename(p)} ({len(df)} records)"
                            )
                    else:
                        self.log_signal.emit(
                            f"   [!] Không tìm thấy dữ liệu đạt chuẩn cho Cat {cat_id}."
                        )
                    time.sleep(0.3)

                processed_months += 1
                self.progress_signal.emit(
                    int((processed_months / max(1, total_months)) * 100)
                )

                if current_month == 12:
                    current_month = 1
                    current_year += 1
                else:
                    current_month += 1
                time.sleep(0.5)

            if self.is_running:
                self.finished_signal.emit(
                    True, f"Đã hoàn thành Ingestion đợt [{batch_id}]!"
                )
            else:
                self.finished_signal.emit(False, "Tiến trình đã bị dừng lại.")
        except Exception as e:
            self.finished_signal.emit(False, str(e))


class MainWindow(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle(
            "YouTube Lakehouse Ingestion Engine - Data Engineering Tool"
        )
        self.resize(1000, 720)
        self.worker = None
        self._init_ui()

    def _init_ui(self):
        main_layout = QVBoxLayout(self)

        title_box = QGroupBox()
        title_vbox = QVBoxLayout()
        lbl_title = QLabel("YOUTUBE DATA INGESTION ENGINE")
        lbl_title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        lbl_sub = QLabel(
            "Lakehouse Bronze/Raw Ingestion Pipeline | PySpark & Delta Lake Ready"
        )
        lbl_sub.setStyleSheet("color: #888;")
        title_vbox.addWidget(lbl_title)
        title_vbox.addWidget(lbl_sub)
        title_box.setLayout(title_vbox)
        main_layout.addWidget(title_box)

        body_layout = QHBoxLayout()

        cfg_box = QGroupBox("Cấu hình Pipeline")
        cfg_layout = QVBoxLayout()

        # 1. API Key (Tự động đọc từ file .env nếu có)
        cfg_layout.addWidget(QLabel("1. YouTube API v3 Key:"))
        self.txt_api = QLineEdit()
        self.txt_api.setEchoMode(QLineEdit.EchoMode.Password)
        self.txt_api.setPlaceholderText("Dán API Key hoặc đọc tự động từ .env...")
        
        env_key = os.getenv("YOUTUBE_API_KEY", "").strip()
        if env_key:
            self.txt_api.setText(env_key)
            
        cfg_layout.addWidget(self.txt_api)

        # 2. Thời gian
        cfg_layout.addWidget(QLabel("2. Khoảng thời gian (YYYY-MM-DD):"))
        date_h = QHBoxLayout()
        self.txt_start = QLineEdit("2023-01-01")
        self.txt_end = QLineEdit("2023-03-31")
        date_h.addWidget(self.txt_start)
        date_h.addWidget(QLabel("->"))
        date_h.addWidget(self.txt_end)
        cfg_layout.addLayout(date_h)

        # 3. Categories
        cfg_layout.addWidget(QLabel("3. Chọn Danh mục Video:"))
        self.cat_checks = {}
        for name, cid in CATEGORIES_DICT.items():
            cb = QCheckBox(name)
            if cid in ["10", "24"]:
                cb.setChecked(True)
            self.cat_checks[cid] = cb
            cfg_layout.addWidget(cb)

        # 4. Quotas
        cfg_layout.addWidget(QLabel("4. Chỉ tiêu số lượng:"))
        q_form = QFormLayout()
        self.txt_videos = QLineEdit("50")
        self.txt_cmts = QLineEdit("15")
        q_form.addRow("Video/Tháng/Cat:", self.txt_videos)
        q_form.addRow("Top comments/Video:", self.txt_cmts)
        cfg_layout.addLayout(q_form)

        # 5. Xuất file
        cfg_layout.addWidget(QLabel("5. Định dạng & Thư mục đích:"))
        self.cb_format = QComboBox()
        self.cb_format.addItems(
            [
                "CSV (.csv)",
                "JSON Lines (.jsonl)",
                "Parquet (.parquet)",
                "All (Cả 3 loại)",
            ]
        )
        cfg_layout.addWidget(self.cb_format)

        dir_h = QHBoxLayout()
        self.txt_dir = QLineEdit(os.path.abspath("./youtube_raw_data"))
        btn_browse = QPushButton("Chọn...")
        btn_browse.clicked.connect(self._browse_dir)
        dir_h.addWidget(self.txt_dir)
        dir_h.addWidget(btn_browse)
        cfg_layout.addLayout(dir_h)

        cfg_box.setLayout(cfg_layout)
        body_layout.addWidget(cfg_box, stretch=1)

        right_box = QGroupBox("Execution Monitor")
        right_layout = QVBoxLayout()
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setFont(QFont("Courier", 11))
        right_layout.addWidget(self.txt_log)

        self.progress = QProgressBar()
        self.progress.setValue(0)
        right_layout.addWidget(self.progress)
        right_box.setLayout(right_layout)
        body_layout.addWidget(right_box, stretch=2)

        main_layout.addLayout(body_layout)

        btn_h = QHBoxLayout()
        self.btn_run = QPushButton("BẮT ĐẦU CÀO DỮ LIỆU")
        self.btn_run.setFixedHeight(40)
        self.btn_run.setStyleSheet(
            "background-color: #0d6efd; color: white; font-weight: bold;"
        )
        self.btn_run.clicked.connect(self._start)

        self.btn_stop = QPushButton("DỪNG LẠI")
        self.btn_stop.setFixedHeight(40)
        self.btn_stop.setEnabled(False)
        self.btn_stop.setStyleSheet(
            "background-color: #dc3545; color: white; font-weight: bold;"
        )
        self.btn_stop.clicked.connect(self._stop)

        btn_h.addWidget(self.btn_run, stretch=4)
        btn_h.addWidget(self.btn_stop, stretch=1)
        main_layout.addLayout(btn_h)

    def _browse_dir(self):
        f = QFileDialog.getExistingDirectory(
            self, "Chọn thư mục", os.getcwd()
        )
        if f:
            self.txt_dir.setText(f)

    def _log(self, text):
        now = datetime.now().strftime("%H:%M:%S")
        self.txt_log.append(f"[{now}] {text}")

    def _start(self):
        api_key = self.txt_api.text().strip()
        if not api_key:
            QMessageBox.critical(
                self, "Lỗi", "Vui lòng nhập YouTube API Key hợp lệ (hoặc cấu hình trong file .env)!"
            )
            return

        cats = [
            cid for cid, cb in self.cat_checks.items() if cb.isChecked()
        ]
        if not cats:
            QMessageBox.warning(
                self, "Cảnh báo", "Hãy chọn ít nhất 1 danh mục!"
            )
            return

        config = {
            "api_key": api_key,
            "start_date": self.txt_start.text().strip(),
            "end_date": self.txt_end.text().strip(),
            "categories": cats,
            "target_videos": int(self.txt_videos.text().strip() or 50),
            "target_cmts": int(self.txt_cmts.text().strip() or 15),
            "format": self.cb_format.currentText(),
            "out_dir": self.txt_dir.text().strip(),
        }

        self.btn_run.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.progress.setValue(0)
        self.txt_log.clear()

        self.worker = CrawlerWorker(config)
        self.worker.log_signal.connect(self._log)
        self.worker.progress_signal.connect(self.progress.setValue)
        self.worker.finished_signal.connect(self._finished)
        self.worker.start()

    def _stop(self):
        if self.worker:
            self.worker.stop()
            self.btn_stop.setEnabled(False)
            self._log("[Đang gửi tín hiệu dừng tới worker...]")

    def _finished(self, success, msg):
        self.btn_run.setEnabled(True)
        self.btn_stop.setEnabled(False)
        if success:
            self._log(f"\n=== {msg} ===")
            QMessageBox.information(self, "Hoàn tất", msg)
        else:
            self._log(f"\n[DỪNG/LỖI]: {msg}")
            QMessageBox.warning(self, "Thông báo", msg)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())