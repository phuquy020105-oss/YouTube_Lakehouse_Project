-- Tạo bảng youtube_videos lưu trữ dữ liệu thô ban đầu
CREATE TABLE IF NOT EXISTS public.youtube_videos (
    video_id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    channel_id VARCHAR(50),
    channel_title VARCHAR(255),
    category_id BIGINT,
    category_name VARCHAR(100),
    published_at VARCHAR(50),
    duration_sec BIGINT,
    video_type VARCHAR(50),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_rate FLOAT,
    top_comments TEXT,
    thumbnail_url TEXT,
    tags TEXT,
    description TEXT,
    subscriber_count BIGINT,
    channel_video_count BIGINT,
    collected_at VARCHAR(50),
    batch_id VARCHAR(50)
);