import os
import sys
import logging
import re
import io
import json
import time

# Imports for Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import redis
from rq import Worker, Connection, Queue

# ==============================================================================
# CẤU HÌNH LOGGING VÀ MÔI TRƯỜNG
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER - %(levelname)s - %(message)s', stream=sys.stdout)
VIDEO_DIRECTORY = os.getenv('VIDEO_DIRECTORY', '/videos')
if not os.path.exists(VIDEO_DIRECTORY):
    os.makedirs(VIDEO_DIRECTORY)
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')

# Kết nối tới Redis bằng tên service 'redis' trong Docker
redis_conn = redis.Redis(host='redis', port=6379, db=0)

# ==============================================================================
# LOGIC TẢI VIDEO (CHẠY TRONG WORKER)
# ==============================================================================
def get_google_drive_file_id(url):
    """Trích xuất File ID từ các định dạng URL của Google Drive."""
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if match: return match.group(1)
    match = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    if match: return match.group(1)
    return None

def publish_update(schedule_id, data):
    data['id'] = schedule_id
    redis_conn.publish('download_updates', json.dumps(data))

def download_video_from_google_api(file_id, output_path, schedule_id):
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(output_path, 'wb')
        
        logging.info(f"Worker bắt đầu tải file Google Drive ID: {file_id}...")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        last_reported_progress = -1

        while done is False:
            status, done = downloader.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                if progress > last_reported_progress + 4:
                    last_reported_progress = progress
                    publish_update(schedule_id, {'type': 'progress', 'progress': progress})

        publish_update(schedule_id, {'type': 'progress', 'progress': 100})
        logging.info(f"Worker tải xong file: {output_path}")
        return True
    except Exception as e:
        logging.error(f"LỖI Worker khi tải file ID {file_id}: {e}")
        return False

def download_video_task(schedule_id, video_url):
    logging.info(f"Nhận việc tải video cho ID: {schedule_id}")
    
    file_id = get_google_drive_file_id(video_url)
    if not file_id:
        logging.error(f"URL không hợp lệ: {video_url}")
        publish_update(schedule_id, {'type': 'fail'})
        return

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logging.error(f"Thiếu tệp credentials '{SERVICE_ACCOUNT_FILE}'.")
        publish_update(schedule_id, {'type': 'fail'})
        return

    output_filename = f"{schedule_id}_video.mp4"
    output_path = os.path.join(VIDEO_DIRECTORY, output_filename)
    
    # Thử tải với cơ chế retry
    max_retries = 5
    retry_count = 0
    initial_retry_delay_seconds = 60
    success = False

    while retry_count < max_retries:
        if download_video_from_google_api(file_id, output_path, schedule_id):
            success = True
            break
        
        retry_count += 1
        if retry_count < max_retries:
            delay = initial_retry_delay_seconds * (2 ** (retry_count - 1))
            logging.warning(f"Tải thất bại cho ID={schedule_id}. Thử lại sau {delay} giây...")
            time.sleep(delay)

    if success:
        publish_update(schedule_id, {'type': 'complete', 'filename': output_filename})
    else:
        logging.error(f"Tải thất bại cho ID={schedule_id} sau {max_retries} lần thử.")
        publish_update(schedule_id, {'type': 'fail'})

# ==============================================================================
# ĐIỂM KHỞI CHẠY CỦA WORKER
# ==============================================================================
if __name__ == '__main__':
    listen = ['default']
    # Không cần time.sleep(5) nữa vì docker-compose sẽ xử lý healthcheck
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        logging.info(f"--- WORKER ĐANG LẮNG NGHE TRÊN HÀNG ĐỢI '{listen[0]}' ---")
        worker.work()

