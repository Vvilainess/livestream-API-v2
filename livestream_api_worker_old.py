# SỬA LỖI: Thêm monkey patch cho gevent.
# Dòng này phải được đặt ở trên cùng, trước tất cả các import khác.
from gevent import monkey
monkey.patch_all()

import datetime
import uuid
import subprocess
import os
import sys
import logging
import time
import threading
import json
from flask import Flask, request
from flask_cors import CORS
from flask_socketio import SocketIO
import psutil
from collections import deque
import redis
from rq import Queue

# ==============================================================================
# CẤU HÌNH LOGGING VÀ MÔI TRƯỜNG
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
VIDEO_DIRECTORY = os.getenv('VIDEO_DIRECTORY', '/videos')
if not os.path.exists(VIDEO_DIRECTORY):
    os.makedirs(VIDEO_DIRECTORY)
logging.info(f"Thư mục video được cấu hình: {VIDEO_DIRECTORY}")

# ==============================================================================
# CẤU HÌNH FLASK, REDIS VÀ WEBSOCKET
# ==============================================================================
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent', message_queue='redis://redis:6379')

redis_conn = redis.Redis(host='redis', port=6379, db=0)
q = Queue(connection=redis_conn)
pubsub = redis_conn.pubsub()

# ==============================================================================
# BIẾN TOÀN CỤC VÀ KHÓA (LOCK)
# ==============================================================================
schedules = []
running_streams = {}
retry_start_times = {}
state_lock = threading.Lock()
background_monitor_thread = None
redis_listener_thread = None
RETRY_WINDOW_SECONDS = 180

# ==============================================================================
# FFMPEG COMMANDS
# ==============================================================================
FFMPEG_BASE_OPTIONS = "-loglevel error -hide_banner"
FFMPEG_CMD_INFINITE = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} '
    '-i "{video_file}" -c:v copy -c:a copy -f flv '
    '-nostdin "{rtmp_url}"'
)
FFMPEG_CMD_TIMED = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} '
    '-i "{video_file}" -t {duration} '
    '-c:v copy -c:a copy -f flv '
    '-nostdin "{rtmp_url}"'
)

# ==============================================================================
# CÁC HÀM TIỆN ÍCH
# ==============================================================================
def force_kill_process(pid):
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        all_procs = children + [parent]
        for proc in all_procs:
            try: proc.terminate()
            except psutil.NoSuchProcess: pass
        _, alive = psutil.wait_procs(all_procs, timeout=3)
        for proc in alive:
            try: proc.kill()
            except psutil.NoSuchProcess: pass
    except psutil.NoSuchProcess:
        pass

def get_schedule_status(schedule):
    broadcast_time_str = schedule['broadcastDateTime']
    try:
        broadcast_time = datetime.datetime.fromisoformat(broadcast_time_str.replace('Z', '+00:00'))
        current_time = datetime.datetime.now(datetime.timezone.utc)
        if schedule['status'] in ['FAILED', 'COMPLETED', 'STOPPING', 'RETRYING', 'DOWNLOADING_VIDEO', 'QUEUED_FOR_DOWNLOAD']:
            return schedule['status']
        if current_time >= broadcast_time:
            duration_minutes = schedule.get('durationMinutes')
            if duration_minutes:
                end_time = broadcast_time + datetime.timedelta(minutes=duration_minutes)
                if current_time >= end_time: return "COMPLETED"
            return "LIVE"
        return "PENDING"
    except Exception as e:
        logging.error(f"Lỗi khi lấy trạng thái lịch trình ID={schedule.get('id')}: {e}")
        return "UNKNOWN"

def is_process_running(process):
    if not process: return False
    try:
        p = psutil.Process(process.pid)
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    return True

# ==============================================================================
# LOGIC QUẢN LÝ LUỒNG FFMPEG
# ==============================================================================
def manage_ffmpeg_stream(schedule):
    schedule_id = schedule['id']
    target_status = get_schedule_status(schedule)
    if schedule['status'] in ['DOWNLOADING_VIDEO', 'QUEUED_FOR_DOWNLOAD'] or not schedule.get('videoIdentifier'): return
    if schedule['status'] != 'RETRYING' and schedule['status'] != target_status: schedule['status'] = target_status
    process = running_streams.get(schedule_id)
    if is_process_running(process):
        if schedule_id in retry_start_times:
            logging.info(f"Luồng ID={schedule_id} đã phục hồi. Hủy chu kỳ retry.")
            retry_start_times.pop(schedule_id, None)
        if target_status != 'LIVE':
            logging.info(f"Luồng ID={schedule_id} đã hết giờ/bị dừng. Dừng process...")
            force_kill_process(process.pid)
            del running_streams[schedule_id]
        return
    if schedule_id in running_streams: del running_streams[schedule_id]
    if schedule['status'] not in ['LIVE', 'RETRYING']: return
    video_filename = schedule['videoIdentifier']
    video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)
    if not os.path.isfile(video_file_path):
        schedule['status'] = 'FAILED'
        logging.error(f"LỖI: File video không tồn tại: {video_file_path}. Luồng ID={schedule_id} sẽ không bắt đầu.")
        return
    first_failure_time = retry_start_times.get(schedule_id)
    if first_failure_time:
        elapsed_time = time.time() - first_failure_time
        if elapsed_time > RETRY_WINDOW_SECONDS:
            schedule['status'] = 'FAILED'
            logging.error(f"Luồng ID={schedule_id} FAILED. Ngừng thử lại sau {RETRY_WINDOW_SECONDS} giây.")
            retry_start_times.pop(schedule_id, None)
            return
        else:
            schedule['status'] = 'RETRYING'
            logging.warning(f"Thử lại luồng ID={schedule_id}...")
    else:
        if schedule['status'] == 'PENDING':
            broadcast_time = datetime.datetime.fromisoformat(schedule['broadcastDateTime'].replace('Z', '+00:00'))
            if datetime.datetime.now(datetime.timezone.utc) >= broadcast_time:
                schedule['status'] = 'LIVE'
            else: return
        elif schedule['status'] == 'LIVE' and schedule_id not in running_streams:
            logging.info(f"Luồng ID={schedule_id} bắt đầu phát.")
        elif schedule['status'] not in ['LIVE', 'RETRYING']:
            logging.warning(f"Luồng ID={schedule_id} gặp sự cố. Bắt đầu chu kỳ thử lại.")
            retry_start_times[schedule_id] = time.time()
            schedule['status'] = 'RETRYING'
    if schedule['status'] in ['LIVE', 'RETRYING']:
        rtmp_url = f"{schedule['rtmpServer']}/{schedule['streamKey']}"
        duration_minutes = schedule.get('durationMinutes')
        command = FFMPEG_CMD_TIMED.format(video_file=video_file_path, rtmp_url=rtmp_url, duration=duration_minutes * 60) if duration_minutes else FFMPEG_CMD_INFINITE.format(video_file=video_file_path, rtmp_url=rtmp_url)
        logging.info(f"KHỞI ĐỘNG luồng ID={schedule_id} (Trạng thái: {schedule['status']})...")
        try:
            popen_kwargs = { 'shell': True, 'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE, 'universal_newlines': True }
            if os.name != 'nt': popen_kwargs['preexec_fn'] = os.setsid
            new_process = subprocess.Popen(command, **popen_kwargs)
            running_streams[schedule_id] = new_process
            socketio.sleep(2)
            if new_process.poll() is not None:
                _, stderr = new_process.communicate()
                logging.error(f"LỖI NGAY LẬP TỨC: ffmpeg cho ID={schedule_id} đã tắt. Lỗi: {stderr.strip()}")
                del running_streams[schedule_id]
                if schedule_id not in retry_start_times: retry_start_times[schedule_id] = time.time()
                schedule['status'] = 'RETRYING'
                return
            schedule['status'] = 'LIVE'
            logging.info(f"FFmpeg khởi động thành công cho ID={schedule_id} (PID: {new_process.pid})")
            retry_start_times.pop(schedule_id, None)
        except Exception as e:
            logging.exception(f"LỖI NGHIÊM TRỌNG khi chạy Popen cho ID={schedule_id}: {e}")
            if schedule_id not in retry_start_times: retry_start_times[schedule_id] = time.time()
            schedule['status'] = 'RETRYING'

# ==============================================================================
# LUỒNG GIÁM SÁT NỀN
# ==============================================================================
def background_monitor():
    """Chỉ giám sát các luồng ffmpeg, không quản lý download nữa."""
    logging.info("Bắt đầu luồng giám sát ffmpeg...")
    while True:
        try:
            with state_lock:
                schedules_copy = list(schedules)
                for schedule in schedules_copy:
                    if schedule.get('videoIdentifier'):
                        manage_ffmpeg_stream(schedule)
            socketio.emit('broadcast_update', schedules)
        except Exception as e:
            logging.exception(f"LỖI trong background_monitor: {e}")
        socketio.sleep(2)

def redis_listener():
    """Lắng nghe thông báo từ worker qua Redis Pub/Sub."""
    logging.info("Bắt đầu luồng lắng nghe Redis...")
    pubsub.subscribe('download_updates')
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                schedule_id = data.get('id')
                if not schedule_id: continue

                with state_lock:
                    schedule = next((s for s in schedules if s['id'] == schedule_id), None)
                    if not schedule: continue

                    event_type = data.get('type')
                    if event_type == 'progress':
                        progress = data.get('progress', 0)
                        schedule['download_progress'] = progress
                        socketio.emit('download_progress', {'id': schedule_id, 'progress': progress})
                    
                    elif event_type == 'complete':
                        schedule['videoIdentifier'] = data.get('filename')
                        schedule['status'] = 'PENDING'
                        schedule.pop('download_progress', None)
                        logging.info(f"Worker báo cáo tải xong cho ID={schedule_id}. File: {data.get('filename')}")
                        socketio.emit('broadcast_update', schedules)

                    elif event_type == 'fail':
                        schedule['status'] = 'FAILED'
                        schedule.pop('download_progress', None)
                        logging.error(f"Worker báo cáo tải thất bại cho ID={schedule_id}.")
                        socketio.emit('broadcast_update', schedules)

            except json.JSONDecodeError:
                logging.warning(f"Nhận được tin nhắn Redis không hợp lệ: {message['data']}")
            except Exception as e:
                logging.exception(f"Lỗi khi xử lý tin nhắn từ Redis: {e}")

# ==============================================================================
# CÁC SỰ KIỆN WEBSOCKET
# ==============================================================================
@socketio.on('connect')
def handle_connect():
    global background_monitor_thread, redis_listener_thread
    logging.info(f"Client connected: {request.sid}")
    with state_lock:
        if background_monitor_thread is None or background_monitor_thread.dead:
            logging.info("Khởi động luồng giám sát ffmpeg.")
            background_monitor_thread = socketio.start_background_task(target=background_monitor)
            
        if redis_listener_thread is None or redis_listener_thread.dead:
            logging.info("Khởi động luồng lắng nghe Redis.")
            redis_listener_thread = socketio.start_background_task(target=redis_listener)
            
    socketio.emit('broadcast_update', schedules)

@socketio.on('create_schedule')
def handle_create_schedule(data):
    video_url = data.get('videoUrl')
    if not (video_url and video_url.strip()): return {'success': False, 'error': 'Vui lòng cung cấp URL video.'}
    if not data.get('title', '').strip(): return {'success': False, 'error': 'Vui lòng nhập tiêu đề.'}
    if not data.get('broadcastDateTime'): return {'success': False, 'error': 'Vui lòng chọn ngày giờ phát.'}
    if not data.get('rtmpServer', '').strip(): return {'success': False, 'error': 'Vui lòng nhập RTMP Server.'}
    if not data.get('streamKey', '').strip(): return {'success': False, 'error': 'Vui lòng nhập Stream Key.'}

    with state_lock:
        new_schedule = {
            "id": str(uuid.uuid4()), "title": data['title'], "broadcastDateTime": data['broadcastDateTime'], 
            "rtmpServer": data['rtmpServer'], "streamKey": data['streamKey'], 
            "durationMinutes": data.get('durationMinutes'), "status": "QUEUED_FOR_DOWNLOAD", 
            "videoUrl": video_url.strip(), "download_progress": 0
        }
        schedules.append(new_schedule)
        
        q.enqueue('download_worker.download_video_task', new_schedule['id'], video_url, job_timeout='2h')
        logging.info(f"Đã giao việc tải video cho worker, ID={new_schedule['id']}")
    
    socketio.emit('broadcast_update', schedules)
    return {'success': True, 'schedule': new_schedule}

@socketio.on('stop_schedule')
def handle_stop_schedule(data):
    schedule_id = data.get('id')
    with state_lock:
        schedule = next((s for s in schedules if s['id'] == schedule_id), None)
        if schedule:
            schedule['status'] = 'STOPPING'
            if schedule_id in running_streams:
                pid = running_streams[schedule_id].pid
                logging.info(f"Dừng tiến trình FFmpeg cho lịch trình ID={schedule_id} (PID: {pid}).")
                force_kill_process(pid)
                del running_streams[schedule_id]
            retry_start_times.pop(schedule_id, None)
            
            # TODO: Hủy job trong RQ nếu nó đang ở hàng đợi.
            
            schedule['status'] = 'COMPLETED'
            socketio.emit('broadcast_update', schedules)

@socketio.on('delete_schedule')
def handle_delete_schedule(data):
    schedule_id = data.get('id')
    with state_lock:
        schedule_to_delete = next((s for s in schedules if s['id'] == schedule_id), None)
        if schedule_to_delete:
            if schedule_id in running_streams:
                force_kill_process(running_streams[schedule_id].pid)
                del running_streams[schedule_id]
            retry_start_times.pop(schedule_id, None)
            
            # TODO: Gửi tín hiệu hủy cho worker nếu job đang chạy (tính năng nâng cao)
            
            if schedule_to_delete.get('videoIdentifier'):
                file_path = os.path.join(VIDEO_DIRECTORY, schedule_to_delete['videoIdentifier'])
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logging.info(f"Đã xóa file video '{file_path}'.")
                    except Exception as e:
                        logging.error(f"Lỗi khi xóa file video '{file_path}': {e}")

        schedules[:] = [s for s in schedules if s['id'] != schedule_id]
        socketio.emit('broadcast_update', schedules)

@socketio.on('emergency_stop_all')
def handle_emergency_stop_all():
    logging.warning("Yêu cầu dừng khẩn cấp TẤT CẢ các luồng đang hoạt động.")
    with state_lock:
        schedules_copy = list(schedules)
        for schedule in schedules_copy:
            schedule_id = schedule['id']
            if schedule_id in running_streams:
                pid = running_streams[schedule_id].pid
                logging.info(f"Dừng khẩn cấp luồng ID={schedule_id} (PID: {pid}).")
                force_kill_process(pid)
            
            if schedule['status'] in ['LIVE', 'RETRYING', 'PENDING', 'DOWNLOADING_VIDEO', 'QUEUED_FOR_DOWNLOAD']:
                 schedule['status'] = 'COMPLETED'

        running_streams.clear()
        retry_start_times.clear()
        
        # TODO: Xóa hàng đợi RQ
        # q.empty()

        socketio.emit('broadcast_update', schedules)
    logging.info("Đã hoàn tất dừng khẩn cấp.")

@socketio.on('get_process_stats')
def get_process_stats():
    stats = {
        'running_streams_count': len(running_streams), 
        'schedule_pids': {}, 
        'retry_counts': {},
        'ffmpeg_processes': [], 
        'download_queue_length': q.count,
        'downloads_in_progress': q.started_job_registry.count
    }
    with state_lock:
        for schedule_id, process in running_streams.items():
            if is_process_running(process):
                stats['schedule_pids'][schedule_id] = process.pid
        for schedule_id in retry_start_times:
            stats['retry_counts'][schedule_id] = int(time.time() - retry_start_times[schedule_id])
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.cmdline()
            if 'ffmpeg' in ' '.join(cmdline):
                stats['ffmpeg_processes'].append(f"PID: {proc.pid}, CMD: {' '.join(cmdline)}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    socketio.emit('process_stats', stats)

# ==============================================================================
# ĐIỂM KHỞI CHẠY
# ==============================================================================
if __name__ == '__main__':
    logging.info("--- KHỞI ĐỘNG SERVER FLASK (API & GIAO TIẾP) ---")
    socketio.run(app, host='0.0.0.0', port=5000)

