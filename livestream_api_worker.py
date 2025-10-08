# Thêm monkey patch cho gevent. Phải đặt ở trên cùng.
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
import redis
from rq import Queue, Worker

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

redis_conn = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
q = Queue(connection=redis_conn)
pubsub = redis_conn.pubsub()

# ==============================================================================
# BIẾN TOÀN CỤC (CHỈ DÀNH CHO CÁC TIẾN TRÌNH CỤC BỘ)
# ==============================================================================
running_streams = {}
retry_start_times = {}
state_lock = threading.Lock()
background_monitor_thread = None
redis_listener_thread = None
RETRY_WINDOW_SECONDS = 180

# ==============================================================================
# CÁC HÀM TƯƠNG TÁC VỚI REDIS (QUẢN LÝ TRẠNG THÁI)
# ==============================================================================
def get_current_process_stats():
    # 1. Thống kê FFMPEG (Tiến trình đang chạy)
    running_streams_count = len(running_streams)
    schedule_pids = {sid: proc.pid for sid, proc in running_streams.items()}
    retry_counts = {sid: round(time.time() - start_time) for sid, start_time in retry_start_times.items()}

    # 2. Thống kê RQ (Hàng đợi tải video)
    try:
        # Lấy độ dài hàng đợi
        download_queue_length = q.count
        
        # Lấy thông tin workers và jobs đang thực hiện
        active_workers = Worker.all(connection=redis_conn)
        downloads_in_progress = []
        for worker in active_workers:
            if worker.get_current_job():
                job = worker.get_current_job()
                # Job ID là ID lịch trình
                downloads_in_progress.append(job.args[0])
    except Exception as e:
        logging.error(f"Lỗi khi lấy thống kê RQ: {e}")
        download_queue_length = -1
        downloads_in_progress = ["ERROR"]
    
    # 3. Lấy danh sách các tiến trình FFMPEG (Output thô)
    ffmpeg_processes = []
    try:
        # Sử dụng psutil để tìm các tiến trình con của API Worker có chứa 'ffmpeg'
        current_process = psutil.Process(os.getpid())
        for child in current_process.children(recursive=True):
            try:
                cmdline = ' '.join(child.cmdline())
                if 'ffmpeg' in cmdline:
                    ffmpeg_processes.append(cmdline)
            except psutil.NoSuchProcess:
                continue
    except Exception as e:
        logging.error(f"Lỗi khi lấy danh sách tiến trình FFMPEG: {e}")
        ffmpeg_processes = ["ERROR: Could not fetch process list."]
        
    return {
        'running_streams_count': running_streams_count,
        'schedule_pids': schedule_pids,
        'retry_counts': retry_counts,
        'download_queue_length': download_queue_length,
        'downloads_in_progress': downloads_in_progress,
        'ffmpeg_processes': ffmpeg_processes
    }
    
def save_schedule(schedule_data):
    key = f"schedule:{schedule_data['id']}"
    # Chuyển đổi các giá trị không phải chuỗi sang JSON để lưu vào Redis Hash
    pipeline = redis_conn.pipeline()
    for field, value in schedule_data.items():
        if not isinstance(value, str):
            pipeline.hset(key, field, json.dumps(value))
        else:
            pipeline.hset(key, field, value)
    pipeline.execute()

def get_schedule(schedule_id):
    key = f"schedule:{schedule_id}"
    schedule_data = redis_conn.hgetall(key)
    if not schedule_data:
        return None
    # Chuyển đổi các giá trị JSON về lại kiểu Python
    for field, value in schedule_data.items():
        try:
            schedule_data[field] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            # Bỏ qua nếu không phải là JSON hợp lệ
            pass
    return schedule_data

def get_all_schedules():
    """Lấy tất cả các lịch trình từ Redis một cách hiệu quả."""
    keys = redis_conn.keys("schedule:*")
    schedules = []
    if not keys:
        return schedules
    pipeline = redis_conn.pipeline()
    for key in keys:
        pipeline.hgetall(key)
    results = pipeline.execute()
    for schedule_data in results:
        if not schedule_data: continue
        for field, value in schedule_data.items():
            try:
                schedule_data[field] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass
        schedules.append(schedule_data)
    return schedules

def delete_schedule_from_redis(schedule_id):
    key = f"schedule:{schedule_id}"
    redis_conn.delete(key)

# ==============================================================================
# FFMPEG COMMANDS
# ==============================================================================
FFMPEG_BASE_OPTIONS = "-loglevel error -hide_banner"
FFMPEG_CMD_INFINITE = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} -analyzeduration 20M -probesize 10M '
    '-i "{video_file}" -c:v copy -c:a copy -f flv -nostdin "{rtmp_url}"'
)
FFMPEG_CMD_TIMED = (
    f'ffmpeg -re -stream_loop -1 {FFMPEG_BASE_OPTIONS} -analyzeduration 20M -probesize 10M '
    '-i "{video_file}" -t {duration} -c:v copy -c:a copy -f flv -nostdin "{rtmp_url}"'
)

# ==============================================================================
# CÁC HÀM TIỆN ÍCH KHÁC
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
    except psutil.NoSuchProcess: pass

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
    except psutil.NoSuchProcess: return False
    return True

# ==============================================================================
# LOGIC QUẢN LÝ LUỒNG FFMPEG
# ==============================================================================
def manage_ffmpeg_stream(schedule):
    schedule_id = schedule['id']
    original_status = schedule['status']
    target_status = get_schedule_status(schedule)
    
    if schedule.get('status') in ['DOWNLOADING_VIDEO', 'QUEUED_FOR_DOWNLOAD'] or not schedule.get('videoIdentifier'): return

    if schedule['status'] != 'RETRYING' and schedule['status'] != target_status:
        schedule['status'] = target_status

    process = running_streams.get(schedule_id)
    if is_process_running(process):
        if schedule_id in retry_start_times:
            logging.info(f"Luồng ID={schedule_id} đã phục hồi.")
            with state_lock: retry_start_times.pop(schedule_id, None)
        if target_status != 'LIVE':
            logging.info(f"Luồng ID={schedule_id} đã hết giờ/bị dừng. Dừng process...")
            force_kill_process(process.pid)
            with state_lock: del running_streams[schedule_id]
        if original_status != schedule['status']: save_schedule(schedule)
        return

    if schedule_id in running_streams:
        with state_lock: del running_streams[schedule_id]

    if schedule['status'] not in ['LIVE', 'RETRYING']:
        if original_status != schedule['status']: save_schedule(schedule)
        return

    video_filename = schedule['videoIdentifier']
    video_file_path = os.path.join(VIDEO_DIRECTORY, video_filename)
    if not os.path.isfile(video_file_path):
        schedule['status'] = 'FAILED'
        logging.error(f"Lỗi: File video không tồn tại: {video_file_path}.")
        save_schedule(schedule)
        return

    first_failure_time = retry_start_times.get(schedule_id)
    if first_failure_time and (time.time() - first_failure_time > RETRY_WINDOW_SECONDS):
        schedule['status'] = 'FAILED'
        logging.error(f"Luồng ID={schedule_id} FAILED. Ngừng thử lại.")
        with state_lock: retry_start_times.pop(schedule_id, None)
        save_schedule(schedule)
        return
    
    rtmp_url = f"{schedule['rtmpServer']}/{schedule['streamKey']}"
    duration_minutes = schedule.get('durationMinutes')
    command = FFMPEG_CMD_TIMED.format(video_file=video_file_path, rtmp_url=rtmp_url, duration=duration_minutes * 60) if duration_minutes else FFMPEG_CMD_INFINITE.format(video_file=video_file_path, rtmp_url=rtmp_url)
    
    logging.info(f"Khởi động luồng ID={schedule_id} (Trạng thái: {schedule['status']})...")
    try:
        popen_kwargs = { 'shell': True, 'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE, 'universal_newlines': True }
        if os.name != 'nt': popen_kwargs['preexec_fn'] = os.setsid
        
        new_process = subprocess.Popen(command, **popen_kwargs)
        with state_lock: running_streams[schedule_id] = new_process
        socketio.sleep(3) # Tăng thời gian chờ để ffmpeg ổn định

        if new_process.poll() is not None:
            _, stderr = new_process.communicate()
            logging.error(f"Lỗi ngay lập tức: ffmpeg cho ID={schedule_id} đã tắt. Lỗi: {stderr.strip()}")
            schedule['status'] = 'RETRYING'
            with state_lock:
                del running_streams[schedule_id]
                if schedule_id not in retry_start_times: retry_start_times[schedule_id] = time.time()
        else:
            schedule['status'] = 'LIVE'
            logging.info(f"FFmpeg khởi động thành công cho ID={schedule_id} (PID: {new_process.pid})")
            with state_lock: retry_start_times.pop(schedule_id, None)
    except Exception as e:
        logging.exception(f"Lỗi nghiêm trọng khi chạy Popen cho ID={schedule_id}: {e}")
        schedule['status'] = 'RETRYING'
        with state_lock:
            if schedule_id not in retry_start_times: retry_start_times[schedule_id] = time.time()
    
    if original_status != schedule['status']:
        save_schedule(schedule)

# ==============================================================================
# LUỒNG GIÁM SÁT NỀN
# ==============================================================================
def background_monitor():
    logging.info("Bắt đầu luồng giám sát ffmpeg...")
    while True:
        try:
            current_schedules = get_all_schedules()
            for schedule in current_schedules:
                if schedule.get('videoIdentifier') and schedule.get('status') not in ['COMPLETED', 'FAILED']:
                    manage_ffmpeg_stream(schedule)
            socketio.emit('broadcast_update', current_schedules)
        except Exception as e:
            logging.exception(f"Lỗi trong background_monitor: {e}")
        socketio.sleep(2)

def redis_listener():
    logging.info("Bắt đầu luồng lắng nghe Redis...")
    pubsub.subscribe('download_updates')
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                schedule_id = data.get('id')
                if not schedule_id: continue

                schedule = get_schedule(schedule_id)
                if not schedule: continue

                event_type = data.get('type')
                event_type = data.get('type')
                if event_type == 'progress':
                    # KIỂM TRA VÀ CẬP NHẬT TRẠNG THÁI NẾU CẦN
                    if schedule.get('status') == 'QUEUED_FOR_DOWNLOAD':
                        schedule['status'] = 'DOWNLOADING_VIDEO'
                        logging.info(f"Trạng thái của ID={schedule_id} đã chuyển sang DOWNLOADING_VIDEO.")
                    
                    schedule['download_progress'] = data.get('progress', 0)
                    save_schedule(schedule)
                    
                    # Gửi cả hai thông tin cập nhật cùng lúc
                    socketio.emit('download_progress', {'id': schedule_id, 'progress': schedule['download_progress']})
                    socketio.emit('broadcast_update', get_all_schedules()) # Gửi trạng thái mới cho client
                
                elif event_type == 'complete':
                    schedule['videoIdentifier'] = data.get('filename')
                    schedule['status'] = 'PENDING'
                    schedule.pop('download_progress', None)
                    save_schedule(schedule)
                    logging.info(f"Worker báo cáo tải xong cho ID={schedule_id}.")
                    socketio.emit('broadcast_update', get_all_schedules())

                elif event_type == 'fail':
                    schedule['status'] = 'FAILED'
                    schedule.pop('download_progress', None)
                    save_schedule(schedule)
                    logging.error(f"Worker báo cáo tải thất bại cho ID={schedule_id}.")
                    socketio.emit('broadcast_update', get_all_schedules())

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
    socketio.emit('broadcast_update', get_all_schedules())

@socketio.on('create_schedule')
def handle_create_schedule(data):
    if not data.get('videoUrl',"").strip(): return {'success': False, 'error': 'Vui lòng cung cấp URL video.'}
    if not data.get('title',"").strip(): return {'success': False, 'error': 'Vui lòng nhập tiêu đề.'}
    if not data.get('broadcastDateTime'): return {'success': False, 'error': 'Vui lòng chọn ngày giờ phát.'}
    if not data.get('rtmpServer',"").strip(): return {'success': False, 'error': 'Vui lòng nhập RTMP Server.'}
    if not data.get('streamKey',"").strip(): return {'success': False, 'error': 'Vui lòng nhập Stream Key.'}
    
    new_schedule = {
        "id": str(uuid.uuid4()), "title": data['title'], "broadcastDateTime": data['broadcastDateTime'], 
        "rtmpServer": data['rtmpServer'], "streamKey": data['streamKey'], 
        "durationMinutes": data.get('durationMinutes'), "status": "QUEUED_FOR_DOWNLOAD", 
        "videoUrl": data.get('videoUrl',"").strip(), "download_progress": 0
    }
    save_schedule(new_schedule)
    q.enqueue('download_worker.download_video_task', new_schedule['id'], new_schedule['videoUrl'], job_timeout='2h')
    logging.info(f"Đã giao việc tải video cho worker, ID={new_schedule['id']}")
    socketio.emit('broadcast_update', get_all_schedules())
    return {'success': True, 'schedule': new_schedule}
    
@socketio.on('stop_schedule')
def handle_stop_schedule(data):
    schedule_id = data.get('id')
    if not schedule_id:
        return {'success': False, 'error': 'Không có ID lịch trình.'}

    logging.info(f"Nhận yêu cầu dừng luồng cho ID={schedule_id}...")
    
    # Lấy thông tin lịch trình từ Redis
    schedule = get_schedule(schedule_id)
    if not schedule:
        logging.warning(f"Không tìm thấy lịch trình ID={schedule_id} để dừng.")
        return {'success': False, 'error': 'Không tìm thấy lịch trình.'}

    with state_lock:
        # 1. Dừng tiến trình FFMPEG nếu nó đang chạy
        if schedule_id in running_streams:
            process = running_streams[schedule_id]
            logging.info(f"Dừng tiến trình FFMPEG cho lịch trình ID={schedule_id} (PID: {process.pid}).")
            force_kill_process(process.pid)
            del running_streams[schedule_id]
        else:
            logging.info(f"Không có tiến trình FFMPEG nào đang chạy trong bộ nhớ cho ID={schedule_id}.")

        # 2. Xóa khỏi danh sách retry (nếu có)
        retry_start_times.pop(schedule_id, None)

    # 3. Cập nhật trạng thái trong Redis
    schedule['status'] = 'COMPLETED'
    save_schedule(schedule)
    logging.info(f"Đã cập nhật trạng thái của ID={schedule_id} thành COMPLETED.")

    # 4. Gửi cập nhật cho tất cả client
    socketio.emit('broadcast_update', get_all_schedules())
    return {'success': True}

@socketio.on('delete_schedule')
def handle_delete_schedule(data):
    schedule_id = data.get('id')
    schedule_to_delete = get_schedule(schedule_id)
    if schedule_to_delete:
        with state_lock:
            if schedule_id in running_streams:
                force_kill_process(running_streams[schedule_id].pid)
                del running_streams[schedule_id]
            retry_start_times.pop(schedule_id, None)
        
        if schedule_to_delete.get('videoIdentifier'):
            try:
                file_path = os.path.join(VIDEO_DIRECTORY, schedule_to_delete['videoIdentifier'])
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logging.info(f"Đã xóa file video '{file_path}'.")
            except Exception as e:
                logging.error(f"Lỗi khi xóa file video: {e}")

        delete_schedule_from_redis(schedule_id)
        socketio.emit('broadcast_update', get_all_schedules())

@socketio.on('emergency_stop_all')
def handle_emergency_stop_all():
    logging.warning("Yêu cầu dừng khẩn cấp TẤT CẢ các luồng.")
    with state_lock:
        for schedule_id, process in running_streams.items():
            logging.info(f"Dừng khẩn cấp luồng ID={schedule_id} (PID: {process.pid}).")
            force_kill_process(process.pid)
        running_streams.clear()
        retry_start_times.clear()

    # Xóa tất cả các job đang chờ trong hàng đợi
    q.empty()
    logging.info("Đã xóa tất cả các job đang chờ trong hàng đợi download.")

    all_schedules = get_all_schedules()
    for schedule in all_schedules:
        if schedule['status'] not in ['COMPLETED', 'FAILED']:
            schedule['status'] = 'COMPLETED'
            save_schedule(schedule)
            
    socketio.emit('broadcast_update', get_all_schedules())
    logging.info("Đã hoàn tất dừng khẩn cấp.")
    
@socketio.on('get_process_stats')
def handle_get_process_stats():
    """Thu thập và gửi thống kê debug về cho client."""
    stats = get_current_process_stats()
    socketio.emit('process_stats', stats, room=request.sid)

# ==============================================================================
# ĐIỂM KHỞI CHẠY
# ==============================================================================
if __name__ == '__main__':
    # Kiểm tra kết nối Redis trước khi chạy
    try:
        redis_conn.ping()
        logging.info("Kết nối Redis thành công.")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Không thể kết nối tới Redis: {e}")
        sys.exit(1)

    logging.info("--- KHỞI ĐỘNG SERVER FLASK (API & GIAO TIẾP) ---")
    socketio.run(app, host='0.0.0.0', port=5000)

