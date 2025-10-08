# Sử dụng một image Python gọn nhẹ
FROM python:3.9-slim

# Đặt thư mục làm việc bên trong container
WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg

# Sao chép file requirements trước để tận dụng Docker cache
COPY requirements.txt .

# Cài đặt các thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã nguồn của backend vào
COPY . .

# Mở cổng 5000 để container có thể nhận kết nối
EXPOSE 5000

# Lệnh để chạy ứng dụng khi container khởi động
CMD ["python", "livestream_api_worker.py"]