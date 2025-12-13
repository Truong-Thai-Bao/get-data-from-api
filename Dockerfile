# Sử dụng image gốc của Airflow
FROM apache/airflow:2.6.0-python3.9

# Copy file requirements vào trong ảnh
COPY requirements.txt /requirements.txt

# Chuyển sang user root để cài đặt
USER airflow

# Cài đặt thư viện NGAY KHI BUILD (Chỉ làm 1 lần duy nhất)
RUN pip install --no-cache-dir -r /requirements.txt