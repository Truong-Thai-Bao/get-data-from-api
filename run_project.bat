@echo off
title AUTO RUN BIG DATA PIPELINE (Spark - Kafka - Cassandra)
color 0A

echo ================================================================
echo   HE THONG BIG DATA REAL-TIME (SELF-HEALING MODE)
echo ================================================================
echo.

:: --- BUOC 1: KHOI DONG HA TANG ---
echo [1/5] Dang khoi dong Docker Compose...
echo        (Tu dong don dep cac service thua nhu schema-registry/worker...)
docker-compose up -d --remove-orphans

:: --- BUOC 2: CHO DOI ---
echo.
echo [2/5] Dang doi 45 giay de Cassandra va Spark tinh ngu...
echo        (Vui long khong tat cua so nay!)
timeout /t 45 /nobreak >nul

:: --- BUOC 3: NAP CODE MOI NHAT ---
echo.
echo [3/5] Dang copy file code "spark-stream.py" vao Container...
docker cp spark-stream.py get-data-spark-master-1:/opt/spark/

:: --- BUOC 4: CAI DAT MOI TRUONG ---
echo.
echo [4/5] Dang cai dat lai thu vien "cassandra-driver"...
docker exec -u 0 get-data-spark-master-1 pip install cassandra-driver

:: --- BUOC 5: KICH HOAT ---
echo.
echo [5/5] Kich hoat che do "Bat Tu" (Background Mode)...
docker exec -d get-data-spark-master-1 /opt/spark/bin/spark-submit --conf "spark.jars.ivy=/tmp/.ivy" --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/spark-stream.py

echo.
echo ================================================================
echo                 TRANG THAI HE THONG
echo ================================================================

:: Kiem tra xem tien trinh Python co dang chay khong
docker top get-data-spark-master-1 | findstr "python" >nul
if %errorlevel% equ 0 (
    echo [OK]  Spark Streaming dang chay ON DINH (Co Python process).
    echo [OK]  He thong se tu dong khoi dong lai neu gap loi mang/DB.
) else (
    color 0C
    echo [ERROR] Spark chua chay! Co the do loi code hoac thieu RAM.
    echo         Hay kiem tra log: docker logs get-data-spark-master-1
)

echo.
echo ----------------------------------------------------------------
echo  - Airflow: http://localhost:8080 (Admin/admin)
echo  - Kiem tra DB: docker exec -it cassandra cqlsh
echo ----------------------------------------------------------------
echo.
pause