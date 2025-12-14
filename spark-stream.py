import logging
from datetime import datetime

import time
# Sửa lại Import đúng chuẩn PySpark
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType



def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_stream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
    print('Keyspace created successfully')

def create_table(session):
    session.execute("""
            CREATE TABLE IF NOT EXISTS spark_stream.air_quality (
                date timestamp,
                temperature float,
                dew float,
                humidity float,
                pressure float,
                wind_speed float,
                pm25 float,
                PRIMARY KEY (date)
            );
        """)
    print('Table created successfully')


def insert_data(session, **kwargs):
    print('interting data...')

    date = kwargs.get('date')
    temperature = kwargs.get('temperature')
    dew = kwargs.get('dew')
    humidity = kwargs.get('humidity')
    pressure = kwargs.get('pressure')
    wind_speed = kwargs.get('wind_speed')
    pm25 = kwargs.get('pm25')

    try:
        session.execute("""
            INSERT INTO spark_stream.air_quality(date, temperature, dew, humidity, pressure,wind_speed, pm25)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
        """,(date,temperature,dew,humidity,pressure,wind_speed,pm25))
        logging.info(f'Data insert at:{date}')
    except Exception as e:
        logging.error(f'An exception error occurred due to{e}')



def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info('Spark connection created successfully')
    except Exception as e:
        logging.error(f'Could not create spark connection due to exception: {e}')

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers','broker:29092') \
        .option('subscribe', 'current_aqi') \
        .option('startingOffsets','earliest') \
        .load()
        logging.info('kafka df created successfully')

    except Exception as e:
        logging.error(f'kafka df could not be created because of{e}')

    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session

    except Exception as e:
        logging.error(f'Error due to {e}')

        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('date', StringType(), False),
        StructField('temperature', FloatType(), False),
        StructField('dew', FloatType(), False),
        StructField('humidity', FloatType(), False),
        StructField('pressure', FloatType(), False),
        StructField('wind_speed', FloatType(), False),
        StructField('pm25', FloatType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select('data.*')

    # Cast String Date sang Timestamp cho Cassandra
    sel = sel.withColumn("date", col("date").cast(TimestampType()))

    return sel


if __name__ == "__main__":
    while True:
        spark_conn = None
        try:

            # 1. Tạo kết nối Spark
            spark_conn = create_spark_connection()

            # 2. Tạo kết nối Cassandra & Bảng
            session = create_cassandra_connection()
            create_keyspace(session)
            create_table(session)

            # 3. Đọc từ Kafka
            spark_df = connect_to_kafka(spark_conn)
            selection_df = create_selection_df_from_kafka(spark_df)


            streaming_query = selection_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option('checkpointLocation', '/tmp/checkpoint_v2') \
                .option('keyspace', 'spark_stream') \
                .option('table', 'air_quality') \
                .start()

            # Chờ cho đến khi bị tắt hoặc gặp lỗi
            streaming_query.awaitTermination()

        except Exception as e:
            print(f"\n!!! HE THONG BI SAP DO LOI: {e}")

            # Dọn dẹp session cũ nếu có
            if spark_conn:
                try:
                    spark_conn.stop()
                except:
                    pass

            time.sleep(10)  # Nghỉ 10 giây
            continue  # Quay lại đầu vòng lặp # Quay lại đầu vòng lặp while để chạy lại từ đầu