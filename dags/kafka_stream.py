import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'Airflow',
    'email': ['thaibao09092004@gmail.com'],
    'start_date': datetime(2025, 12, 8),
    'retries': 1
}



def calc_dewpoint(temp, rh):
    import math
    a = 17.27
    b = 237.7
    alpha = (a * temp) / (b + temp) + math.log(rh/100)
    return round((b * alpha) / (a - alpha),1)


def format_data(res):
    #reformat data and rename columns
    data = {}
    data['date'] = datetime.now().isoformat() #current day
    data['temperature'] = res['temperature_2m']
    data['dew'] = res['dew_point']
    data['humidity'] = res['relative_humidity_2m']
    data['pressure'] = round(res['surface_pressure'],1)
    data['wind_speed'] = res['wind_speed_10m']
    data['pm25'] = res['pm25']

    return data


def batch_data():
    import requests

    get_pm25 = requests.get('https://air-quality-api.open-meteo.com/v1/air-quality?latitude=10.823&longitude=106.6296&'
                            'current=pm2_5&timezone=Asia%2FBangkok')
    pm25 = get_pm25.json()
    pm25 = pm25['current']['pm2_5']

    get_others = requests.get('https://api.open-meteo.com/v1/forecast?latitude=10.823&longitude=106.6296&'
                              'current=temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m&'
                              'timezone=Asia%2FBangkok&wind_speed_unit=ms')
    others = get_others.json()
    others = others['current']
    others.pop('time',None) #drop key time
    others.pop('interval',None) #drop key interval
    dew = calc_dewpoint(others['temperature_2m'], others['relative_humidity_2m']) #cal dew point
    others['dew_point'] = dew
    others['pm25'] = pm25

    return others


def stream_data():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=10000 # time out: 10 secs
    )
    res = batch_data() # get data
    res = format_data(res) # format data
    producer.send('current_aqi', json.dumps(res).encode('utf-8')) # send procedure


with DAG(
    'user_automation',
    default_args=default_args,
    schedule='*/2 * * * *', #Run at 0th minute of each hour
    catchup=False
) as dag:
    batch_task = PythonOperator(
        task_id='batch_task',
        python_callable=stream_data
    )