from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from dotenv import dotenv_values
import os
from os import walk
import random
import paramiko

def create():
    with open('test.txt', 'w') as f:
        for _ in range(50):
            f.write(str(random.randint(0,100)))

def pull():
    create()
    os.system('scp /opt/airflow/test.txt DE_4@146.120.224.155:/home/4_group/Lapis/')
    
dag = DAG(
    'fetch_token_and_save_to_file',
    description='DAG для получения токена и сохранения в файл',
    schedule_interval='0 0 * * *',  # Здесь можно указать расписание выполнения
    start_date=datetime(2022, 1, 1),  # Здесь можно указать дату начала выполнения
    catchup=False
)

fetch_token_task = PythonOperator(
    task_id='fetch_token',
    python_callable=pull,
    dag=dag
)

fetch_token_task