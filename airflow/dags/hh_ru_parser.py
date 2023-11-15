from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

import re
import json
import random
import os
import time

import requests
from bs4 import BeautifulSoup
import lxml

from dotenv import dotenv_values
from fake_useragent import FakeUserAgent
from datetime import date, timedelta

import pandas as pd
import psycopg2

class hh_parser:
    def __init__(self, max_page_count=3) -> None:
        self.max_page_count = max_page_count
        self.vac_name_list = self.get_vacancy_name_list()

        self.re_html_tag_remove = r'<[^>]+>'

        self.df = pd.DataFrame(['vacancy_id', 'vacancy_name', 'towns', 
                                'level', 'company', 'salary_from', 'salary_to',
                                'exp_from', 'exp_to', 'description', 
                                'job_type', 'job_format', 'languages', 
                                'skills', 'source_vac', 
                                'date_created', 'date_of_download', 
                                'status', 'date_closed',
                                'version_vac', 'actual'])

    def topars(self):
        for vac_name in self.vac_name_list[0:10]:
            self.pars_vac(vac_name)

    def pars_vac(self, vac_name):
        for page_number in range(self.max_page_count):
            params = {
                'text': f'{vac_name}',
                'page': page_number,
                'per_page': 50,
                'area': '113',
                'only_with_salary': 'true',
                'negotiations_order': 'updated_at',
                'vacancy_search_order': 'publication_time'
            }

            try:
                print(f'get 1.{page_number} {vac_name}')
                req = requests.get('https://api.hh.ru/vacancies', params=params).json()
                
                res = {}
                if 'items' in req.keys():
                    for item in req['items']:
                        res['vacancy_id'] = item['alternate_url']
                        res['vacancy_name'] = item['']
                        res['towns'] = item['area']['name']
                        res['level'] = item['']
                        res['company'] = item['employer']['name']

                        if item['salary'] == None:
                            res['salary_from'] = item['']
                            res['salary_to'] = item['']
                        else:
                            res['salary_from'] = item['']
                            res['salary_to'] = item['']

                        if item['experience'] == None:
                            res['exp_from'] = '0'
                            res['exp_to'] = '100'
                        elif item['experience']['id'] == 'noExperience':
                            res['exp_from'] = '0'
                            res['exp_to'] = '0'
                        elif item['experience']['id'] == 'between1And3':
                            res['exp_from'] = '1'
                            res['exp_to'] = '3'
                        elif item['experience']['id'] == 'between3And6':
                            res['exp_from'] = '3'
                            res['exp_to'] = '6'
                        else:
                            res['exp_from'] = '6'
                            res['exp_to'] = '100'

                        res['description'] = re.sub(self.re_html_tag_remove, '', item['description'])[0:65530]

                        res['job_type'] = item['']
                        res['job_format'] = item['']
                        res['languages'] = item['']
                        res['skills'] = ' '.join(skill['name'] for skill in item['key_skills'])
                        res['source_vac'] = 'hh.ru'
                        res['date_created'] = item['published_at']
                        res['date_of_download'] = str(datetime.datetime.now())
                        res['status'] = item['']
                        res['date_closed'] = item['']
                        res['version_vac'] = item['']
                        res['actual'] = item['']
                        
                else:
                    print(req)

            except Exception as e:
                print(f'ERROR {vac_name}', e)
                continue

    def get_vacancy_name_list(self):
        vac_name_list = []
        with open('/opt/airflow/data_dag_pars/vac_name_list.csv', encoding='utf-8') as f:
            for line in f:
                vac_name_list.append(line.replace('\n', '').replace('+',' ').replace('-',' '))
        return vac_name_list
    

dag = DAG(
    'HH_parsing',
    description='DAG для парсинга вакансий HH',
    schedule_interval=None,
    start_date=datetime(2022, 11, 10)
)

parser = hh_parser()

Create_table = PostgresOperator(
    task_id="create_hh_table",
    sql="""
        CREATE TABLE IF NOT EXISTS hh (
            vacancy_id VARCHAR(2048) NOT NULL,
            vacancy_name VARCHAR(255) NOT NULL,
            towns VARCHAR(100),
            level VARCHAR(50),
            company VARCHAR(255),
            salary_from BIGINT,
            salary_to BIGINT,
            exp_from SMALLINT,
            exp_to SMALLINT,
            description TEXT,
            job_type VARCHAR(255),
            job_format VARCHAR(255),
            languages VARCHAR(255),
            skills VARCHAR(1024),
            source_vac VARCHAR(255),
            date_created DATE,
            date_of_download DATE NOT NULL, 
            status VARCHAR(32),
            date_closed DATE,
            version_vac INTEGER NOT NULL,
            actual SMALLINT
        )""",
    postgres_conn_id = "PostgreSQL_DEV",
    dag=dag
)

Pars = PythonOperator(
    task_id='call_method_topars',
    python_callable=parser.topars,
    dag=dag
)

Create_table >> Pars