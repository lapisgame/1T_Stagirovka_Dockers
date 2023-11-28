from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime

import re
import time

import requests

from datetime import date, datetime

import pandas as pd

class hh_parser:
    def __init__(self, max_page_count=3) -> None:
        self.table_name = 'hh_row'
        self.max_page_count = max_page_count
        self.vac_name_list = self.get_vacancy_name_list()

        #& Регулярное выражение для удаление HTML тегов из описания
        self.re_html_tag_remove = r'<[^>]+>'

        self.new_df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 
                                'level', 'company', 'salary_from', 'salary_to',
                                'exp_from', 'exp_to', 'description', 
                                'job_type', 'job_format', 'languages', 
                                'skills', 'source_vac', 
                                'date_created', 'date_of_download', 
                                'status', 'date_closed',
                                'version_vac', 'actual'])
        
        self.pg_hook = PostgresHook(postgres_conn_id='PostgreSQL_DEV')

    #* Проверка содержится ли в нужном DataFrame в столбце vacancy_id значение target_id
    def check_vac_id(self, dataframe, target_id):
        return not dataframe[dataframe['vacancy_id'] == target_id].empty

    #* Функция установки версии вакансии в базе данных
    def get_version(self, arr):
        try:
            all_df = pd.concat([self.new_df, self.old_df], ignore_index=True)
            if self.check_vac_id(self.old_df, arr['vacancy_id']):
                #& Поиск максимума если есть отличия
                new_df = all_df[all_df['vacancy_id']==arr['vacancy_id']]
                if not new_df.empty:
                    version = new_df.max()['version_vac']
                    if len(new_df) > 1:
                        temp_df = new_df[new_df['version_vac']==version]
                    else:
                        temp_df = new_df
                    
                    #^ Если имеются различия в хотябы одном поле значение version_vac увеличивает на 1
                    if (tuple(temp_df['description'])[0] != arr['description'] or \
                    tuple(temp_df['vacancy_name'])[0] != arr['vacancy_name'] or \
                    tuple(temp_df['towns'])[0] != arr['towns'] or \
                    tuple(temp_df['salary_from'])[0] != arr['salary_from'] or \
                    tuple(temp_df['salary_to'])[0] != arr['salary_to'] or \
                    tuple(temp_df['job_type'])[0] != arr['job_type'] or \
                    tuple(temp_df['job_format'])[0] != arr['job_format'] or \
                    tuple(temp_df['skills'])[0] != arr['skills']):
                        return int(version)+1
                    else:
                        return -1

            return 1
        except Exception as exp:
            return 1

    #* Функция устанавливающая всем версиям вакансии кроме последней фраг actual в 0
    def set_actual(self):
        connection = self.pg_hook.get_conn()
        cur = connection.cursor()

        all_df = pd.read_sql(f"SELECT * FROM {self.table_name}", connection)
        dist_df = pd.read_sql(f"SELECT DISTINCT ON (vacancy_id) vacancy_id FROM {self.table_name}", connection)
        
        for index, row in dist_df.iterrows():
            vacancy_id = row['vacancy_id']
            max_version = all_df[all_df['vacancy_id'] == vacancy_id]['version_vac'].max()
            
            #& Обновление 'actual' во всех строках с текущим 'vacancy_id' кроме строки с максимальной версией
            all_df.loc[(all_df['vacancy_id'] == vacancy_id) & (all_df['version_vac'] != max_version), 'actual'] = 0

        #^ Запись измененных данных в базу данных, с заменой текущих данных
        cur.execute(f'DELETE FROM {self.table_name}', connection)
        connection.commit()

        tpls = [tuple(x) for x in all_df.to_numpy()]
        sql = "INSERT INTO hh_row VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur.executemany(sql, tpls)

        connection.commit()
        connection.close()

    #* Основная функция парсинга
    def topars(self):
        connection = self.pg_hook.get_conn()
        cur = connection.cursor()

        #^ Получение уже имеющихся данных, необходимо для дальнейшего сравнения на существование и версионность
        self.old_df = pd.read_sql(f"SELECT * FROM {self.table_name}", connection)
        for vac_name in self.vac_name_list:
            self.pars_vac(vac_name, index=self.vac_name_list.index(vac_name)+1)
            time.sleep(5)
        
        logging.info('ПАРСИНГ ЗАВЕРШЕН')

        #& В new_df остаются только данные которые в своей строке отличаются в vacancy_id и version_vac от уже имеющихся в old_df
        if len(self.old_df) > 0:
            self.new_df = self.new_df[~self.new_df.set_index(['vacancy_id', 'version_vac']).index.isin(self.old_df.set_index(['vacancy_id', 'version_vac']).index)]

        #& Замена всех NULL значений на нули (их быть не должно, но мало ли)
        self.res_df = self.new_df.fillna(0)

        logging.info('INSERT НАЧАЛО')

        #& Запись полученных данных в базу
        tpls = [tuple(x) for x in self.res_df.to_numpy()]
        sql = "INSERT INTO hh_row VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur.executemany(sql, tpls)

        connection.commit()
        connection.close()
        logging.info('INSERT УСПЕШНО ЗАВЕРШИТЬ')

    #* Добавление в new_df всех вакансий которые возможно получить по названию vac_name
    def pars_vac(self, vac_name:str, index:int):
        for page_number in range(self.max_page_count):
            params = {
                'text': f'{vac_name}',
                'page': page_number,
                'per_page': 20,
                'area': '113',
                'only_with_salary': 'true',
                'negotiations_order': 'updated_at',
                'vacancy_search_order': 'publication_time'
            }

            try:
                logging.info(f'get 1.{page_number} {index}/{len(self.vac_name_list)} - {vac_name}')
                req = requests.get('https://api.hh.ru/vacancies', params=params).json()
                time.sleep(5)
                
                if 'items' in req.keys():
                    for item in req['items']:
                        item = requests.get(f'https://api.hh.ru/vacancies/{item["id"]}').json()
                        res = {}
                        try:
                            res['vacancy_id'] = f'https://hh.ru/vacancy/{item["id"]}'
                            res['vacancy_name'] = item['name']
                            res['towns'] = item['area']['name']
                            res['level'] = ''
                            res['company'] = item['employer']['name']

                            if item['salary'] == None:
                                res['salary_from'] = 0
                                res['salary_to'] = 999999999
                            else:
                                if item['salary']['from'] != None:
                                    res['salary_from'] = int(item['salary']['from'])
                                else:
                                    res['salary_from'] = 0
                                    
                                if item['salary']['to'] != None:
                                    res['salary_to'] = int(item['salary']['to'])
                                else:
                                    res['salary_to'] = 999999999

                            if item['experience'] == None:
                                res['exp_from'] = '0'
                                res['exp_to'] = '100'
                            elif item['experience']['id'] == 'noExperience':
                                res['exp_from'] = '0'
                                res['exp_to'] = '0'
                                res['level'] = 'Junior'
                            elif item['experience']['id'] == 'between1And3':
                                res['exp_from'] = '1'
                                res['exp_to'] = '3'
                                res['level'] = 'Middle'
                            elif item['experience']['id'] == 'between3And6':
                                res['exp_from'] = '3'
                                res['exp_to'] = '6'
                                res['level'] = 'Senior'
                            else:
                                res['exp_from'] = '6'
                                res['exp_to'] = '100'
                                res['level'] = 'Lead'

                            res['description'] = re.sub(self.re_html_tag_remove, '', item['description'])

                            res['job_type'] = item['employment']['name']
                            res['job_format'] = item['schedule']['name']

                            res['languages'] = 'Russian'

                            res['skills'] = ' '.join(skill['name'] for skill in item['key_skills'])

                            res['source_vac'] = 'hh.ru'

                            res['date_created'] = item['published_at']

                            res['date_of_download'] = datetime.now()
                            res['status'] = item['type']['name']

                            res['date_closed'] = date(2025, 1, 1)

                            res['version_vac'] = self.get_version(res) 
                            res['actual'] = '1'      

                            if res['version_vac'] != -1:
                                self.new_df = pd.concat([self.new_df, pd.DataFrame(pd.json_normalize(res))], ignore_index=True)
                        except Exception as exc:
                            logging.error(f'В процессе парсинга вакансии https://hh.ru/vacancy/{item["id"]} произошла ошибка {exc} \n\n')

                else:
                    logging.info(req)

            except Exception as e:
                logging.error(f'ERROR {vac_name} {e}')
                time.sleep(5)
                continue

    #* Перезапись в базу данных только унакальных по совокупности vacancy_id и version_vac строк с полным удалением остального
    def rewrite_distinct(self):
        connection = self.pg_hook.get_conn()
        cur = connection.cursor()
        df = pd.read_sql(f"SELECT DISTINCT ON (vacancy_id, version_vac) * FROM {self.table_name}", connection)

        cur.execute(f'DELETE FROM {self.table_name}', connection)
        connection.commit()

        tpls = [tuple(x) for x in df.to_numpy()]
        sql = "INSERT INTO hh_row VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cur.executemany(sql, tpls)

        connection.commit()
        connection.close()

    #* Функция, получающая список названий вакансий из csv файла
    def get_vacancy_name_list(self):
        vac_name_list = []
        with open('/opt/airflow/data_dag_pars/vac_name_list.csv', encoding='utf-8') as f:
            for line in f:
                vac_name_list.append(line.replace('\n', '').replace('+',' ').replace('-',' '))
        return vac_name_list

dag = DAG(
    'HH_parsing',
    description='DAG для парсинга вакансий HH',
    schedule="@daily",
    start_date=datetime(2023, 11, 21)
)

parser = hh_parser()

Create_table = PostgresOperator(
    task_id="create_hh_row_table",
    sql="""
        CREATE TABLE IF NOT EXISTS hh_row (
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

Rewrite_DISTINCT = PythonOperator(
    task_id='rewrite_distinct_df',
    python_callable=parser.rewrite_distinct,
    dag=dag
)

Set_actual = PythonOperator(
    task_id='set_actual',
    python_callable=parser.set_actual,
    dag=dag
)

Create_table >> Pars >> Rewrite_DISTINCT >> Set_actual