from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

import psycopg2
import time

def test_connect_def():
    pg_hook = PostgresHook(postgres_conn_id='PostgreSQL_DEV')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute("""DELETE INTO pet VALUES (1, 'Кевин', 'Кот', '2022-12-25', 'Иван')""")
    conn.commit()

    cur.execute("""INSERT INTO pet VALUES (1, 'Кевин', 'Кот', '2022-12-25', 'Иван')""")
    conn.commit()

    conn.close()

with DAG(
    dag_id='asdhjasd',
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
        postgres_conn_id = 'PostgreSQL_DEV'
    )

    test_connect = PythonOperator(
        task_id="test_connect_to_table_used_psycopg2",
        python_callable=test_connect_def
    )

    create_pet_table >> test_connect