from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import random
import string



create_table_sql = """
CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(50) NOT NULL,
    email VARCHAR(50) NOT NULL,
    idade INTEGER NOT NULL
);
"""

def extract_and_transform():
    data = []
    for i in range(1, 6):
        nome = ''.join(random.choices(string.ascii_letters, k=10))
        email = f"{nome.lower()}@example.com"
        idade = random.randint(18, 70)
        data.append((nome, email, idade))
    return data

def load(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(create_table_sql)
    
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    for x in data:
        cursor.execute("INSERT INTO usuarios (nome, email, idade) VALUES (%s, %s, %s)", x)
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    "Desafio",
    schedule_interval="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["Desafio"],
) as dag:

    extract_and_transfom_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_and_transform,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load,
    )

extract_and_transfom_data >> load_data