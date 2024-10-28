import os
import json
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Функция для того, чтобы забирать данные из Oracle и записывать их в файл
#def get_data_from_oracle():
    # oracle_hook = OracleHook(oracle_conn_id='ora_lisa')
    # data = oracle_hook.get_pandas_df(sql="select ORA_HASH(table_name||tablespace_name) oid, s.* FROM client_from_star s, all_tables WHERE owner='SERPS' AND TABLE_NAME = 'CLIENT_FROM_STAR'")
    
    # # Открываем csv файл
    # csv_file_path = '/var/dags/dags_lisa/subway_model/subway_airflow/csv_model/out.csv'

    # # Записываем в новый файл
    # data.to_csv(csv_file_path, index = False)

# Функция для формирования нового csv файла с идентификаторов выгрузки
# def data_to_csv(execution_date) :

#     # Открываем csv файл
#     csv_file_path = '/var/dags/dags_lisa/subway_model/subway_airflow/csv_model/out.csv'
#     csv_file_npath = '/var/dags/dags_lisa/subway_model/subway_airflow/csv_model/new_out.csv'
#     df = pd.read_csv(csv_file_path)

#     # Присоединяем столбец с датой к данным
#     df.insert(loc = 0,
#           column = 'dttm',
#           value = execution_date)

#     # Записываем в новый файл
#     df.to_csv(csv_file_npath, index = False)

    # Функция выгрузки данных из Oracle в csv файл с идентификатором выгрузки
def from_ora_to_csv_with_date(execution_date) :
    oracle_hook = OracleHook(oracle_conn_id='ora_lisa')
    data = oracle_hook.get_pandas_df(sql="select ORA_HASH(table_name||tablespace_name) oid, s.* FROM client_from_star s, all_tables WHERE owner='SERPS' AND TABLE_NAME = 'CLIENT_FROM_STAR'")
    #csv_file_npath = '/var/dags/dags_lisa/subway_model/subway_airflow/csv_model/new_out.csv'
    csv_file_npath = '/var/dags/dags_lisa/subway_ne/subway_proj/csv_model/new_out.csv'

    # Присоединяем столбец с датой к данным
    data.insert(loc = 0,
        column = 'dttm',
        value = execution_date)

    # Записываем в новый файл
    data.to_csv(csv_file_npath, index = False)
            

with DAG(
  dag_id="L_from_ora_to_postgres", 
  start_date=datetime.datetime(2024, 10, 14),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag_n:
    
    # Добавление данных о текущей выгрузке в таблицу
    # upd_meta = PostgresOperator(
    #     task_id = 'update_meta',
    #     postgres_conn_id = 'dbt_postgres',
    #     sql = 'sql_scripts/merge_metadata.sql',
    #     params = {"run_id" : "{{ run_id}}", "execution_date" : "{{ execution_date }}", "param1" : "csv"},
    #     dag = dag_n,
    # )

    # Выгрузка данных из Oracle в csv файл
    # insert_from_ora_to_csv = PythonOperator(
    #     task_id = 'insert_from_ora_to_csv',
    #     python_callable = get_data_from_oracle,
    #     dag = dag_n,
    # )

    # Вставка нового столбца в csv файл со значением текущей выгрузки
    # insert_to_csv = PythonOperator(
    #     task_id = 'insert_to_csv',
    #     python_callable = data_to_csv,
    #     op_kwargs={"execution_date" : "{{ execution_date }}"},
    #     dag = dag_n,
    # )

    insert_from_ora_to_csv = PythonOperator(
         task_id = 'insert_to_csv',
         python_callable = from_ora_to_csv_with_date,
         op_kwargs={"execution_date" : "{{ execution_date }}"},
         dag = dag_n,
     )

    # Делаем выгрузку в Postgres
    insert_into_postgres = BashOperator (
        task_id = "insert_into_postgres",
        bash_command=f"export PGPASSWORD=dbt "
        + f"&& psql -Udbt_user -hdesktop-5h7tutm -dpostgres "
        + '-c "\copy dbt_schema.ods_client_csv FROM \'/var/dags/dags_lisa/subway_ne/subway_proj/csv_model/new_out.csv\' delimiter \',\' csv header"',
    )

#upd_meta >> insert_from_ora_to_csv >> insert_to_csv >> insert_into_postgres 
insert_from_ora_to_csv >> insert_into_postgres