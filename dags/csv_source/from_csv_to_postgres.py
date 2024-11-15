import os
import json
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook


    # Функция выгрузки данных из Oracle в csv файл с идентификатором выгрузки
def from_ora_to_csv_with_date(execution_date) :
    oracle_hook = OracleHook(oracle_conn_id='ora_lisa')
    data = oracle_hook.get_pandas_df(sql="select ORA_HASH(table_name||tablespace_name) oid, s.* FROM client_from_star s, all_tables WHERE owner='SERPS' AND TABLE_NAME = 'CLIENT_FROM_STAR'")
    #csv_file_npath = '/var/dags/dags_lisa/subway_model/subway_airflow/csv_model/new_out.csv'
    csv_file_npath = '/var/dags/dags_alex/subway_proj_new/csv_model/new_out.csv'


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
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts/client_sql',
) as dag_n:
    
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
        + '-c "\copy dbt_schema.ods_client_csv FROM \'/var/dags/dags_alex/subway_proj_new/csv_model/new_out.csv\' delimiter \',\' csv header"',
    )


insert_from_ora_to_csv >> insert_into_postgres