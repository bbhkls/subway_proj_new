import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_cut_ods_table", 
  start_date=datetime.datetime(2024, 10, 17),
  schedule_interval = None,
  catchup=False,
) as dag:
    
#  запускаем dbt модель обрезка таблицы ods
    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"sql_sqcripts": ["ods_client_cut.sql"]},
        dag = dag,
    )

transform