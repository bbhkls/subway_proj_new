import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_meta", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
    upd_meta = PostgresOperator(
        task_id = 'update_meta',
        postgres_conn_id = 'dbt_postgres',
        sql = 'sql_scripts/merge_metadata.sql',
        params = {"run_id" : "{{ run_id}}", "execution_date" : "{{ execution_date }}", "param1" : "csv"},
        dag = dag,
    )

upd_meta