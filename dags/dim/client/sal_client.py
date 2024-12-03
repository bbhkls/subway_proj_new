import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_sal_client", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts',
) as dag:
    
# Заполнение SAL с помощью dbt
    
    transform_client = PythonOperator(
        task_id = "transform_client",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["tbl_union_client_sat.sql",]},
        dag = dag,
    )
    
# Вставка в SAL и переунификация
    sal_upd_client = PostgresOperator(
        task_id = "update_SAL_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'merge_sal.sql',
        params = {"sat_table" : "tbl_union_client_sat", "logic_key": ("name_desc", "birthday_dt"), "dim" : "CLIENT"}, 
        dag = dag, 
    )

transform_client >> sal_upd_client