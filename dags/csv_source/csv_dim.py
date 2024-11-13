import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_source_csv_dim", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts',
) as dag:

# Заполнение DIM с помощью dbt
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_dim.sql"]},
        dag = dag,
    )
    
    dim_ins = PostgresOperator(
        task_id = "ins_dim",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_EM_DIM_CLIENT" select * from ins_to_dim',
        dag = dag, 
    )

# Обновление концов временных интервалов
    dim_dttm_upd = PostgresOperator(
        task_id = "dim_dttm_upd",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_dim.sql',
        params = {"run_id": "{{ run_id}}", "execution_date":"{{execution_date}}", "dim":"CLIENT"},
        dag = dag, 
    )
    
transform >> dim_ins >> dim_dttm_upd
