import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_source_csv_hub", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts/client_sql',
) as dag:
    
# # Заполнение Hub с помощью dbt
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_hub_client_client_from_star_orcl.sql"]},
        dag = dag,
    )
   
    
    hub_upd = PostgresOperator(
        task_id = "upd_hub",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CLIENT" select * from ins_to_hub_client_client_from_star_orcl',
        dag = dag, 
    )

transform >> hub_upd