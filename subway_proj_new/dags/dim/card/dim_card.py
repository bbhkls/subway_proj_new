import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_dim_card", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts',
) as dag:
    
    # Создаем view для занесения в DIM с помощью dbt
    exec_dbt = PythonOperator(
        task_id = "exec_dbt",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_upd_to_dim_card.sql"]},
        dag = dag,
    )

    upd_dim = PostgresOperator(
        task_id = "update_DIM_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'ins_upd_dim.sql',
        params = {
            "dim": "CARD", 
            "vie": "ins_upd_to_dim_card", 
            "column_dim": 
                (
                "card_num_cnt",
	            "card_service_name_desc",
	            "discount_procent_cnt"
                )}, 
        dag = dag, 
    )


exec_dbt >> upd_dim