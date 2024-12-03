import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_source_serps.receip_ora_t_link", 
  start_date=datetime.datetime(2024, 10, 18),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new',
) as dag:
    
# Заполнение T-Link с помощью dbt
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_t_link_receipt_receipt_post_orcl.sql"]},
        dag = dag,
    )
    
    t_link_upd = PostgresOperator(
        task_id = "upd_t_link",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_T_RECEIPT_POST" select * from ins_to_t_link_receipt_receipt_post_orcl',
        dag = dag, 
    )

transform >> t_link_upd

