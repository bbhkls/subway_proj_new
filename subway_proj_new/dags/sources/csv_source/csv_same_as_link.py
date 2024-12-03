import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="Al_source_csv_sal", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts/client_sql',
  
) as dag:
    
# Заполнение SAL с помощью dbt
    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["tbl_union_client_sat.sql", "ins_to_sal_client.sql"]},
        dag = dag,
    )
    
    ins_to_sal = PostgresOperator(
        task_id = "ins_to_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_A_CLIENT" select * from ins_to_sal_client ',
        dag = dag, 
    )

# Переунификация
    sal_upd = PostgresOperator(
        task_id = "update_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_same_as_link.sql',
        params = {"table" : "tbl_union_client_sat", "name_p" : "name_desc", "birthday" : "birthday_dt"}, 
        dag = dag, 
    )

transform >> ins_to_sal >> sal_upd
