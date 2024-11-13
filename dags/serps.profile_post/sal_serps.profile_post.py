import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_serps.profile_ora_sal", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts/client_sql',
) as dag:
    
# Заполнение SAL с помощью dbt
    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_sal.sql"]},
        dag = dag,
    )
    
    ins_to_sal = PostgresOperator(
        task_id = "ins_to_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_A_CLIENT" select * from ins_to_sal ',
        dag = dag, 
    )

# Переунификация
    sal_upd = PostgresOperator(
        task_id = "update_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_same_as_link.sql',
        dag = dag, 
    )

transform >> ins_to_sal >> sal_upd
