import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_hub", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
# Заполнение Hub с помощью dbt
    hub_ins_dbt = BashOperator(
        task_id = "ins_hub",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + "&& dbt run --models models/example/ins_to_hub.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    hub_upd = PostgresOperator(
        task_id = "upd_hub",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CLIENT" select * from ins_to_hub',
        dag = dag, 
    )

hub_ins_dbt >> hub_upd