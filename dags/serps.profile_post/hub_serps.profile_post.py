import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_serps.profile_post_hub_card", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
# Заполнение Hub с помощью dbt

    # Card

    hub_ins_dbt_card = BashOperator(
        task_id = "ins_hub_card",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + "&& dbt run --models models/example/ins_to_hub_profile_card.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    hub_upd_card = PostgresOperator(
        task_id = "upd_hub_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CARD" select distinct * from ins_to_hub_profile_card',
        dag = dag, 
    )

    # Client

    hub_ins_dbt_client = BashOperator(
        task_id = "ins_hub_client",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + "&& dbt run --models models/example/ins_to_hub_profile_client.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    hub_upd_client = PostgresOperator(
        task_id = "upd_hub_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CLIENT" select distinct * from ins_to_hub_profile_client',
        dag = dag, 
    )

[hub_ins_dbt_card, hub_ins_dbt_client] >> [hub_upd_card, hub_upd_client]