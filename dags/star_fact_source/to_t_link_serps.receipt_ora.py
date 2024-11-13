import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_serps.receip_ora_t_link", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
# Заполнение T-Link с помощью dbt
    t_link_ins_dbt = BashOperator(
        task_id = "ins_t_link",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + "&& dbt run --models models/example/ins_to_receip_card_t_link.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    t_link_upd = PostgresOperator(
        task_id = "upd_t_link",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_T_RECEIPT_POST" select * from ins_to_receip_card_t_link',
        dag = dag, 
    )

t_link_ins_dbt >> t_link_upd