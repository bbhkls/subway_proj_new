import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_serps.receipt_post_to_ods", 
  start_date=datetime.datetime(2024, 10, 14),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
    hub_upd = PostgresOperator(
        task_id = "ins_ods",
        postgres_conn_id = 'dbt_postgres',
        sql = """insert into dbt_schema.ods_receipt_post
                 select '{{ execution_date }}', 'serps.receipt_post'::regclass::oid, rp.*
                 from serps.receipt_post rp""",
        params = {"execution_date" : "{{ execution_date }}", "param1" : "ods_client_cut"},
        dag = dag, 
    )
