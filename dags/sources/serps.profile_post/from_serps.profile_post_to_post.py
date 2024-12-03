import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_serps.profile_post_to_ods", 
  start_date=datetime.datetime(2024, 10, 14),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new',
) as dag:
    
    ods_upd = PostgresOperator(
        task_id = "ins_ods",
        postgres_conn_id = 'dbt_postgres',
        sql = """insert into dbt_schema.ods_profile_card_post
                 select '{{ execution_date }}', 'serps.profile_card_post'::regclass::oid, rp.*
                 from serps.profile_card_post rp""",
        params = {"execution_date" : "{{ execution_date }}"},
        dag = dag, 
    )
