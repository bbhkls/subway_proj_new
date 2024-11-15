import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands


with DAG(
  dag_id="L_serps.profile_post_hub_card", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new',
) as dag:
    
# Заполнение Hub с помощью dbt
    hub_ins = PythonOperator(
     task_id = "ins_hub_client_card",
        python_callable = run_dbt_commands,
        op_kwargs={"sql_sqcripts": ["ins_to_hub_client_card_profile_card_post_pg.sql", "ins_to_hub_card_profile_card_post_pg.sql"]},
        dag = dag,
    )
    
    # Card
    hub_upd_card = PostgresOperator(
        task_id = "upd_hub_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CARD" select distinct * from ins_to_hub_card_profile_card_post_pg',
        dag = dag, 
    )

    # Client
    hub_upd_client = PostgresOperator(
        task_id = "upd_hub_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_H_CLIENT" select distinct * from ins_to_hub_client_card_profile_card_post_pg',
        dag = dag, 
    )

hub_ins >> [hub_upd_card, hub_upd_client]