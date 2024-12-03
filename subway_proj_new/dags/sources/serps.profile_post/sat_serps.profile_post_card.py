import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="Al_serps.profile_post_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts',
) as dag:
    

  
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_sat_card_profilecard_post_pg.sql"]},
        dag = dag,
    )
    
    # Вставка всех записей в сателит
    satelite_ins = PostgresOperator(
        task_id = "ins_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_S_PROFILE_CARD_POST" select * from ins_to_sat_card_profilecard_post_pg',
        dag = dag, 
    )
    
    # Обновляем флаги в Satellite
    satelite_upd = PostgresOperator(
        task_id = "update_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_S_PROFILE_CARD_POST", "key_p" : ("card_rk", )},
        dag = dag, 
    )



transform >> satelite_ins >> satelite_upd