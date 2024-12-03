import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands


with DAG(
  dag_id="Al_serps.profile_post_m_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts',
) as dag:
    
# Заполнение Satellite с помощью dbt

    
    m_satellite_trf_client_pg = PythonOperator(
        task_id = "m_sat_trf_client_pg",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_modif_to_m_sat_client_profile_card_post_pg.sql", "ins_to_m_sat_client_profile_card_post_pg.sql"]},
        dag = dag,
    )

    # Вставка всех записей в сателит
    m_satelite_ins_client_pg = PostgresOperator(
        task_id = "m_sat_ins_client_pg",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_M_CLIENT_PROFILE_POST" select * from "ins_to_m_sat_client_profile_card_post_pg"',
        dag = dag, 
    )
    
    m_satelite_upd_client_pg = PostgresOperator(
        task_id = "update_m_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_M_CLIENT_PROFILE_POST", "key_p" : ("client_rk", )},
        dag = dag, 
    )
    
m_satellite_trf_client_pg >> m_satelite_ins_client_pg >> m_satelite_upd_client_pg
