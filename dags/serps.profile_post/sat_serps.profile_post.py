
import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands


with DAG(
  dag_id="L_serps.profile_post_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts',
) as dag:
    
# Заполнение Satellite с помощью dbt

  # Client

    satelite_dbt_client = PythonOperator(
        task_id = "dbt_model_client",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_or_modif_m_sat_profile_serps.sql", "ins_del_m_sat_profile_serps.sql", "ins_to_m_sat_profile_serps.sql"]},
        dag = dag,
    )
   
   # Card
    
    satelite_dbt_card = PythonOperator(
        task_id = "dbt_model_card",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_or_modif_sat_profile_card.sql", "ins_del_sat_profile_card.sql", "ins_to_sat_profile_card.sql"]},
        dag = dag,
    )

    # Вставка всех записей в сателит

    # Client

    satelite_ins_client = PostgresOperator(
        task_id = "ins_satelite_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_M_CLIENT_PROFILE_POST" select * from ins_to_m_sat_profile_serps',
        dag = dag, 
    )
    
    # Card

    satelite_ins_card = PostgresOperator(
        task_id = "ins_satelite_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_S_PROFILE_CARD_POST" select * from ins_to_sat_profile_card',
        dag = dag, 
    )

    # Обновляем флаги в Satellite

    # Client

    satelite_upd_client = PostgresOperator(
        task_id = "update_satelite_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_M_CLIENT_PROFILE_POST", "key_p" : "client_rk" },
        dag = dag, 
    )

    # Card

    satelite_upd_card = PostgresOperator(
        task_id = "update_satelite_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_S_PROFILE_CARD_POST", "key_p" : "card_rk" },
        dag = dag, 
    )
    
    satelite_dbt_client >> satelite_ins_client >> satelite_upd_client
    satelite_dbt_card >> satelite_ins_card >> satelite_upd_card
