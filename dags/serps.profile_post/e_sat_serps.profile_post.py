
import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands


with DAG(
  dag_id="L_serps.profile_post_e_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts',
) as dag:
    
# Заполнение Satellite с помощью dbt

  # Client

    e_satelite_dbt_client = PythonOperator(
        task_id = "dbt_model_client",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_or_modif_e_sat_profile_client.sql", "ins_del_e_sat_profile_client.sql", "ins_to_e_sat_profile_client.sql"]},
        dag = dag,
    )
   
   # Card
    
    e_satelite_dbt_card = PythonOperator(
        task_id = "dbt_model_card",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_or_modif_e_sat_profile_card.sql", "ins_del_e_sat_profile_card.sql", "ins_to_e_sat_profile_card.sql"]},
        dag = dag,
    )

    # Вставка всех записей в сателит

    # Client

    e_satelite_ins_client = PostgresOperator(
        task_id = "ins_e_satelite_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_E_CLIENT" select * from ins_to_e_sat_profile_client',
        dag = dag, 
    )
    
    # Card

    e_satelite_ins_card = PostgresOperator(
        task_id = "ins_e_satelite_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_E_CARD" select * from ins_to_e_sat_profile_card',
        dag = dag, 
    )

    # Обновляем флаги в Satellite

    # Client

    e_satelite_upd_client = PostgresOperator(
        task_id = "update_e_satelite_client",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_E_CLIENT", "key_p" : "client_rk" },
        dag = dag, 
    )

    # Card

    e_satelite_upd_card = PostgresOperator(
        task_id = "update_e_satelite_card",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_E_CARD", "key_p" : "card_rk" },
        dag = dag, 
    )
    
    e_satelite_dbt_client >> e_satelite_ins_client >> e_satelite_upd_client
    e_satelite_dbt_card >> e_satelite_ins_card >> e_satelite_upd_card
