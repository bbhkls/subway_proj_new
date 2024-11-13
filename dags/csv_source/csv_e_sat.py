import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands


with DAG(
  dag_id="L_source_csv_e_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts',
) as dag:
    
# Заполнение Satellite с помощью dbt

    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_or_modif_e_sat.sql", "ins_del_e_sat_macros.sql", "ins_to_e_sat.sql"]},
        dag = dag,
    )
    
    # Вставка всех записей в сателит
    e_satelite_ins = PostgresOperator(
        task_id = "ins_e_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_E_CLIENT" select * from ins_to_e_sat',
        dag = dag, 
    )
    
    e_satelite_upd = PostgresOperator(
        task_id = "update_e_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_E_CLIENT", "key_p" : "client_rk"},
        dag = dag, 
    )
    
transform >> e_satelite_ins >> e_satelite_upd
