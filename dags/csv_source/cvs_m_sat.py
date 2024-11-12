import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_arina.subway_fold.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_source_csv_m_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj/sql_scripts',
) as dag:
    

  
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"sql_sqcripts": ["ins_new_or_modif_m_sat_client_orcl.sql", "ins_del_m_sat_client_orcl.sql", "ins_to_m_sat_client_orcl.sql"]},
        dag = dag,
    ) 
    
    # Вставка всех записей в сателит
    satelite_ins = PostgresOperator(
        task_id = "ins_m_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" select * from ins_to_m_sat_client_orcl',
        dag = dag, 
    )
    
   # Обновляем флаги в Satellite
    satelite_upd = PostgresOperator(
        task_id = "update_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_M_CLIENT_SUBWAY_STAR", "key_p" : "client_rk"},
        dag = dag, 
    )



transform >> satelite_ins >> satelite_upd