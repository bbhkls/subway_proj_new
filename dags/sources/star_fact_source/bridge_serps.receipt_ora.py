import os
import json
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_serps.receip_ora_to_bridge", 
  start_date=datetime.datetime(2024, 10, 18),
  schedule_interval = None,
  catchup=False,
) as dag:
    
#  запускаем dbt модель обрезка таблицы ods
      transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_to_new_b_receip_card.sql", "ins_to_modif_b_receip_card.sql", "ins_to_b_receip_card.sql"]},
        dag = dag,
    )
      
      t_bridge_upd = PostgresOperator(
        task_id = "upd_t_bridge",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_B_RECEIPT_CARD_POST" select * from ins_to_b_receip_card',
        dag = dag, 
    )
      
      
      transform >> t_bridge_upd