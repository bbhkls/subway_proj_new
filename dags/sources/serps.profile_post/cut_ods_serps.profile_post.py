import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_serps.profile_post_cut_ods", 
  start_date=datetime.datetime(2024, 10, 17),
  schedule_interval = None,
  catchup=False,
) as dag:
    
#  запускаем dbt модель обрезка таблицы ods
  transform = PythonOperator(
          task_id = "transform",
          python_callable = run_dbt_commands,
          op_kwargs={"models": ["ods_cut_client_profile_card_post_pg.sql"]},
          dag = dag,
      )

  transform
