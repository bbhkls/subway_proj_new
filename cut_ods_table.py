import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="L_cut_ods_table", 
  start_date=datetime.datetime(2024, 10, 17),
  schedule_interval = None,
  catchup=False,
) as dag:
    
#  запускаем dbt модель обрезка таблицы ods
    cut_table_dbt = BashOperator(
          task_id="cut_dbt",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt run  --models models/example/ods_client_cut.sql --vars '{execution_date : {{ execution_date }}}' ", 
      )

cut_table_dbt