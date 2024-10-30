import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
# Заполнение Satellite с помощью dbt

    # Данные, которые были обновлены или новые записи
    ins_new_mod_val = BashOperator(
          task_id="ins_new_or_modif",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt run --models models/example/ins_new_or_modif_sat.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    # Данные, которые были удалены
    ins_del_val = BashOperator( 
          task_id="ins_del", 
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project"  
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate'  
          + "&& dbt run --models models/example/ins_del_sat_macros.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'",  
      )
    
    # Объединение данных для вставки
    union_ins_val = BashOperator(
          task_id="ins_union",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt run --models models/example/ins_to_sat.sql", 
      )
    
    # Вставка всех записей в сателит
    satelite_ins = PostgresOperator(
        task_id = "ins_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_S_CLIENT" select * from ins_to_sat',
        dag = dag, 
    )
    
    # Обновляем флаги в Satellite
    satelite_upd = PostgresOperator(
        task_id = "update_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'sql_scripts/update_(e_)sat.sql',
        params = {"param1" : "GPR_RV_S_CLIENT"},
        dag = dag, 
    )

ins_new_mod_val >> ins_del_val >> union_ins_val >> satelite_ins >> satelite_upd