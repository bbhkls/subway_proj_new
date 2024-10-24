import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_e_sat", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_model/subway_airflow',
) as dag:
    
# Заполнение Satellite с помощью dbt

    # Данные, которые были обновлены или новые записи
    ins_new_mod_val = BashOperator(
          task_id="ins_new_or_modif",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + f"&& dbt run --models models/example/ins_new_or_modif_e_sat.sql", 
      )
    
    # Данные, которые были удалены
    ins_del_val = BashOperator(
          task_id="ins_del",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + f"&& dbt run --models models/example/ins_del_e_sat.sql", 
      )
    
    # Объединение данных для вставки
    union_ins_val = BashOperator(
          task_id="ins_union",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + f"&& dbt run --models models/example/ins_to_e_sat.sql", 
      )
    
    # Вставка всех записей в сателит
    e_satelite_ins = PostgresOperator(
        task_id = "ins_e_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_RV_E_CLIENT" select * from ins_to_e_sat',
        dag = dag, 
    )
    
    # Обновляем флаги в Satellite
    e_satelite_upd = PostgresOperator(
        task_id = "update_e_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/update_e_sat.sql',
        dag = dag, 
    )

ins_new_mod_val >> ins_del_val >> union_ins_val >> e_satelite_ins >> e_satelite_upd