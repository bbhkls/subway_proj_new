import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_pit", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_ne/subway_proj',
) as dag:
    
# Заполнение Satellite с помощью dbt

    # Вставка новых значений, если записи об экземпляре есть и данные там изменились
    ins_mod_val = BashOperator(
          task_id="ins_modif",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt run --models models/example/ins_modif_pit.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    # Вставка записей о новых экземпляров
    ins_new_val = BashOperator(
          task_id="ins_new",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt run --models models/example/ins_new_pit.sql --vars '{execution_date : {{ execution_date }}, run_id : {{ run_id }} }'", 
      )
    
    # Объединение данных для вставки
    union_ins_val = BashOperator(
          task_id="ins_union",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + f"&& dbt run --models models/example/ins_to_pit.sql", 
      )
    
    # Вставка всех записей в сателит
    pit_ins = PostgresOperator(
        task_id = "ins_pit",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_P_CLIENT" select * from ins_to_pit',
        dag = dag, 
    )
    
    # Обновление конца периода в случае, когда экземпляр сущности не удален
    pit_not_del_upd = PostgresOperator(
        task_id = "not_del_upd",
        postgres_conn_id = 'dbt_postgres',
        sql = 'sql_scripts/update_not_del_pit.sql',
        dag = dag, 
    )

    # Обновление конца периода в случае, когда экземпляр сущности удален
    pit_del_upd = PostgresOperator(
        task_id = "del_upd",
        postgres_conn_id = 'dbt_postgres',
        sql = 'sql_scripts/update_del_pit.sql',
        dag = dag, 
    )

ins_mod_val >> ins_new_val >> union_ins_val >> pit_ins >> pit_not_del_upd >> pit_del_upd