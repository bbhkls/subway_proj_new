import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_source_csv_pit", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts/client_sql',
) as dag:
    
# Заполнение PIT с помощью dbt
    
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_to_pit_client.sql", "ins_to_pit_client.sql"]},
        dag = dag,
    )
    
    # Вставка всех записей в сателит
    pit_ins = PostgresOperator(
        task_id = "ins_pit",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_P_CLIENT" select * from ins_to_pit_client',
        dag = dag, 
    )
    
    # Обновление конца периода в случае, когда экземпляр сущности не удален
    # pit_not_del_upd = PostgresOperator(
    #     task_id = "not_del_upd",
    #     postgres_conn_id = 'dbt_postgres',
    #     sql = 'update_not_del_pit.sql',
    #     params = {"run_id": "{{ run_id}}", "execution_date":"{{execution_date}}", "dim":"CLIENT", "satellits": ('"dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"', '"dbt_schema"."GPR_RV_M_PROFILE_POST"')},
    #     dag = dag, 
    # )

    # # Обновление конца периода в случае, когда экземпляр сущности удален
    # pit_del_upd = PostgresOperator(
    #     task_id = "del_upd",
    #     postgres_conn_id = 'dbt_postgres',
    #     sql = 'update_del_pit.sql',
    #     params = {"run_id": "{{ run_id}}", "execution_date":"{{execution_date}}", "dim":"CLIENT"},
    #     dag = dag, 
    # )

transform >> pit_ins #>> pit_not_del_upd >> pit_del_upd