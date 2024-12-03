import os
import json
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dags_alex.subway_proj_new.usefull_func.create_transform_func import run_dbt_commands

with DAG(
  dag_id="L_pit_client", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_alex/subway_proj_new/sql_scripts',
) as dag:
    
    # Заполнение PIT с помощью dbt
    transform = PythonOperator(
        task_id = "transform",
        python_callable = run_dbt_commands,
        op_kwargs={"models": ["ins_new_to_pit_client.sql", "ins_to_pit_client.sql"]},
        dag = dag,
    )
    
    # Вставка всех записей в PIT
    pit_ins = PostgresOperator(
        task_id = "ins_pit",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_P_CLIENT" select * from ins_to_pit_client',
        dag = dag, 
    )
    
    # Обновление данных, в случаях, если добавилась строка в пит, обновление прошлого периода. 
    # Или закрытие периода, в случаях, когда экземпляр сущности удален на всех источниках
    pit_upd = PostgresOperator(
        task_id = "pit_upd",
        postgres_conn_id = 'dbt_postgres',
        sql = 'update_pit.sql',
        params = {"run_id": "{{run_id}}", "execution_date" : "{{ execution_date }}", "dim": "CLIENT", "satellits": (("GPR_RV_M_CLIENT_SUBWAY_STAR", ("name_desc", "birthday_dt"), "client_subway_star_vf_dttm"), ("GPR_RV_M_CLIENT_PROFILE_POST", ("fio_desc", "birthday_dt"), "profile_client_post_vf_dttm"))},
        dag = dag, 
    )


transform >> pit_ins >> pit_upd