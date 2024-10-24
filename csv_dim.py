import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_hub", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_model/subway_airflow',
) as dag:

# Заполнение DIM с помощью dbt
    dim_ins_dbt = BashOperator(
        task_id = "ins_dbt_dim",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + f"&& dbt run --models models/example/ins_to_dim.sql", 
      )
    
    dim_ins = PostgresOperator(
        task_id = "ins_dim",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_EM_DIM_CLIENT" select * from ins_to_dim',
        dag = dag, 
    )

# Обновление концов временных интервалов
    dim_dttm_upd = PostgresOperator(
        task_id = "dim_dttm_upd",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/update_dim.sql',
        dag = dag, 
    )
    
dim_ins_dbt >> dim_ins >> dim_dttm_upd