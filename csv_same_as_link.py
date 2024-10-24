import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
  dag_id="L_source_csv_sal", 
  start_date=datetime.datetime(2024, 10, 16),
  schedule_interval = None,
  catchup=False,
  template_searchpath='/var/dags/dags_lisa/subway_model/subway_airflow',
) as dag:
    
# Заполнение SAL с помощью dbt
    sal_ins_dbt = BashOperator(
        task_id = "ins_dbt_SAL",
        bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
        + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
        + f"&& dbt run --models models/example/ins_to_sal.sql", 
      )
    
    ins_to_sal = PostgresOperator(
        task_id = "ins_to_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'Insert into dbt_schema."GPR_BV_A_CLIENT" select * from ins_to_sal',
        dag = dag, 
    )

# Переунификация
    sal_upd = PostgresOperator(
        task_id = "update_SAL",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/update_same_as_link.sql',
        dag = dag, 
    )

sal_ins_dbt >> ins_to_sal >> sal_upd