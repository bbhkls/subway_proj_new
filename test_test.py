from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_test_dag',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    trigger_ora_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='L_from_ora_to_postgres',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
    )

    trigger_cut_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_cut_ods_table',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    trigger_ora_post >> trigger_cut_ods