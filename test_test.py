from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_test_dag',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    # Выгрузка из Oracle в Postgres
    trigger_ora_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='L_from_ora_to_postgres',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_cut_ods_table',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    #  Модификация HUB объекта
    trigger_hub_mod = TriggerDagRunOperator(
        task_id='hub_mod',
        trigger_dag_id='L_source_csv_hub',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    #  Модификация Satellite объекта
    trigger_sat_mod = TriggerDagRunOperator(
        task_id='sat_mod',
        trigger_dag_id='L_source_csv_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    trigger_ora_post >> trigger_cut_ods >> [trigger_hub_mod, trigger_sat_mod]