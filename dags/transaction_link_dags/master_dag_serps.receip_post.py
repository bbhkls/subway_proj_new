from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_master_dag_serps.receip_post',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    # ODS слой
    # Выгрузка из Postgres в Postgres
    trigger_post_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='L_serps.receipt_post_to_ods',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_post_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_cut_ods_serps.receip_post',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # RV слой
    # Модификация T_LINK объекта
    trigger_t_link_mod = TriggerDagRunOperator(
        task_id='hub_mod',
        trigger_dag_id='L_source_serps.receip_post_t_link',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Заключительный этап
    # Обновляем METА данные о выгрузке в таблице
    trigger_meta_post_mod = TriggerDagRunOperator(
        task_id='meta_mod',
        trigger_dag_id='L_source_serps.receip_post_meta',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

trigger_post_post >> trigger_cut_post_ods >> trigger_t_link_mod >> trigger_meta_post_mod