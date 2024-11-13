from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_serps.receip_ora_master_dag',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    # ODS слой
    # Выгрузка из Postgres в Postgres
    trigger_ora_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='L_serps.receipt_ora_to_ods',
        execution_date='{{ execution_date }}',
        wait_for_completion = True,
        reset_dag_run=True,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_post_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_serps.receip_ora_cut_ods',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # RV слой
    # Модификация T_LINK объекта
    trigger_t_link_mod = TriggerDagRunOperator(
        task_id='t_link_mod',
        trigger_dag_id='L_source_serps.receip_ora_t_link',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Заключительный этап
    # Обновляем METА данные о выгрузке в таблице
    trigger_meta_post_mod = TriggerDagRunOperator(
        task_id='meta_mod',
        trigger_dag_id='L_source_serps.receip_ora_meta',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

trigger_ora_post >> trigger_cut_post_ods >> trigger_t_link_mod>> trigger_meta_post_mod