from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    # dag_id = 'A_master_dag',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    # ODS слой
    # Выгрузка из Oracle в Postgres
    trigger_ora_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='A_from_ora_to_postgres',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='A_cut_ods_table',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # RV слой
    # Модификация HUB объекта
    trigger_hub_mod = TriggerDagRunOperator(
        task_id='hub_mod',
        trigger_dag_id='A_source_csv_hub',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Модификация Satellite объекта
    trigger_sat_mod = TriggerDagRunOperator(
        task_id='sat_mod',
        trigger_dag_id='A_source_csv_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Модификация Effective Satellite объекта
    trigger_e_sat_mod = TriggerDagRunOperator(
        task_id='e_sat_mod',
        trigger_dag_id='A_source_csv_e_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # BV слой
    # Модификация SAL объекта
    trigger_same_as_link_mod = TriggerDagRunOperator(
        task_id='same_as_link_mod',
        trigger_dag_id='A_source_csv_sal',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Модификация PIT объекта
    trigger_pit_mod = TriggerDagRunOperator(
        task_id='pit_mod',
        trigger_dag_id='A_source_csv_pit',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # EM слой
    # Модификация DIM объекта
    trigger_dim_mod = TriggerDagRunOperator(
        task_id='dim_mod',
        trigger_dag_id='A_source_csv_dim',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    # Заключительный этап
    # Обновляем METА данные о выгрузке в таблице
    trigger_meta_mod = TriggerDagRunOperator(
        task_id='meta_mod',
        trigger_dag_id='A_source_csv_meta',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
    )

    trigger_ora_post >> trigger_cut_ods >> [trigger_hub_mod, trigger_sat_mod, trigger_e_sat_mod] >> trigger_same_as_link_mod
    trigger_same_as_link_mod >> trigger_pit_mod >> trigger_dim_mod >> trigger_meta_mod