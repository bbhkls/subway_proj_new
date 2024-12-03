from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_serps.receip_ora_master_dag',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 18)
) as dag:
    
    dbt_compile = BashOperator(
          task_id="dbt_compile",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt compile --vars '{execution_date : " + '{{execution_date}}' + " }'",
      )

    # ODS слой
    # Выгрузка из Postgres в Postgres
    trigger_ora_post = TriggerDagRunOperator(
        task_id='ora_post',
        trigger_dag_id='L_serps.receipt_ora_to_ods',
        execution_date='{{ execution_date }}',
        wait_for_completion = True,
        reset_dag_run=True,
        poke_interval = 30,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_post_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_serps.receip_ora_cut_ods',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 30,
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
        poke_interval = 30,
    )
    
    
    # BV слой
    # Заполнение объекта Bridge, генерация новой сущности Карта Лояльности 
    trigger_bridge_mod = TriggerDagRunOperator(
        task_id='bridge_mod',
        trigger_dag_id='L_serps.receip_ora_to_bridge',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 30,
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
        poke_interval = 10,
    )

trigger_ora_post >> trigger_cut_post_ods >> trigger_t_link_mod>>  trigger_bridge_mod >> trigger_meta_post_mod

