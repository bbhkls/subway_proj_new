from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_serps.profile_post_master',
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
    # Выгрузка из Oracle в Postgres
    trigger_ora_post = TriggerDagRunOperator(
        task_id='post_post',
        trigger_dag_id='L_serps.profile_post_to_ods',
        execution_date='{{ execution_date }}',
        wait_for_completion = True,
        reset_dag_run=True,
        poke_interval = 10,
    )

    # Обрезка ODS таблицы по текущей выгрузке
    trigger_cut_ods = TriggerDagRunOperator(
        task_id='cut_ods',
        trigger_dag_id='L_serps.profile_post_cut_ods',
        execution_date='{{ execution_date }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    # RV слой
    # Модификация HUB объекта
    trigger_hub_mod = TriggerDagRunOperator(
        task_id='hub_mod',
        trigger_dag_id='L_serps.profile_post_hub_card',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    # Модификация Satellite объекта
    trigger_m_sat_mod = TriggerDagRunOperator(
        task_id='m_sat_mod',
        trigger_dag_id='Al_serps.profile_post_m_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    trigger_sat_mod = TriggerDagRunOperator(
        task_id='sat_mod',
        trigger_dag_id='Al_serps.profile_post_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    # Модификация Effective Satellite объекта
    trigger_e_sat_mod = TriggerDagRunOperator(
        task_id='e_sat_mod',
        trigger_dag_id='L_serps.profile_post_e_sat',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    # # BV слой
    # # Модификация SAL объекта
    # trigger_same_as_link_mod = TriggerDagRunOperator(
    #     task_id='same_as_link_mod',
    #     trigger_dag_id='L_serps.profile_ora_sal',
    #     execution_date='{{ execution_date }}',
    #     trigger_run_id='{{ run_id }}',
    #     reset_dag_run=True,
    #     wait_for_completion = True,
    # )

    # # Модификация PIT объекта
    # trigger_pit_mod = TriggerDagRunOperator(
    #     task_id='pit_mod',
    #     trigger_dag_id='L_serps.profile_post_pit',
    #     execution_date='{{ execution_date }}',
    #     trigger_run_id='{{ run_id }}',
    #     reset_dag_run=True,
    #     wait_for_completion = True,
    # )

    # # EM слой
    # # Модификация DIM объекта
    # trigger_dim_mod = TriggerDagRunOperator(
    #     task_id='dim_mod',
    #     trigger_dag_id='L_source_csv_dim_client',
    #     execution_date='{{ execution_date }}',
    #     trigger_run_id='{{ run_id }}',
    #     reset_dag_run=True,
    #     wait_for_completion = True,
    # )

    # Заключительный этап
    # Обновляем METА данные о выгрузке в таблице
    trigger_meta_mod = TriggerDagRunOperator(
        task_id='meta_mod',
        trigger_dag_id='L_serps.profile_post_meta',
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10,
    )

    dbt_compile >> trigger_ora_post >> trigger_cut_ods >> [trigger_hub_mod, trigger_m_sat_mod, trigger_sat_mod, trigger_e_sat_mod] 
    [trigger_hub_mod, trigger_m_sat_mod, trigger_sat_mod, trigger_e_sat_mod] >> trigger_meta_mod