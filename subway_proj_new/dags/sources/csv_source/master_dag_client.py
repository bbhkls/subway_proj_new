from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_master_dag',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 16)
) as dag:
    
    
    ALL_DAGS = [
        
        {'task_id' : 'ora_post', 'dag_id' : 'Al_from_ora_to_postgres', 'dependes_on' : 'cut_ods'}, 
        {'task_id' : 'cut_ods', 'dag_id' : 'L_cut_ods_table', 'dependes_on' : ['hub_mod', 'e_sat_mod', 'm_sat_mod']},
        {'task_id' : 'hub_mod', 'dag_id' : 'L_source_csv_hub', 'dependes_on' : 'meta_mod'},
        {'task_id' : 'm_sat_mod', 'dag_id' : 'L_source_csv_m_sat', 'dependes_on' : 'meta_mod'},
        {'task_id' : 'e_sat_mod', 'dag_id' : 'Al_source_csv_e_sat', 'dependes_on' : 'meta_mod'},
        #{'task_id' : 'same_as_link_mod', 'dag_id' : 'Al_source_csv_sal', 'dependes_on' : 'meta_mod'},
        #{'task_id' : 'pit_mod', 'dag_id' : 'L_source_csv_pit', 'dependes_on' : 'dim_mod'},
        #{'task_id' : 'dim_mod', 'dag_id' : 'L_source_csv_dim', 'dependes_on' : 'meta_mod'},
        {'task_id' : 'meta_mod', 'dag_id' : 'L_source_csv_meta', 'dependes_on' : ''},
    ]
    
    dbt_task = dict()
    
    dbt_compile = BashOperator(
          task_id="dbt_compile",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt compile --vars '{execution_date : " + '{{execution_date}}' + " }'",
      )


    for ind, dag in enumerate(ALL_DAGS):
        
        dbt_task[dag['task_id']] = TriggerDagRunOperator(
        task_id = dag["task_id"],
        trigger_dag_id= dag["dag_id"],
        execution_date='{{ execution_date }}',
        trigger_run_id='{{ run_id }}',
        reset_dag_run=True,
        wait_for_completion = True,
        poke_interval = 10
        )
        
    dbt_compile 
    for ind, dag in enumerate(ALL_DAGS):
        if dag['task_id'] == 'ora_post':
            dbt_compile >> dbt_task[dag['task_id']]
        if dag['dependes_on'] != '':
            
            if isinstance(dag['dependes_on'], list) :
                for i in dag['dependes_on']:
                    dbt_task[dag['task_id']] >> dbt_task[i]
            else:       
                dbt_task[dag['task_id']] >> dbt_task[dag['dependes_on']]
