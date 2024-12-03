from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id = 'L_master_dag_pit',
    #schedule_interval= '* */1 * * *',
    schedule_interval= None,
    catchup=False,
    start_date=datetime(2024, 10, 16)
) as dag:
    
    
    ALL_DAGS = [
        
        {'task_id' : 'client_pit', 'dag_id' : 'L_pit_client', 'dependes_on' : ''}, 
        {'task_id' : 'card_pit', 'dag_id' : 'L_pit_card', 'dependes_on' : ''},
    ]
    
    dbt_task = dict()
    
    dbt_compile = BashOperator(
          task_id="dbt_compile",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" 
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' 
          + "&& dbt compile --vars '{execution_date : " + '{{execution_date}}' + " }'",
      )

    # Цикл для создания тасков

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

    # Цикл для создания зависимостей

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
    
    