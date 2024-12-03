import csv
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id= 'L_main_master_dag',
    schedule_interval= None,
    catchup= False,
) as dag:

    dbt_task = dict()
    csv_path = '/var/dags/dags_alex/subway_proj_new/csv_model/data_main_master_dag.csv'
    with open(csv_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='\'')
        for row in spamreader:
            dbt_task[row[0]] = TriggerDagRunOperator(
            task_id = row[0],
            trigger_dag_id= row[1],
            execution_date='{{ execution_date }}',
            trigger_run_id='{{ run_id }}',
            reset_dag_run=True,
            wait_for_completion = True,
            poke_interval = 30,
            )
            
    with open(csv_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='\'')
        for row in spamreader:
            if row[2] != ' ':
                if len(row) > 3 :
                    for i in range(2,len(row)):
                        dbt_task[row[0]] >> dbt_task[row[i]]
                else:       
                    dbt_task[row[0]] >> dbt_task[row[2]]