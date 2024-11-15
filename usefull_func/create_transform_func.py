import subprocess



def run_dbt_commands(sql_sqcripts, **context):
    # Определение команд для выполнения
    
    for sql_sqcript in sql_sqcripts:
        commands = [
            "cd /home/anarisuto-12/dbt/subway_project",
            "source /home/anarisuto-12/dbt/venv/bin/activate",
            "dbt run --models models/example/" + sql_sqcript + " --vars '{execution_date : " + str(context["execution_date"]) + " , run_id :  " + str(context["run_id"]) + " }'"
        ]
        
        # может добавить сюда передачу параметов 

        # Объединение команд в одну строку
        full_command = ' && '.join(commands)

        result = subprocess.run(full_command, shell=True, executable='/bin/bash', capture_output=True, text=True)
    
        
