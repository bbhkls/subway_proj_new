import subprocess

def run_dbt_commands(models, **context):
    """
    функция принимает список из названия dbt моделей с постфиксом .sql и выполняет их

    params:
        models - список строк - названия dbt моделей следующего формата - <название_dbt_модели>.sql
        пример: models=['model1.sql', 'model2.sql']
    """
    # Определение команд для выполнения
    
    for model in models:
        commands = [
            "cd /home/anarisuto-12/dbt/subway_project",
            "source /home/anarisuto-12/dbt/venv/bin/activate",
            "dbt run --models " + model + " --vars '{execution_date : " + str(context["execution_date"]) + " , run_id :  " + str(context["run_id"]) + " }'"
        ]

        # Объединение команд в одну строку
        full_command = ' && '.join(commands)

        result = subprocess.run(
            full_command, 
            shell=True, 
            executable='/bin/bash', 
            capture_output=True, 
            text=True,
            check=True    
        )
        
        print(result)