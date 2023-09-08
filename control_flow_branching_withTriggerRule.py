from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime


def branch_func():
    rand_value = randint(1, 10)
    if rand_value > 5:
        return 'Task_B'
    return 'Task_C'


with DAG("my_ControlFlowBranchingTriggerRuleDag",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:
    Task_A = BashOperator(
        task_id="Task_A",
        bash_command="echo 'This is the first task Task A'"
    )

    Task_Branch = BranchPythonOperator(
        task_id='Task_Branch',
        python_callable=branch_func
    )
    Task_B = BashOperator(
        task_id="Task_B",
        bash_command="echo 'This is the Task B'"
    )
    Task_C = BashOperator(
        task_id="Task_C",
        bash_command="echo 'This is the Task C'"
    )

    Task_D = BashOperator(
        task_id="Task_D",
        bash_command="echo 'This is the Task D'",
        trigger_rule='none_failed_min_one_success'
    )

    Task_A >> Task_Branch >> [Task_B, Task_C] >> Task_D
