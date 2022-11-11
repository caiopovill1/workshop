from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
}
with DAG(dag_id='example_dag1',
         default_args=args,
         schedule_interval='*/5 * * * *',
         start_date=datetime.now()) as dag:
    
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo hellor world, my first task'
    )
    
    task1