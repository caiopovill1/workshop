from airflow import DAG

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
}

def print_random():
    from random import randint
    number = randint(0,100)
    if number >= 50:
        return 'maior'
    else:
        return 'menor'
    
with DAG(
    dag_id='exemple_dag4',
    schedule_interval='*/5 * * * *',
    default_args=args,
    start_date=datetime.now()) as dag:
    
    def print_random():
        from random import randint
        number = randint(0,100)
        if number >= 50:
            return 'maior'
        else:
            return 'menor'
    
    
    task1 = PythonOperator(
        task_id='greet',
        python_callable=print_random
    )
    
    task1