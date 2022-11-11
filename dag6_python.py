from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime


with DAG(
    dag_id='exemple_dag6',
    schedule_interval='*/5 * * * *',
    start_date = datetime.now()
) as dag:
    
    def print_random():
        from random import randint
        number = randint(0,100)
        if number >= 50:
            return 'maior'
        else:
            return 'menor'
        
    def lucky(name):
        return f'{name} YOU ARE LUCKY'
    
    def unlucky(name):
        return f'{name} YOU ARE UNLUCKY'
        
    
    random = BranchPythonOperator(
        task_id = 'Random_number',
        python_callable= print_random
    )
    
    maior = DummyOperator(
        task_id = 'maior'
    )
    
    menor = DummyOperator(
        task_id = 'menor'
    )
    
    winner = PythonOperator(
        task_id= 'winner',
        python_callable= lucky,
        op_kwargs={'name':'joao'})
    
    loser = PythonOperator(
        task_id = 'loser',
        python_callable=unlucky,
        op_kwargs={'name':'joao'}
        )
    
    random >> maior >> winner
    random >> menor >> loser