from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime


with DAG(
    dag_id='exemple_dag5',
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
        
    def lucky():
        return 'YOU ARE LUCKY'
    
    def unlucky():
        return 'YOU ARE UNLUCKY'
        
    
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
        python_callable= lucky
    )
    
    loser = PythonOperator(
        task_id = 'loser',
        python_callable=unlucky
        )
    
    random >> maior >> winner
    random >> menor >> loser
    
    
        
    