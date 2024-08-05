# dags/alt_churn.py
import pendulum
import os, sys
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.append('/home/mle-user/mle_projects/mle-airflow/plugins/')
sys.path.append('/opt/airflow/plugins/')
sys.path.append('/home/mle-user/airflow/plugins/')
from steps.churn import create_table, extract, transform, load

with DAG(
    dag_id='prepare_alt_churn_dataset',  # This is the required dag_id argument
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
) as dag:
    step1 = PythonOperator(task_id='create_table', python_callable=create_table)
    step2 = PythonOperator(task_id='extract', python_callable=extract)
    step3 = PythonOperator(task_id='transform', python_callable=transform)
    step4 = PythonOperator(task_id='load', python_callable=load)
    step1 >> step2 >> step3 >> step4