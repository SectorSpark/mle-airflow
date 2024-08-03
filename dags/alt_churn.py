# dags/alt_churn.py
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
# продолжите код и запустите его в виртуальной машине #
# после отработки кода нажмите кнопку Проверить, добавлять свое решение необязательно #

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