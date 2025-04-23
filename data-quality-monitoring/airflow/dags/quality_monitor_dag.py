
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

def run_metrics_collector():
    subprocess.run(["python3", "pyspark/metrics_collector.py"])

dag = DAG('quality_monitor', start_date=datetime(2023, 1, 1), schedule_interval='@hourly')

task = PythonOperator(task_id='run_collector', python_callable=run_metrics_collector, dag=dag)
