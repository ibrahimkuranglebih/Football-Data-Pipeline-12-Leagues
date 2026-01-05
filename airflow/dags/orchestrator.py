import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

sys.path.append('/opt/airflow/extract')
from insert_records import main

default_args = {
    'description' : 'A DAG to orchestrate football data ingestion',
    'start_date' : datetime(2026,1,4),
    'catchup' : False
}

dag = DAG(
    dag_id = 'football-dag',
    default_args=default_args,
    schedule=timedelta(minutes=1)
)

with dag : 
    extract_data = PythonOperator(
        task_id = 'extract_data_task',
        python_callable = main
    )
    
    transform_data = DockerOperator(
        task_id = 'transform_data_task',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'run',
        working_dir = '/usr/app',
        mounts = [
            Mount(
                source = '/home/ibrahim/repos/football-data-pipeline/dbt/football_dbt',
                target = '/usr/app',
                type = 'bind'
            ),
            Mount(
                source = '/home/ibrahim/repos/football-data-pipeline/dbt/profiles.yml',
                target = '/root/.dbt/profiles.yml',
                type = 'bind'
            )
        ],
        network_mode = '',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    
    
     > 