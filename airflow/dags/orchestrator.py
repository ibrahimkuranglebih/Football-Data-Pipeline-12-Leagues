import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import pendulum

sys.path.append('/opt/airflow/extract')

from insert_records import main
local_time = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='football_pipeline',
    start_date=pendulum.datetime(2026, 1, 4, tz=local_time),
    schedule="0 8,20 * * *",
    default_args=default_args,
    catchup=False,
    tags=['football', 'pipeline','dbt','elt']
) as dag:

    extract_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=main
    )

    transform_data = DockerOperator(
        task_id='dbt_transform',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run --project-dir /usr/app',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source='/home/ibrahim/repos/football-data-pipeline/dbt/football_dbt',
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source='/home/ibrahim/repos/football-data-pipeline/dbt',
                target='/root/.dbt',
                type='bind'
            )
        ],
        network_mode='football-data-pipeline_football-network',
        mount_tmp_dir=False,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    extract_data >> transform_data