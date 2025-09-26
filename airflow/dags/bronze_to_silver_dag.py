from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from datasets import silver_sptrans_posicoes

with DAG(
    dag_id="bronze_to_silver",
    start_date=pendulum.datetime(2025, 9, 23, tz="America/Sao_Paulo"),
    schedule_interval="5 * * * *",
    catchup=False,
    tags=["sptrans", "bronze", "silver", "spark"],
) as dag:

    command = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "/opt/bitnami/spark/apps/bronze_to_silver.py "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%m') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%d') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') }}"
    )

    submit_spark_job_docker = DockerOperator(
        task_id="submit_bronze_to_silver_spark_job",
        image="bitnami/spark:3.5",
        command=command,
        network_mode="fia-projeto-final_sptrans-network",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/c/Users/guilherme/Desktop/FIA/Docker/FIA-Projeto-Final/spark/apps",
                target="/opt/bitnami/spark/apps",
                type="bind"
            )
        ],
        outlets=[silver_sptrans_posicoes]
    )