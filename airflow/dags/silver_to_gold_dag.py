from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from docker.types import Mount
from datetime import timedelta
import os

from datasets import silver_sptrans_posicoes

host_spark_apps_path = os.environ.get("HOST_SPARK_APPS_DIR")

with DAG(
    dag_id="silver_to_gold",
    start_date=pendulum.datetime(2025, 9, 23, tz="America/Sao_Paulo"),
    schedule=[silver_sptrans_posicoes],
    catchup=False,
    tags=["sptrans", "silver", "gold", "spark", "batch"],
) as dag:
    
    command = (
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0 "
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        "/opt/spark/apps/silver_to_gold_batch.py "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%m') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%d') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') }}"
    )

    # Executa todo o processo
    submit_spark_job_docker  = DockerOperator(
        task_id="submit_silver_to_gold_spark_job",
        image="apache/spark:3.5.7-java17-python3",
        command=command,
        network_mode="fia-projeto-final_sptrans-network",
        auto_remove=True,
        user='root',
        mounts=[
            Mount(
                source=host_spark_apps_path,
                target="/opt/spark/apps",
                type="bind",
            ),
            Mount(
                source="fia-projeto-final_spark_ivy_cache", # Nome do projeto + nome do volume
                target="/root/.ivy2",         # Pasta de cache do Ivy dentro do container
                type="volume"
            )
        ],
        retries=2,
        retry_delay=timedelta(minutes=1)
    )