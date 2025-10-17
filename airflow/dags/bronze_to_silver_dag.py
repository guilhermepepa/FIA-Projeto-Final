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
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.2.0 "
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        "/opt/spark/apps/bronze_to_silver_batch.py "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%m') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%d') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') }}"
    )

    submit_spark_job_docker = DockerOperator(
        task_id="submit_bronze_to_silver_spark_job",
        image="apache/spark:3.5.7-java17-python3",
        command=command,
        network_mode="fia-projeto-final_sptrans-network",
        auto_remove=True,
        user='root',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/c/Users/guilherme/Desktop/FIA/Docker/FIA-Projeto-Final/spark/apps",
                target="/opt/spark/apps",
                type="bind"
            ),
            Mount(
                source="fia-projeto-final_spark_ivy_cache", # Nome do projeto + nome do volume
                target="/root/.ivy2",         # Pasta de cache do Ivy dentro do container
                type="volume"
            )
        ],
        outlets=[silver_sptrans_posicoes]
    )