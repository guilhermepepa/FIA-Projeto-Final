from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

host_spark_apps_path = os.environ.get("HOST_SPARK_APPS_DIR")

with DAG(
    dag_id="create_dimensions",
    start_date=pendulum.datetime(2025, 9, 26, tz="America/Sao_Paulo"),
    schedule_interval="@daily", # Roda uma vez por dia
    catchup=False,
    tags=["sptrans", "gold", "dimensions"],
) as dag:
    
    spark_command = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 "
        "/opt/bitnami/spark/apps/create_dim_linha.py"
    )

    task_create_dim_linha = DockerOperator(
        task_id="spark_job_create_dim_linha",
        image="bitnami/spark:3.5",
        command=spark_command,
        network_mode="fia-projeto-final_sptrans-network",
        auto_remove=True,
        user='root',
        mounts=[
            Mount(
                source="/c/Users/guilherme/Desktop/FIA/Docker/FIA-Projeto-Final/spark/apps",
                target="/opt/bitnami/spark/apps",
                type="bind",
            ),
            Mount(
                source="fia-projeto-final_spark_ivy_cache", # Nome do projeto + nome do volume
                target="/root/.ivy2",         # Pasta de cache do Ivy dentro do container
                type="volume"
            )
        ]
    )