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
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0 "
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        "/opt/spark/apps/create_dim_linha.py"
    )

    task_create_dim_linha = DockerOperator(
        task_id="spark_job_create_dim_linha",
        image="apache/spark:3.5.7-java17-python3",
        command=spark_command,
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
        ]
    )