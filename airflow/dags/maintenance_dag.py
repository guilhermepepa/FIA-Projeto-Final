from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

host_spark_apps_path = os.environ.get("HOST_SPARK_APPS_DIR")

with DAG(
    dag_id="delta_lake_maintenance",
    start_date=pendulum.datetime(2025, 9, 23, tz="America/Sao_Paulo"),
    # Roda uma vez por dia, às 4 da manhã (horário de baixa movimentação)
    schedule="0 4 * * *", 
    catchup=False,
    tags=["sptrans", "maintenance", "spark", "delta"],
    doc_md="""
    ### Pipeline de Manutenção do Data Lakehouse
    Esta DAG executa tarefas periódicas para manter a performance das tabelas Delta Lake.
    1. **OPTIMIZE:** Compacta arquivos pequenos em arquivos maiores para acelerar as leituras.
    2. **Z-ORDER:** Reorganiza os dados fisicamente para otimizar as buscas durante operações de `MERGE`.
    3. **VACUUM:** Remove versões antigas e não utilizadas dos arquivos de dados.
    """
) as dag:
    
    command = (
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        '--conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" '
        '--conf "spark.hadoop.fs.s3a.access.key=admin" '
        '--conf "spark.hadoop.fs.s3a.secret.key=projetofinal" '
        '--conf "spark.hadoop.fs.s3a.path.style.access=true" '
        '--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" '
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        '--conf "spark.executor.memory=2g" ' 
        "/opt/spark/apps/optimize_delta_gold_tables.py"
    )

    submit_maintenance_job = DockerOperator(
        task_id="submit_delta_maintenance_job",
        image="apache/spark:3.5.7-java17-python3",
        command=command,
        network_mode="fia-projeto-final_sptrans-network",
        auto_remove=True,
        user='root',
        mounts=[
            Mount(
                source="/c/Users/guilherme/Desktop/FIA/Docker/FIA-Projeto-Final/spark/apps",
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