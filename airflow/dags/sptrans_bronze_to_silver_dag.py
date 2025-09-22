from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="sptrans_bronze_to_silver",
    start_date=pendulum.datetime(2025, 9, 22, tz="America/Sao_Paulo"),
    schedule_interval="5 * * * *",
    catchup=False,
    tags=["sptrans", "bronze", "silver", "spark"],
) as dag:
    
    app_args = [
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y') }}",
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%m') }}",
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%d') }}",
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') }}",
    ]

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_bronze_to_silver_spark_job",
        conn_id="spark_default",  # <--- Usa a conexão que definimos via variável de ambiente
        application="/opt/bitnami/spark/apps/bronze_to_silver_incremental.py",
        application_args=app_args,
        conf={
            "spark.driver.extraJavaOptions": "-Divy.home=/tmp",
            "spark.executor.extraJavaOptions": "-Divy.home=/tmp",
        }
    )