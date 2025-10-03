from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from docker.types import Mount

from datasets import silver_sptrans_posicoes

with DAG(
    dag_id="silver_to_gold",
    start_date=pendulum.datetime(2025, 9, 23, tz="America/Sao_Paulo"),
    schedule=[silver_sptrans_posicoes],
    catchup=False,
    tags=["sptrans", "silver", "gold", "spark"],
) as dag:
    
    command = (
        "spark-submit "
        "--master spark://spark-master:7077 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 "
        "/opt/bitnami/spark/apps/silver_to_gold.py "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%m') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%d') }} "
        "{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') }}"
    )

    # TAREFA 1: Rodar o Spark para popular a tabela de staging
    task_spark_silver_to_gold  = DockerOperator(
        task_id="submit_silver_to_gold_spark_job",
        image="bitnami/spark:3.5",
        command=command,
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

    # TAREFA 2: Garantir que a tabela FATO final exista
    task_create_fact_table = PostgresOperator(
        task_id="create_fact_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS fato_operacao_linhas_hora (
                id_tempo INTEGER,
                id_linha BIGINT,
                quantidade_onibus BIGINT
            );
        """
    )

    # TAREFA 3: Apagar os dados da hora correspondente na tabela FATO
    task_delete_from_fact = PostgresOperator(
        task_id="delete_from_fact_table",
        postgres_conn_id="postgres_default",
        sql="""
            DELETE FROM fato_operacao_linhas_hora
            WHERE id_tempo IN (
                SELECT id_tempo FROM dim_tempo
                WHERE data_referencia = '{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y-%m-%d') }}'
                  AND hora_referencia = {{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') | int }}
            );
        """,
    )

    # TAREFA 4: Inserir os novos dados da tabela de staging na FATO
    task_insert_into_fact = PostgresOperator(
        task_id="insert_into_fact_table",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO fato_operacao_linhas_hora (id_tempo, id_linha, quantidade_onibus)
            SELECT id_tempo, id_linha, quantidade_onibus
            FROM staging_fato_operacao_linhas_hora;
        """,
    )

    # Define a nova ordem de execução: Spark -> Create -> Delete -> Insert
    task_spark_silver_to_gold >> task_create_fact_table >> task_delete_from_fact >> task_insert_into_fact
