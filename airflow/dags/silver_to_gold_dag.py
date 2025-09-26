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
        "/opt/bitnami/spark/apps/silver_to_gold_qtde_onibus_por_linha_hora.py "
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
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/c/Users/guilherme/Desktop/FIA/Docker/FIA-Projeto-Final/spark/apps",
                target="/opt/bitnami/spark/apps",
                type="bind",
            )
        ]
    )

    # TAREFA 2: Garantir que a tabela final exista com o schema CORRETO
    task_create_gold_table = PostgresOperator(
        task_id="create_gold_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS dm_onibus_por_linha_hora (
                data_referencia DATE,
                hora_referencia INTEGER,
                codigo_linha BIGINT,
                letreiro_linha VARCHAR,
                nome_linha VARCHAR,
                quantidade_onibus BIGINT
            );
        """
    )

    # TAREFA 3: APAGAR os dados da hora correspondente na tabela final
    task_delete_previous_hour = PostgresOperator(
        task_id="delete_data_for_hour",
        postgres_conn_id="postgres_default",
        sql="""
            DELETE FROM dm_onibus_por_linha_hora
            WHERE data_referencia = '{{ (data_interval_end - macros.timedelta(hours=1)).strftime('%Y-%m-%d') }}'
              AND hora_referencia = {{ (data_interval_end - macros.timedelta(hours=1)).strftime('%H') | int }};
        """,
    )

    # TAREFA 4: INSERIR os novos dados da tabela de staging
    task_insert_from_staging = PostgresOperator(
        task_id="insert_data_from_staging",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO dm_onibus_por_linha_hora (
            data_referencia, hora_referencia, codigo_linha, letreiro_linha, nome_linha, quantidade_onibus
            )
            SELECT
            data_referencia,
            hora_referencia::integer,
            codigo_linha,
            letreiro_linha,
            nome_linha,
            quantidade_onibus
            FROM staging_dm_onibus_por_linha_hora;
        """,
    )

    # Define a nova ordem de execução: Spark -> Create -> Delete -> Insert
    task_spark_silver_to_gold >> task_create_gold_table >> task_delete_previous_hour >> task_insert_from_staging
