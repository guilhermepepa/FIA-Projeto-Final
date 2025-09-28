#!/bin/bash
set -e # Sair imediatamente se um comando falhar

echo ">>> [STREAMING_PROCESSOR]: A instalar dependências Python..."
pip install psycopg2-binary

echo ">>> [STREAMING_PROCESSOR]: A executar spark-submit..."
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --total-executor-cores 1 \
  /opt/bitnami/spark/apps/kafka_to_gold_posicao_atual_onibus.py