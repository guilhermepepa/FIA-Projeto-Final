#!/bin/bash
set -e # Sair imediatamente se um comando falhar

echo ">>> [STREAMING_PROCESSOR - SILVER TO GOLD]: Instalando dependências Python..."
pip install psycopg2-binary

echo ">>> [STREAMING_PROCESSOR - SILVER TO GOLD]: Executando spark-submit..."
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=projetofinal" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.cores.max=10" \
  /opt/spark/apps/silver_to_gold_streaming.py