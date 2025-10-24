import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, current_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType, TimestampType
from datetime import datetime

def log_info(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f',{now.microsecond // 1000:03d}'
    print(f"{timestamp} >>> [SPTRANS_BRONZE_TO_SILVER_STREAM_LOG]: {message}")

def process_and_log_batch(df_batch, epoch_id):
    """
    Esta função é chamada para cada micro-lote.
    Ela registra as partições e depois escreve os dados.
    """
    log_info(f"Iniciando micro-lote {epoch_id}...")
    df_batch.persist()

    if df_batch.isEmpty():
        log_info("Micro-lote vazio. Pulando.")
        df_batch.unpersist()
        return

    partitions_to_write = df_batch.select("ano", "mes", "dia", "hora").distinct().collect()
    
    log_info(f"Serão gravados/atualizados dados nas seguintes partições:")
    for partition in partitions_to_write:
        log_info(f"  - ano={partition.ano}/mes={partition.mes}/dia={partition.dia}/hora={partition.hora}")
        
    num_records = df_batch.count()
    log_info(f"Total de {num_records} registros a serem salvos.")

    df_batch.write \
        .mode("append") \
        .format("delta") \
        .partitionBy("ano", "mes", "dia", "hora") \
        .save("s3a://silver/posicoes_onibus_streaming/")
        
    log_info(f"Micro-lote {epoch_id} concluído com sucesso.")
    df_batch.unpersist()

def main():
    spark = SparkSession.builder.appName("SPTrans Bronze to Silver Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log_info("Processador de streaming Bronze-para-Silver iniciado.")

    # Schema completo do JSON da API (sem alterações)
    schema_veiculo = StructType([
        StructField("p", LongType(), True), StructField("a", BooleanType(), True),
        StructField("ta", StringType(), True), StructField("py", DoubleType(), True),
        StructField("px", DoubleType(), True), StructField("sv", StringType(), True),
        StructField("is", StringType(), True)
    ])
    schema_linha = StructType([
        StructField("c", StringType(), True), StructField("cl", LongType(), True),
        StructField("sl", LongType(), True), StructField("lt0", StringType(), True),
        StructField("lt1", StringType(), True), StructField("qv", LongType(), True),
        StructField("vs", ArrayType(schema_veiculo), True)
    ])
    schema_principal = StructType([
        StructField("hr", StringType(), True),
        StructField("l", ArrayType(schema_linha), True)
    ])

    # Leitura do Kafka e transformações (sem alterações)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sptrans_posicoes_raw") \
        .option("kafka.group.id", "bronze_to_silver_consumer_group") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 20000) \
        .load()

    df_json = df_kafka.select(col("value").cast("string").alias("json"))
    df_parsed = df_json.withColumn("data", from_json(col("json"), schema_principal)).select("data.*")
    df_exploded = df_parsed.select(explode(col("l")).alias("linha")) \
        .select(explode(col("linha.vs")).alias("veiculo"), col("linha.c").alias("letreiro_linha"), col("linha.cl").alias("codigo_linha"))
    
    df_silver = df_exploded.select(
        col("letreiro_linha"),
        col("codigo_linha"),
        col("veiculo.p").alias("prefixo_onibus"),
        col("veiculo.a").alias("acessivel"),
        col("veiculo.ta").cast(TimestampType()).alias("timestamp_captura"),
        col("veiculo.py").alias("latitude"),
        col("veiculo.px").alias("longitude")
    )
    
    df_silver_partitioned = df_silver.withColumn("ano", year(col("timestamp_captura"))) \
                                     .withColumn("mes", month(col("timestamp_captura"))) \
                                     .withColumn("dia", dayofmonth(col("timestamp_captura"))) \
                                     .withColumn("hora", hour(col("timestamp_captura")))

    # Inicia a query usando .foreachBatch para chamar a função e logar
    query = df_silver_partitioned.writeStream \
        .foreachBatch(process_and_log_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/bronze_to_silver") \
        .trigger(processingTime='1 minutes') \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()