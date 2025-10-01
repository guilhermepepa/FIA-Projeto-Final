from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, explode, row_number, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType, TimestampType
import psycopg2

def log_info(message):
    """Função auxiliar para imprimir logs formatados."""
    print(f">>> [SPTRANS_STREAMING_LOG]: {message}")

def upsert_to_postgres(df, epoch_id):
    log_info(f"Iniciando micro-lote {epoch_id}...")
    df.persist()

    if df.isEmpty():
        log_info("Micro-lote vazio. Pulando.")
        df.unpersist()
        return

    windowSpec = Window.partitionBy("prefixo_onibus").orderBy(col("timestamp_captura").desc())
    df_latest_positions = df.withColumn("rank", row_number().over(windowSpec)) \
                                     .filter(col("rank") == 1) \
                                     .select("prefixo_onibus", "letreiro_linha", "codigo_linha", "latitude", "longitude", "timestamp_captura")
    
    log_info(f"Processando {df_latest_positions.count()} posições de ônibus atualizadas.")
    
    # Etapa 1: O Spark sobrescreve a tabela de staging
    df_latest_positions.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/sptrans_dw") \
        .option("dbtable", "staging_posicao_onibus_atual") \
        .option("user", "admin") \
        .option("password", "projetofinal") \
        .option("driver", "org.postgresql.Driver") \
        .save()

    # --- MUDANÇA PRINCIPAL AQUI: Usando psycopg2 para o MERGE ---
    conn = None
    try:
        log_info("Conectando ao PostgreSQL para executar o MERGE.")
        conn = psycopg2.connect(host="postgres", dbname="sptrans_dw", user="admin", password="projetofinal")
        cur = conn.cursor()
        
        merge_sql = """
            INSERT INTO fato_posicao_onibus_atual (prefixo_onibus, letreiro_linha, codigo_linha, latitude, longitude, timestamp_captura)
            SELECT prefixo_onibus, letreiro_linha, codigo_linha, latitude, longitude, timestamp_captura
            FROM staging_posicao_onibus_atual
            ON CONFLICT (prefixo_onibus)
            DO UPDATE SET
                letreiro_linha = EXCLUDED.letreiro_linha,
                codigo_linha = EXCLUDED.codigo_linha,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                timestamp_captura = EXCLUDED.timestamp_captura;
        """
        cur.execute(merge_sql)
        conn.commit()
        cur.close()
        log_info("Comando MERGE executado com sucesso.")
    except Exception as e:
        log_info(f"Ocorreu um erro durante o MERGE: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    log_info(f"Micro-lote {epoch_id} concluído.")
    df.unpersist()

def main():
    spark = SparkSession.builder.appName("SPTrans Realtime Position Processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log_info("Processador de streaming iniciado. Aguardando dados do Kafka...")

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
    
    df_kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "sptrans_posicoes_raw").load()

    df_json = df_kafka.select(col("value").cast("string").alias("json"))
    df_parsed = df_json.withColumn("data", from_json(col("json"), schema_principal)).select("data.*")
    df_exploded_linhas = df_parsed.withColumn("l", explode(col("l")))
    df_exploded_veiculos = df_exploded_linhas.withColumn("vs", explode(col("l.vs")))
    df_final = df_exploded_veiculos.select(
        col("l.c").alias("letreiro_linha"),
        col("l.cl").alias("codigo_linha"),
        col("vs.p").alias("prefixo_onibus"),
        col("vs.a").alias("acessivel"),
        col("vs.ta").cast(TimestampType()).alias("timestamp_captura"),
        col("vs.py").alias("latitude"),
        col("vs.px").alias("longitude")
    )
    
    query = df_final.writeStream.foreachBatch(upsert_to_postgres).trigger(processingTime='2 minutes').start()
    query.awaitTermination()

if __name__ == "__main__":
    main()