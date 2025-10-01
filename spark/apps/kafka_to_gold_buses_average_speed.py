import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, explode, unix_timestamp, when, avg, count, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType, TimestampType
from math import radians, sin, cos, sqrt, atan2
import psycopg2

def log_info(message):
    print(f">>> [SPTRANS_SPEED_STREAM_LOG]: {message}")

def haversine(lon1, lat1, lon2, lat2):
    if None in [lon1, lat1, lon2, lat2]: return 0.0
    R = 6371000
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1; dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def process_and_upsert(df_new_positions, epoch_id):
    log_info(f"Iniciando micro-lote {epoch_id} para cálculo de velocidade.")
    df_new_positions.persist()
    if df_new_positions.isEmpty():
        log_info("Micro-lote vazio. Pulando."); df_new_positions.unpersist(); return

    spark = df_new_positions.sparkSession
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"

    # 1. Lê a última posição conhecida de cada onibus (nossa "memória")
    try:
        df_last_positions = spark.read.jdbc(url=db_url, table="fato_posicao_onibus_atual", properties=db_properties)
    except Exception as e:
        log_info(f"Não foi possível ler a tabela de posições. Pulando o lote. Erro: {e}"); df_new_positions.unpersist(); return

    # 2. Junta os dados novos com os dados históricos
    df_with_history = df_new_positions.join(
        df_last_positions.select(
            col("prefixo_onibus"),
            col("latitude").alias("prev_lat"),
            col("longitude").alias("prev_lon"),
            col("timestamp_captura").alias("prev_ts")
        ), "prefixo_onibus", "inner"
    )

    # 3. Calcula a velocidade apenas para movimentos válidos (timestamp novo > timestamp antigo)
    df_calculations = df_with_history.filter(col("timestamp_captura") > col("prev_ts")) \
        .withColumn("distancia_m", expr("haversine(longitude, latitude, prev_lon, prev_lat)")) \
        .withColumn("tempo_s", unix_timestamp(col("timestamp_captura")) - unix_timestamp(col("prev_ts"))) \
        .withColumn("velocidade_kph", (col("distancia_m") / col("tempo_s")) * 3.6) \
        .withColumn("esta_parado", when((col("distancia_m") < 50) & (col("tempo_s") > 300), 1).otherwise(0)) \
        .filter(col("tempo_s").isNotNull() & (col("tempo_s") > 0))
    
    log_info(f"Calculando KPIs para {df_calculations.count()} eventos de movimento.")
    df_calculations.cache()

    if df_calculations.isEmpty():
        log_info("Nenhum evento de movimento válido neste lote. Pulando."); df_calculations.unpersist(); df_new_positions.unpersist(); return

    df_speed_agg = df_calculations.groupBy("letreiro_linha").agg(avg("velocidade_kph").alias("velocidade_media_kph"))
    df_stopped_agg = df_calculations.filter(col("esta_parado") == 1).groupBy("letreiro_linha").agg(count("*").alias("quantidade_onibus_parados"))

    df_speed_agg.write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "staging_velocidade_media_linha").options(**db_properties).save()
    df_stopped_agg.write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "staging_onibus_parados_linha").options(**db_properties).save()
    
    conn = None
    try:
        conn = psycopg2.connect(host="postgres", dbname="sptrans_dw", user="admin", password="projetofinal")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fato_velocidade_linha (letreiro_linha, velocidade_media_kph)
            SELECT letreiro_linha, velocidade_media_kph FROM staging_velocidade_media_linha
            ON CONFLICT (letreiro_linha) DO UPDATE SET velocidade_media_kph = EXCLUDED.velocidade_media_kph;
        """)
        cur.execute("""
            INSERT INTO fato_onibus_parados_linha (letreiro_linha, quantidade_onibus_parados)
            SELECT letreiro_linha, quantidade_onibus_parados FROM staging_onibus_parados_linha
            ON CONFLICT (letreiro_linha) DO UPDATE SET quantidade_onibus_parados = EXCLUDED.quantidade_onibus_parados;
        """)
        conn.commit()
        cur.close()
        log_info("Comandos MERGE para KPIs operacionais executados com sucesso.")
    except Exception as e:
        log_info(f"Ocorreu um erro durante o MERGE dos KPIs: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    
    log_info(f"Micro-lote {epoch_id} concluído.")
    df_calculations.unpersist()
    df_new_positions.unpersist()

def main():
    spark = SparkSession.builder.appName("SPTrans Realtime Operational KPIs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    spark.udf.register("haversine", haversine, DoubleType())
    log_info("Processador de streaming de KPIs iniciado.")

    # --- LÓGICA DE STREAMING QUE ESTAVA EM FALTA ---
    # Schema completo e preenchido
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
    
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sptrans_posicoes_raw") \
        .option("kafka.group.id", "kpi_speed_consumer_group") \
        .load()

    df_json = df_kafka.select(col("value").cast("string").alias("json"))
    df_parsed = df_json.withColumn("data", from_json(col("json"), schema_principal)).select("data.*")
    
    df_exploded = df_parsed.select(explode(col("l")).alias("linha")) \
        .select(explode(col("linha.vs")).alias("veiculo"), col("linha.c").alias("letreiro_linha"))
    
    df_stream = df_exploded.select(
        col("letreiro_linha"),
        col("veiculo.p").alias("prefixo_onibus"),
        col("veiculo.ta").cast(TimestampType()).alias("timestamp_captura"),
        col("veiculo.py").alias("latitude"),
        col("veiculo.px").alias("longitude")
    ).withWatermark("timestamp_captura", "5 minutes")
    
    query = df_stream.writeStream \
        .foreachBatch(process_and_upsert) \
        .trigger(processingTime='2 minutes') \
        .option("checkpointLocation", "/tmp/spark_checkpoints/kpi_speed") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()