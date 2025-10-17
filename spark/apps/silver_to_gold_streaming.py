import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, desc, unix_timestamp, when, avg, count, expr, to_date, lit, current_timestamp, percentile_approx, hour, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, TimestampType
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime

from delta.tables import *

def log_info(message):
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f',{now.microsecond // 1000:03d}'
    print(f"{timestamp} >>> [SPTRANS_SILVER_TO_GOLD_STREAM_LOG]: {message}")

def haversine(lon1, lat1, lon2, lat2):
    if None in [lon1, lat1, lon2, lat2]: return 0.0
    R = 6371000
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1; dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def process_silver_to_gold(df_micro_batch, epoch_id):
    log_info(f"Iniciando micro-lote Silver-para-Gold {epoch_id}...")
    df_micro_batch.persist()
    if df_micro_batch.isEmpty():
        log_info("Micro-lote vazio. Pulando."); df_micro_batch.unpersist(); return

    spark = df_micro_batch.sparkSession
        
    # --- CAMINHOS PARA AS TABELAS GOLD NO DELTA LAKE (MINIO) ---
    gold_path_posicao = "s3a://gold/fato_posicao_onibus_atual"
    gold_path_velocidade = "s3a://gold/fato_velocidade_linha"
    gold_path_parados = "s3a://gold/fato_onibus_parados_linha"
    gold_path_operacao = "s3a://gold/fato_operacao_linhas_hora"

    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}

    # --- TAREFA 1: CALCULAR KPIs de Velocidade e Paradas ---
    log_info("Iniciando Tarefa 1: Cálculo dos KPIs operacionais.")
    df_calculations = spark.createDataFrame([], StructType([])) # Cria um DataFrame vazio para o caso de falha
    try:
        df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties)
        df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)
        df_last_positions = spark.read.format("delta").load(gold_path_posicao)
        
        df_with_history = df_micro_batch.join(
            df_last_positions.select(col("prefixo_onibus"), col("latitude").alias("prev_lat"), col("longitude").alias("prev_lon"), col("timestamp_captura").alias("prev_ts")),
            "prefixo_onibus", "inner"
        )
        
        log_info(f"Join entre lote atual ({df_micro_batch.count()} registros) e histórico ({df_last_positions.count()} registros) resultou em {df_with_history.count()} correspondências.")

        df_with_time_context = df_with_history.withColumn("data_referencia", to_date("timestamp_captura")) \
                                                .withColumn("hora_referencia", hour("timestamp_captura")) \
                                                .join(df_dim_tempo, ["data_referencia", "hora_referencia"], "inner")


        df_calculations = df_with_time_context.filter(col("timestamp_captura") > col("prev_ts")) \
            .withColumn("distancia_m", expr("haversine(longitude, latitude, prev_lon, prev_lat)")) \
            .withColumn("tempo_s", unix_timestamp(col("timestamp_captura")) - unix_timestamp(col("prev_ts"))) \
            .filter(col("tempo_s").isNotNull() & (col("tempo_s") > 10)) \
            .withColumn("velocidade_kph", (col("distancia_m") / col("tempo_s")) * 3.6) \
            .filter(col("velocidade_kph") < 80) \
            .withColumn("esta_parado", 
                    when(
                        (col("periodo_do_dia") != "Madrugada") &
                        (col("distancia_m") < 150) & 
                        (col("tempo_s") > 300) &
                        (col("tempo_s") < 1200), # Adiciona um limite superior de 20 minutos
                        1
                    ).otherwise(0)
                )
        
        df_calculations.cache()
        log_info(f"Calculando KPIs para {df_calculations.count()} eventos de movimento válidos.")

        if not df_calculations.isEmpty():

            df_speed_agg = df_calculations.filter(col("velocidade_kph") > 5).groupBy("letreiro_linha").agg(percentile_approx("velocidade_kph", 0.85).alias("velocidade_media_kph"))
            df_stopped_agg = df_calculations.filter(col("esta_parado") == 1).groupBy("letreiro_linha").agg(countDistinct("prefixo_onibus").alias("quantidade_onibus_parados"))

            df_speed_with_id = df_speed_agg.join(df_dim_linha, "letreiro_linha", "inner").select("id_linha", "velocidade_media_kph")
            df_stopped_with_id = df_stopped_agg.join(df_dim_linha, "letreiro_linha", "inner").select("id_linha", "quantidade_onibus_parados")

            now_df = spark.createDataFrame([ (1,) ]).withColumn("now", current_timestamp())
            current_date = now_df.select(to_date("now")).first()[0]
            current_hour = now_df.select(hour("now")).first()[0]
            
            id_tempo_df = df_dim_tempo.filter((col("data_referencia") == lit(current_date)) & (col("hora_referencia") == lit(current_hour))).select("id_tempo")
            id_tempo = id_tempo_df.first().id_tempo if id_tempo_df.count() > 0 else None

            if id_tempo:
                now_ts = current_timestamp()
                if df_speed_with_id.count() > 0:
                    df_speed_final = df_speed_with_id.withColumn("id_tempo", lit(id_tempo)).withColumn("updated_at", now_ts)
                    DeltaTable.createIfNotExists(spark).location(gold_path_velocidade).addColumns(df_speed_final.schema).execute()
                    delta_velocidade = DeltaTable.forPath(spark, gold_path_velocidade)
                    delta_velocidade.alias("gold").merge(df_speed_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    log_info("MERGE para 'fato_velocidade_linha' no Delta Lake concluído.")
                
                if df_stopped_with_id.count() > 0:
                    df_stopped_final = df_stopped_with_id.withColumn("id_tempo", lit(id_tempo)).withColumn("updated_at", now_ts)
                    DeltaTable.createIfNotExists(spark).location(gold_path_parados).addColumns(df_stopped_final.schema).execute()
                    delta_parados = DeltaTable.forPath(spark, gold_path_parados)
                    delta_parados.alias("gold").merge(df_stopped_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    log_info("MERGE para 'fato_onibus_parados_linha' no Delta Lake concluído.")


    except Exception as e:
        log_info(f"Não foi possível calcular KPIs. A tabela de posições pode não existir ainda. Erro: {e}")

    # --- TAREFA 2: ATUALIZAR fato_posicao_onibus_atual ---
    log_info("Iniciando Tarefa 2: Atualização da 'memória' de posições atuais.")
    windowSpecPos = Window.partitionBy("prefixo_onibus").orderBy(col("timestamp_captura").desc())
    df_latest_positions = df_micro_batch.withColumn("rank", row_number().over(windowSpecPos)) \
                                         .filter(col("rank") == 1) \
                                         .select("prefixo_onibus", "letreiro_linha", "codigo_linha", "latitude", "longitude", "timestamp_captura")
    
    DeltaTable.createIfNotExists(spark).location(gold_path_posicao).addColumns(df_latest_positions.schema).execute()
    delta_posicao_atual = DeltaTable.forPath(spark, gold_path_posicao)
    
    delta_posicao_atual.alias("gold").merge(
        df_latest_positions.alias("updates"),
        "gold.prefixo_onibus = updates.prefixo_onibus"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    log_info("MERGE para 'fato_posicao_onibus_atual' no Delta Lake concluído.")

    
    # --- TAREFA 3: ARQUITETURA LAMBDA - ATUALIZAÇÃO PROVISÓRIA ---
    log_info("Iniciando Tarefa 3: Atualização provisória da tabela de operação (Lambda).")
    df_contagem_provisoria = df_micro_batch.groupBy(hour("timestamp_captura").alias("hora_referencia"), to_date("timestamp_captura").alias("data_referencia"), "letreiro_linha") \
                                           .agg(countDistinct("prefixo_onibus").alias("quantidade_onibus"))
    
    df_dim_linha_lambda = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties)
    df_dim_tempo_lambda = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)
    df_contagem_com_ids = df_contagem_provisoria.join(df_dim_linha_lambda, "letreiro_linha", "inner").join(df_dim_tempo_lambda, ["data_referencia", "hora_referencia"], "inner")
    df_contagem_final = df_contagem_com_ids.select("id_tempo", "id_linha", "quantidade_onibus")

    DeltaTable.createIfNotExists(spark).location(gold_path_operacao).addColumns(df_contagem_final.schema).execute()
    delta_operacao = DeltaTable.forPath(spark, gold_path_operacao)
    delta_operacao.alias("gold").merge(
        df_contagem_final.alias("updates"),
        "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    log_info("MERGE para 'fato_operacao_linhas_hora' no Delta Lake concluído.")

    # --- TAREFA 4: CARREGAR DADOS PARA A CAMADA DE SERVIR (POSTGRES) ---
    log_info("Iniciando Tarefa 4: Carregamento dos dados do Lakehouse para o PostgreSQL.")
    
    # Carrega cada tabela Delta do Gold e a sobrescreve no Postgres, SE a tabela existir
    if DeltaTable.isDeltaTable(spark, gold_path_posicao):
        spark.read.format("delta").load(gold_path_posicao).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_posicao_onibus_atual").options(**db_properties).save()
        log_info("Tabela 'fato_posicao_onibus_atual' carregada no PostgreSQL.")

    if DeltaTable.isDeltaTable(spark, gold_path_velocidade):
        spark.read.format("delta").load(gold_path_velocidade).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_velocidade_linha").options(**db_properties).save()
        log_info("Tabela 'fato_velocidade_linha' carregada no PostgreSQL.")

    if DeltaTable.isDeltaTable(spark, gold_path_parados):
        spark.read.format("delta").load(gold_path_parados).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_onibus_parados_linha").options(**db_properties).save()
        log_info("Tabela 'fato_onibus_parados_linha' carregada no PostgreSQL.")

    
    log_info("Carregamento para o PostgreSQL concluído.")

    log_info(f"Micro-lote {epoch_id} concluído."); df_calculations.unpersist(); df_micro_batch.unpersist()

def main():
    spark = SparkSession.builder \
        .appName("SPTrans Silver to Gold Streaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    spark.udf.register("haversine", haversine, DoubleType())

    log_info("Processador de streaming Silver-para-Gold iniciado.")

    silver_stream_path = "s3a://silver/posicoes_onibus_streaming/"
    
    df_stream = spark.readStream \
        .format("delta") \
        .option("maxFilesPerTrigger", 50) \
        .load(silver_stream_path)
    
    query = df_stream.writeStream \
        .foreachBatch(process_silver_to_gold) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/silver_to_gold") \
        .trigger(processingTime='4 minutes') \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()

