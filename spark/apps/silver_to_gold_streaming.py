import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, desc, unix_timestamp, when, avg, count, expr, to_date, lit, current_timestamp, percentile_approx, hour, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, TimestampType
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime
from delta.tables import *
import psycopg2 

db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
pg_conn_string = "host='postgres' dbname='sptrans_dw' user='admin' password='projetofinal'"

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

def upsert_postgres(df, table_name, conflict_columns, update_columns):
    """
    Função otimizada para fazer UPSERT no PostgreSQL.
    Escreve para uma tabela de staging e executa um MERGE nativo do Postgres.
    """
    temp_table = f"staging_{table_name}"

    if df is None or df.isEmpty():
        log_info(f"DataFrame para a tabela '{table_name}' está vazio. Pulando UPSERT.")
        return
    
    df.write.mode("overwrite").format("jdbc") \
      .option("url", db_url) \
      .option("dbtable", temp_table) \
      .options(**db_properties) \
      .save()

    cols_str = ", ".join([f'"{c}"' for c in df.columns])
    conflict_str = ", ".join([f'"{c}"' for c in conflict_columns])
    update_set_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_columns])
    
    sql_merge = f"""
        INSERT INTO {table_name} ({cols_str})
        SELECT {cols_str} FROM {temp_table}
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_set_str};
    """
    
    conn = None
    try:
        conn = psycopg2.connect(pg_conn_string)
        cur = conn.cursor()
        cur.execute(sql_merge)
        conn.commit()
        cur.close()
        log_info(f"UPSERT para a tabela '{table_name}' no PostgreSQL concluído.")
    except Exception as e:
        log_info(f"Erro no UPSERT para '{table_name}': {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def overwrite_postgres(df, table_name):
    """
    Função otimizada para fazer TRUNCATE + INSERT no PostgreSQL.
    Usada para tabelas de snapshot NRT (Near Real Time) como 'nrt_velocidade_linha'.
    """
    if df is None:
        log_info(f"DataFrame para OVERWRITE na tabela '{table_name}' é None. Pulando.")
        return
    
    # Se o DataFrame estiver vazio, nós ainda queremos truncar a tabela.
    if df.isEmpty():
        log_info(f"DataFrame para '{table_name}' está vazio. Truncando a tabela de destino...")
        conn = None
        try:
            conn = psycopg2.connect(pg_conn_string); cur = conn.cursor(); cur.execute(f"TRUNCATE TABLE {table_name};"); conn.commit(); cur.close()
            log_info(f"TRUNCATE da tabela '{table_name}' no PostgreSQL concluído.")
        except Exception as e:
            log_info(f"Erro no TRUNCATE para '{table_name}': {e}");
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
    else:
        # Se o DataFrame tiver dados, o modo "overwrite" com "truncate" é mais eficiente.
        df.write.mode("overwrite").format("jdbc") \
          .option("url", db_url) \
          .option("dbtable", table_name) \
          .option("truncate", "true") \
          .options(**db_properties) \
          .save()
        log_info(f"OVERWRITE (Truncate + Insert) para a tabela '{table_name}' no PostgreSQL concluído.")


def process_silver_to_gold(df_micro_batch, epoch_id):
    log_info(f"Iniciando micro-lote Silver-para-Gold {epoch_id}...")
    df_micro_batch.persist()
    if df_micro_batch.isEmpty():
        log_info("Micro-lote vazio. Pulando."); df_micro_batch.unpersist(); return

    spark = df_micro_batch.sparkSession
    
    # Caminhos do Lakehouse (Fonte da Verdade dos KPIs)
    gold_path_velocidade = "s3a://gold/fato_velocidade_linha"
    gold_path_parados = "s3a://gold/fato_onibus_parados_linha"
    
    df_speed_final = None
    df_stopped_final = None
    df_speed_with_id = None
    df_stopped_with_id = None
    df_calculations = spark.createDataFrame(spark.sparkContext.emptyRDD(), df_micro_batch.schema.add("periodo_do_dia", StringType()))

    # --- TAREFA 1: CÁLCULO DE KPIs (USA O POSTGRES COMO 'MEMÓRIA' ANTIGA) ---
    log_info("Iniciando Tarefa 1: Cálculo dos KPIs operacionais.")
    try:
        # Carrega dimensões (pequenas, rápido)
        df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties)
        df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)
        
        # Lê a "memória" de posições do PostgreSQL (muito mais rápido que ler tabela Delta fragmentada)
        log_info("Lendo 'memória' de posições do PostgreSQL...")
        df_last_positions = spark.read.jdbc(url=db_url, table="fato_posicao_onibus_atual", properties=db_properties)
        
        # Junta lote atual com a "memória"
        df_with_history = df_micro_batch.join(df_last_positions.select("prefixo_onibus", col("latitude").alias("prev_lat"), col("longitude").alias("prev_lon"), col("timestamp_captura").alias("prev_ts")), "prefixo_onibus", "inner")
        log_info(f"Join com histórico do Postgres resultou em {df_with_history.count()} correspondências.")

        # Enriquece com dados da dim_tempo
        df_with_time_context = df_with_history.withColumn("data_referencia", to_date("timestamp_captura")).withColumn("hora_referencia", hour("timestamp_captura")).join(df_dim_tempo, ["data_referencia", "hora_referencia"], "inner")

        # Calcula Distância, Tempo e Velocidade
        df_calculations = df_with_time_context.filter(col("timestamp_captura") > col("prev_ts")) \
            .withColumn("distancia_m", expr("haversine(longitude, latitude, prev_lon, prev_lat)")) \
            .withColumn("tempo_s", unix_timestamp(col("timestamp_captura")) - unix_timestamp(col("prev_ts"))) \
            .filter(col("tempo_s").isNotNull() & (col("tempo_s") > 10)) \
            .withColumn("velocidade_kph", (col("distancia_m") / col("tempo_s")) * 3.6) \
            .filter(col("velocidade_kph") < 80) \
            .withColumn("esta_parado", 
                when(
                    (col("velocidade_kph") < 2.0) &
                    (col("periodo_do_dia") != "Madrugada"),
                    1
                ).otherwise(0)
                )
               
        df_calculations.cache()
        log_info(f"Total de eventos de movimento válidos: {df_calculations.count()}.")

        df_speed_agg = df_calculations.filter(col("velocidade_kph") > 5).groupBy("letreiro_linha").agg(percentile_approx("velocidade_kph", 0.85).alias("velocidade_media_kph"))
        df_stopped_agg = df_calculations.filter(col("esta_parado") == 1).groupBy("letreiro_linha").agg(countDistinct("prefixo_onibus").alias("quantidade_onibus_parados"))
        log_info(f"Linhas com velocidade calculada: {df_speed_agg.count()}. Linhas com ônibus parados: {df_stopped_agg.count()}.")
        
        # DataFrames para NRT (Near Real Time)
        df_speed_with_id = df_speed_agg.join(df_dim_linha, "letreiro_linha", "inner").select(col("id_linha"), col("velocidade_media_kph")).cache()
        df_stopped_with_id = df_stopped_agg.join(df_dim_linha, "letreiro_linha", "inner").select(col("id_linha"), col("quantidade_onibus_parados")).cache()

        # DataFrames para Histórico (Lakehouse)
        now_df = spark.createDataFrame([ (1,) ]).withColumn("now", current_timestamp()); current_date = now_df.select(to_date("now")).first()[0]; current_hour = now_df.select(hour("now")).first()[0]
        id_tempo_df = df_dim_tempo.filter((col("data_referencia") == lit(current_date)) & (col("hora_referencia") == lit(current_hour))).select("id_tempo")
        id_tempo = id_tempo_df.first().id_tempo if not id_tempo_df.isEmpty() else None

        if id_tempo:
            now_ts = current_timestamp()
            if not df_speed_with_id.isEmpty():
                df_speed_final = df_speed_with_id.withColumn("id_tempo", lit(id_tempo)).withColumn("updated_at", now_ts)
                DeltaTable.createIfNotExists(spark).location(gold_path_velocidade).addColumns(df_speed_final.schema).execute()
                DeltaTable.forPath(spark, gold_path_velocidade).alias("gold").merge(df_speed_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                log_info("MERGE para 'fato_velocidade_linha' no Delta Lake concluído.")
            
            if not df_stopped_with_id.isEmpty():
                df_stopped_final = df_stopped_with_id.withColumn("id_tempo", lit(id_tempo)).withColumn("updated_at", now_ts)
                DeltaTable.createIfNotExists(spark).location(gold_path_parados).addColumns(df_stopped_final.schema).execute()
                DeltaTable.forPath(spark, gold_path_parados).alias("gold").merge(df_stopped_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                log_info("MERGE para 'fato_onibus_parados_linha' no Delta Lake concluído.")
    except Exception as e:
        log_info(f"Não foi possível calcular KPIs. Erro: {e}")

    
    # --- TAREFA 2: ATUALIZAR 'MEMÓRIA' DE POSIÇÕES ATUAIS (POSTGRES) ---
    log_info("Iniciando Tarefa 2: Atualização da 'memória' de posições atuais no PostgreSQL.")
    windowSpecPos = Window.partitionBy("prefixo_onibus").orderBy(col("timestamp_captura").desc())
    df_latest_positions = df_micro_batch.withColumn("rank", row_number().over(windowSpecPos)).filter(col("rank") == 1).select("prefixo_onibus", "letreiro_linha", "codigo_linha", "latitude", "longitude", "timestamp_captura").cache()
    
    # Em vez de MERGE no Delta, fazemos UPSERT no Postgres (mais rápido)
    upsert_postgres(df_latest_positions, 'fato_posicao_onibus_atual', ['prefixo_onibus'], ['letreiro_linha', 'codigo_linha', 'latitude', 'longitude', 'timestamp_captura'])
    
    # --- TAREFA 3: CARREGAR KPIs NRT PARA O POSTGRESQL ---
    log_info("Iniciando Tarefa 3: Carregamento de KPIs NRT para o PostgreSQL.")
    
    # Envia os DataFrames de KPI (sem id_tempo) direto para as novas tabelas NRT
    if df_speed_with_id is not None:
        overwrite_postgres(df_speed_with_id, 'nrt_velocidade_linha')

    if df_stopped_with_id is not None:
        overwrite_postgres(df_stopped_with_id, 'nrt_onibus_parados_linha')

    if df_speed_final is not None:
        upsert_postgres(df_speed_final, 'fato_velocidade_linha', ['id_tempo', 'id_linha'], ['velocidade_media_kph', 'updated_at'])
    if df_stopped_final is not None:
        upsert_postgres(df_stopped_final, 'fato_onibus_parados_linha', ['id_tempo', 'id_linha'], ['quantidade_onibus_parados', 'updated_at'])

    log_info("Carregamento de KPIs NRT e Históricos para o PostgreSQL concluído.")

    # --- Limpeza ---
    if df_calculations.is_cached: df_calculations.unpersist()
    if df_latest_positions.is_cached: df_latest_positions.unpersist()
    if df_speed_with_id is not None and df_speed_with_id.is_cached: df_speed_with_id.unpersist()
    if df_stopped_with_id is not None and df_stopped_with_id.is_cached: df_stopped_with_id.unpersist()
    if df_speed_final is not None and df_speed_final.is_cached: df_speed_final.unpersist()
    if df_stopped_final is not None and df_stopped_final.is_cached: df_stopped_final.unpersist()
    df_micro_batch.unpersist()
    
    log_info(f"Micro-lote {epoch_id} concluído.")

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
        .trigger(processingTime='2 minutes') \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()

