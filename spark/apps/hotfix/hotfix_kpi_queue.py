import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, unix_timestamp, when, lit, to_date, hour, percentile_approx, countDistinct, row_number, current_timestamp
from math import radians, sin, cos, sqrt, atan2
from delta.tables import *
from datetime import datetime

def log_info(message):
    """Função auxiliar para imprimir logs formatados."""
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S') + f',{now.microsecond // 1000:03d}'
    print(f"{timestamp} >>> [SPTRANS_HOTFIX_KPI_QUEUE_LOG]: {message}")

def haversine(lon1, lat1, lon2, lat2):
    """Função Haversine para calcular distância."""
    if None in [lon1, lat1, lon2, lat2]: return 0.0
    R = 6371000
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1; dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def main():
    
    # --- Data e Horas para Corrigir (UTC) ---
    # Hoje é 2025-11-04. As horas 15, 16, 17 (BRT) são 18, 19, 20 (UTC).
    # Ajuste se o seu fuso horário for diferente.
    ANO_CORRIGIR = 2025
    MES_CORRIGIR = 11
    DIA_CORRIGIR = 4
    HORAS_CORRIGIR = [18, 19, 20] # 15, 16, 17 BRT = 18, 19, 20 UTC

    print("\n" + "="*80)
    log_info(f"INICIANDO JOB HOTFIX (REPARO) DA FILA DE KPIs")
    log_info(f"Período de processamento: {ANO_CORRIGIR}-{MES_CORRIGIR}-{DIA_CORRIGIR} Horas UTC: {HORAS_CORRIGIR}")
    print("="*80 + "\n")

    spark = SparkSession.builder \
        .appName(f"SPTrans Hotfix KPI Queue") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "projetofinal") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    spark.udf.register("haversine", haversine, "double")
    log_info("Sessão Spark iniciada com sucesso!")

    # --- Caminhos ---
    silver_stream_path = "s3a://silver/posicoes_onibus_streaming/"
    silver_kpi_path = "s3a://silver/kpis_historicos_para_processar/"
    
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}

    # --- 1. Ler Dimensões (COM A CORREÇÃO) ---
    log_info("Lendo dimensões do PostgreSQL...")
    
    df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties) \
                        .dropDuplicates(["letreiro_linha"])
                        
    df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)
    
    # --- 2. Encontrar os id_tempo que precisamos corrigir ---
    df_id_tempos_corrigir = df_dim_tempo.filter(
        (col("ano") == ANO_CORRIGIR) & 
        (col("mes") == MES_CORRIGIR) & 
        (col("dia") == DIA_CORRIGIR) & 
        (col("hora_referencia").isin(HORAS_CORRIGIR))
    )
    
    lista_id_tempos = [row.id_tempo for row in df_id_tempos_corrigir.select("id_tempo").collect()]
    if not lista_id_tempos:
        log_info("ERRO: Nenhum id_tempo encontrado para as datas/horas especificadas. Verifique a dim_tempo."); spark.stop(); sys.exit(1)
        
    log_info(f"IDs de Tempo a serem corrigidos: {lista_id_tempos}")

    # --- 3. Recalcular KPIs (Lógica do Streaming, mas em Batch) ---
    log_info("Lendo dados brutos de posição (posicoes_onibus_streaming)...")
    df_posicoes_streaming = spark.read.format("delta").load(silver_stream_path)
    
    # Filtra apenas as partições/horas que queremos corrigir
    df_posicoes_filtradas = df_posicoes_streaming.filter(
        (col("ano") == ANO_CORRIGIR) & 
        (col("mes") == MES_CORRIGIR) & 
        (col("dia") == DIA_CORRIGIR) & 
        (col("hora").isin(HORAS_CORRIGIR))
    )
    
    # Precisamos da "memória" do Postgres para o cálculo de Haversine
    df_last_positions = spark.read.jdbc(url=db_url, table="nrt_posicao_onibus_atual", properties=db_properties)

    log_info("Iniciando recálculo dos KPIs...")
    df_with_history = df_posicoes_filtradas.join(
        df_last_positions.select("prefixo_onibus", col("latitude").alias("prev_lat"), col("longitude").alias("prev_lon"), col("timestamp_captura").alias("prev_ts")), 
        "prefixo_onibus", "inner")

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
                ( (col("hora_referencia") < 4) | (col("hora_referencia") > 7) ) & (col("velocidade_kph") < 2.0),
                1
            ).otherwise(0)
         )

    # --- 4. Agregar os KPIs (Agora com a dim_linha CORRIGIDA) ---
    log_info("Agregando KPIs recalculados...")
    df_speed_agg = df_calculations.filter(col("velocidade_kph") > 5).groupBy("letreiro_linha", "id_tempo").agg(percentile_approx("velocidade_kph", 0.85).alias("velocidade_media_kph"))
    df_stopped_agg = df_calculations.filter(col("esta_parado") == 1).groupBy("letreiro_linha", "id_tempo").agg(countDistinct("prefixo_onibus").alias("quantidade_onibus_parados"))

    # Juntar com a dim_linha CORRIGIDA (com dropDuplicates)
    df_speed_final = df_speed_agg.join(df_dim_linha, "letreiro_linha", "inner") \
                                 .withColumn("updated_at", current_timestamp()) \
                                 .select("id_tempo", "id_linha", "velocidade_media_kph", "updated_at")
    
    df_stopped_final = df_stopped_agg.join(df_dim_linha, "letreiro_linha", "inner") \
                                   .withColumn("updated_at", current_timestamp()) \
                                   .select("id_tempo", "id_linha", "quantidade_onibus_parados", "updated_at")

    # Unir os dois KPIs para uma única escrita
    df_kpis_unidos_corrigidos = df_speed_final.join(
        df_stopped_final,
        ["id_tempo", "id_linha", "updated_at"],
        "full_outer"
    )

    log_info(f"Total de {df_kpis_unidos_corrigidos.count()} registros de KPI corrigidos encontrados.")

    # --- 5. Sobrescrever atomicamente a "fila" Silver ---
    if not df_kpis_unidos_corrigidos.isEmpty():
        log_info(f"Sobrescrevendo partições na fila Silver ({silver_kpi_path})...")
        
        # Converte a lista de IDs em string SQL (ex: '16020, 16021, 16022')
        ids_sql_string = ", ".join(map(str, lista_id_tempos))
        
        df_kpis_unidos_corrigidos.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"id_tempo IN ({ids_sql_string})") \
            .save(silver_kpi_path)
            
        log_info("Hotfix concluído! A fila Silver 'kpis_historicos_para_processar' foi corrigida.")
    else:
        log_info("Nenhum dado de KPI foi gerado pelo hotfix. Verifique os dados de entrada.")

    spark.stop()

if __name__ == "__main__":
    main()