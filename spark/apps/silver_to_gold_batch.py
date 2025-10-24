import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.utils import AnalysisException
from delta.tables import *

def log_info(message):
    """Função auxiliar para imprimir logs formatados."""
    print(f">>> [SPTRANS_SILVER_TO_GOLD_BATCH_LOG]: {message}")

def main():
    if len(sys.argv) != 5:
        print("Erro: Uso incorreto. Forneça <ano> <mes> <dia> <hora>")
        sys.exit(1)
    
    ano = sys.argv[1]
    mes_str = sys.argv[2]
    dia_str = sys.argv[3]
    hora_str = sys.argv[4]
    mes = int(mes_str)
    dia = int(dia_str)
    hora = int(hora_str)

    print("\n" + "="*80)
    log_info(f"INICIANDO JOB SILVER-PARA-GOLD (CRIAÇÃO DA TABELA FATO)")
    log_info(f"Período de processamento: {ano}-{mes_str}-{dia} {hora_str}h (UTC)")
    print("="*80 + "\n")


    spark = SparkSession.builder \
        .appName(f"SPTrans Silver to Gold - Fato Operacao {ano}-{mes_str}-{dia} {hora}h") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "projetofinal") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    log_info("Sessão Spark iniciada com sucesso!")

    # Definições de caminhos e conexão
    silver_posicoes_path = f"s3a://silver/posicoes_onibus/ano={ano}/mes={mes}/dia={dia}/"
    silver_kpi_path = "s3a://silver/kpis_historicos_para_processar/"

    gold_path_operacao = "s3a://gold/fato_operacao_linhas_hora"
    gold_path_velocidade = "s3a://gold/fato_velocidade_linha"
    gold_path_parados = "s3a://gold/fato_onibus_parados_linha"

    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    
     # --- Lendo Dimensões ---
    log_info("Lendo dimensões do PostgreSQL...")
    df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties).dropDuplicates(["letreiro_linha"])
    df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)
    
    # Filtra o id_tempo exato que estamos processando
    df_id_tempo_atual = df_dim_tempo.filter(
        (col("ano") == ano) & (col("mes") == mes) & (col("dia") == dia) & (col("hora_referencia") == hora)
    )
    
    if df_id_tempo_atual.isEmpty():
        log_info(f"ERRO: id_tempo não encontrado para a data {ano}-{mes_str}-{dia_str} {hora_str}h. Verifique a dim_tempo."); spark.stop(); sys.exit(1)
        
    id_tempo = df_id_tempo_atual.select("id_tempo").first()[0]
    log_info(f"ID_TEMPO para este lote: {id_tempo}")

    # --- TAREFA 1: Processar Fato de Operação (Contagem) ---
    log_info("Iniciando Tarefa 1: Processamento da 'fato_operacao_linhas_hora'")
    try:
        df_posicoes = spark.read.format("delta").load(silver_posicoes_path)
        # Filtra pela partição E pela hora, para garantir
        df_posicoes_hora = df_posicoes.filter(
            (col("ano") == ano) & (col("mes") == mes) & (col("dia") == dia) & (hour(col("timestamp_captura")) == hora)
        )
        
        record_count = df_posicoes_hora.count()
        if record_count == 0:
            log_info("Nenhum dado de posição encontrado na Camada Silver para o período. Pulando Tarefa 1.")
        else:
            log_info(f"Agregando {record_count} registros de posição...")
            df_contagem = df_posicoes_hora.groupBy("letreiro_linha").agg(countDistinct("prefixo_onibus").alias("quantidade_onibus"))
            df_fato_final = df_contagem.join(df_dim_linha, "letreiro_linha", "inner") \
                                     .withColumn("id_tempo", lit(id_tempo)) \
                                     .select("id_tempo", "id_linha", "quantidade_onibus")
            
            log_info("Executando MERGE na 'fato_operacao_linhas_hora' (MinIO)...")
            DeltaTable.createIfNotExists(spark).location(gold_path_operacao).addColumns(df_fato_final.schema).execute()
            DeltaTable.forPath(spark, gold_path_operacao).alias("gold").merge(
                df_fato_final.alias("updates"),
                "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha"
            ).whenMatchedUpdate(set = { "quantidade_onibus": col("updates.quantidade_onibus") }).whenNotMatchedInsertAll().execute()
            log_info("MERGE no Lakehouse (MinIO) concluído.")
            
    except Exception as e:
        log_info(f"Erro na Tarefa 1: {e}")

    # --- TAREFA 2: Processar Fatos de KPI (Velocidade e Parados) ---
    log_info("Iniciando Tarefa 2: Processamento dos KPIs (Velocidade e Parados)")
    try:
        df_kpis_silver = spark.read.format("delta").load(silver_kpi_path)
        # Filtra os KPIs pré-calculados pelo streaming para a hora exata deste lote
        df_kpis_hora = df_kpis_silver.filter(col("id_tempo") == id_tempo)

        if df_kpis_hora.isEmpty():
            log_info("Nenhum dado de KPI encontrado na Camada Silver para o período. Pulando Tarefa 2.")
        else:
            df_kpis_hora.cache()
            log_info(f"Processando {df_kpis_hora.count()} registros de KPI da Silver...")

            # Prepara os dataframes de KPI, removendo duplicatas por segurança
            df_speed_final = df_kpis_hora.filter(col("velocidade_media_kph").isNotNull()).select("id_tempo", "id_linha", "velocidade_media_kph", "updated_at").dropDuplicates(["id_tempo", "id_linha"])
            df_stopped_final = df_kpis_hora.filter(col("quantidade_onibus_parados").isNotNull()).select("id_tempo", "id_linha", "quantidade_onibus_parados", "updated_at").dropDuplicates(["id_tempo", "id_linha"])

            # MERGE para Velocidade no Lakehouse
            if not df_speed_final.isEmpty():
                log_info("Executando MERGE na 'fato_velocidade_linha' (MinIO)...")
                DeltaTable.createIfNotExists(spark).location(gold_path_velocidade).addColumns(df_speed_final.schema).execute()
                DeltaTable.forPath(spark, gold_path_velocidade).alias("gold").merge(
                    df_speed_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                log_info("MERGE 'fato_velocidade_linha' no Lakehouse concluído.")
            
            # MERGE para Parados no Lakehouse
            if not df_stopped_final.isEmpty():
                log_info("Executando MERGE na 'fato_onibus_parados_linha' (MinIO)...")
                DeltaTable.createIfNotExists(spark).location(gold_path_parados).addColumns(df_stopped_final.schema).execute()
                DeltaTable.forPath(spark, gold_path_parados).alias("gold").merge(
                    df_stopped_final.alias("updates"), "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                log_info("MERGE 'fato_onibus_parados_linha' no Lakehouse concluído.")
            
            df_kpis_hora.unpersist()
    except Exception as e:
        log_info(f"Erro na Tarefa 2: {e}")

    # --- TAREFA 3: CARREGAR DADOS CONSOLIDADOS PARA O POSTGRESQL ---
    log_info("Iniciando Tarefa 3: Carregamento de TODAS as tabelas de fatos históricas para o PostgreSQL.")
    try:
        # 1. Carrega fato_operacao_linhas_hora
        if DeltaTable.isDeltaTable(spark, gold_path_operacao):
            log_info("Carregando 'fato_operacao_linhas_hora' para o PostgreSQL...")
            spark.read.format("delta").load(gold_path_operacao).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_operacao_linhas_hora").option("truncate", "true").options(**db_properties).save()
            log_info("Carregamento de 'fato_operacao_linhas_hora' concluído.")

        # 2. Carrega fato_velocidade_linha
        if DeltaTable.isDeltaTable(spark, gold_path_velocidade):
            log_info("Carregando 'fato_velocidade_linha' para o PostgreSQL...")
            spark.read.format("delta").load(gold_path_velocidade).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_velocidade_linha").option("truncate", "true").options(**db_properties).save()
            log_info("Carregamento de 'fato_velocidade_linha' concluído.")

        # 3. Carrega fato_onibus_parados_linha
        if DeltaTable.isDeltaTable(spark, gold_path_parados):
            log_info("Carregando 'fato_onibus_parados_linha' para o PostgreSQL...")
            spark.read.format("delta").load(gold_path_parados).write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_onibus_parados_linha").option("truncate", "true").options(**db_properties).save()
            log_info("Carregamento de 'fato_onibus_parados_linha' concluído.")
            
    except Exception as e:
        log_info(f"Erro na Tarefa 3 (Carga no PostgreSQL): {e}")

    log_info("Job BATCH Silver-para-Gold concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()