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
    silver_path = f"s3a://silver/posicoes_onibus/ano={ano}/mes={mes}/dia={dia}/"
    gold_path_operacao = "s3a://gold/fato_operacao_linhas_hora"
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    
    # --- 1. LEITURA DOS DADOS ---
    log_info("Lendo dados da camada Silver (Delta) e dimensões.")
    df_posicoes = spark.read.format("delta").load(silver_path)
    df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties).dropDuplicates(["letreiro_linha"])
    df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)

    # --- 2. TRANSFORMAÇÃO E AGREGAÇÃO ---
    df_posicoes_hora = df_posicoes.filter(
        (col("ano") == ano) & (col("mes") == mes) & (col("dia") == dia) & (hour(col("timestamp_captura")) == hora)
    )

    if df_posicoes_hora.count() == 0:
        log_info(f"Nenhum dado na camada Silver para a hora processada. Encerrando."); spark.stop(); sys.exit(0)

    log_info("Agregando dados para a contagem definitiva.")
    df_contagem = df_posicoes_hora.groupBy("letreiro_linha").agg(countDistinct("prefixo_onibus").alias("quantidade_onibus"))
    
    df_com_id_linha = df_contagem.join(df_dim_linha, "letreiro_linha", "inner")
    df_com_data_hora = df_com_id_linha.withColumn("data_referencia", to_date(lit(f"{ano}-{mes_str}-{dia}"))).withColumn("hora_referencia", lit(hora))
    df_fato = df_com_data_hora.join(df_dim_tempo, ["data_referencia", "hora_referencia"], "inner")
    df_fato_final = df_fato.select("id_tempo", "id_linha", "quantidade_onibus")

    # --- 3. MERGE NA CAMADA GOLD DO LAKEHOUSE (MINIO) ---
    log_info("Executando MERGE na tabela 'fato_operacao_linhas_hora' do Delta Lake.")
    DeltaTable.createIfNotExists(spark).location(gold_path_operacao).addColumns(df_fato_final.schema).execute()
    delta_operacao = DeltaTable.forPath(spark, gold_path_operacao)
    
    # Este comando atualiza os registros existentes e insere os novos
    delta_operacao.alias("gold").merge(
        df_fato_final.alias("updates"),
        "gold.id_tempo = updates.id_tempo AND gold.id_linha = updates.id_linha"
    ).whenMatchedUpdate(set = { "quantidade_onibus": col("updates.quantidade_onibus") }).whenNotMatchedInsertAll().execute()
    log_info("MERGE no Delta Lake concluído.")
    
    # --- 4. CARREGAR DADOS PARA A CAMADA DE SERVIR (POSTGRES) ---
    log_info("Carregando dados da tabela Delta Gold para o PostgreSQL.")
    df_gold_delta = spark.read.format("delta").load(gold_path_operacao)
    
    # Sobrescreve a tabela no Postgres com a versão mais recente e completa do Lakehouse
    df_gold_delta.write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", "fato_operacao_linhas_hora").option("truncate", "true").options(**db_properties).save()
    log_info("Carregamento para o PostgreSQL concluído.")

    log_info("Job BATCH Silver-para-Gold concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()