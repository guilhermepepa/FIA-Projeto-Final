import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.utils import AnalysisException

def log_info(message):
    """Função auxiliar para imprimir logs formatados."""
    print(f">>> [SPTRANS_LOG]: {message}")

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
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    staging_table_name = "staging_fato_operacao_linhas_hora"
    
    # --- 1. LEITURA DOS DADOS DE ORIGEM (SILVER E DIMENSÕES) ---
    
    try:
        log_info(f"Lendo dados de posições da camada Silver de: {silver_path}")
        df_posicoes = spark.read.format("parquet").load(silver_path)
        if df_posicoes.count() == 0:
            log_info("Dados da camada Silver vazios. Encerrando."); spark.stop(); sys.exit(0)
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            log_info(f"Caminho {silver_path} não encontrado. Encerrando."); spark.stop(); sys.exit(0)
        else: raise e

    log_info("Lendo dimensão 'dim_linha' do PostgreSQL.")
    df_dim_linha = spark.read.jdbc(url=db_url, table="dim_linha", properties=db_properties)
    
    log_info("Lendo dimensão 'dim_tempo' do PostgreSQL.")
    df_dim_tempo = spark.read.jdbc(url=db_url, table="dim_tempo", properties=db_properties)

    # --- 2. TRANSFORMAÇÃO E AGREGAÇÃO ---

    df_posicoes_hora = df_posicoes.filter(hour(col("timestamp_captura")) == hora)
    if df_posicoes_hora.count() == 0:
        log_info(f"Nenhum dado na camada Silver para a hora {hora}. Encerrando."); spark.stop(); sys.exit(0)
    
    log_info("Agregando dados para contar ônibus por linha...")
    df_contagem_onibus = df_posicoes_hora.groupBy("letreiro_linha") \
        .agg(countDistinct("prefixo_onibus").alias("quantidade_onibus"))

    # --- 3. JUNÇÃO COM AS DIMENSÕES PARA OBTER AS CHAVES ESTRANGEIRAS ---

    log_info("Enriquecendo com chaves das dimensões...")
    
    # Junta com dim_linha para obter id_linha
    df_com_id_linha = df_contagem_onibus.join(df_dim_linha, "letreiro_linha", "inner")
    
    # Adiciona as colunas de data e hora para a junção com dim_tempo
    df_com_data_hora = df_com_id_linha.withColumn("data_referencia", to_date(lit(f"{ano}-{mes_str}-{dia}"))) \
                                      .withColumn("hora_referencia", lit(hora))

    # Junta com dim_tempo para obter id_tempo
    df_fato = df_com_data_hora.join(df_dim_tempo, ["data_referencia", "hora_referencia"], "inner")

    # Seleciona as colunas finais da tabela fato: apenas chaves e métricas
    df_fato_final = df_fato.select(
        col("id_tempo"),
        col("id_linha"),
        col("quantidade_onibus")
    )
    
    df_fato_final.cache()
    num_registros = df_fato_final.count()
    
    log_info("Amostra da Tabela Fato final criada:")
    df_fato_final.show(10)

    if num_registros == 0:
        log_info("Tabela Fato final está vazia. Encerrando."); spark.stop(); sys.exit(0)

    # --- 4. ESCRITA NA CAMADA GOLD (STAGING) ---

    log_info(f"Salvando {num_registros} registros na tabela de staging '{staging_table_name}'...")
    df_fato_final.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", staging_table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .save()
    
    print("\n" + "="*80)
    log_info("JOB CONCLUÍDO COM SUCESSO!")
    print("="*80 + "\n")
    spark.stop()

if __name__ == "__main__":
    main()