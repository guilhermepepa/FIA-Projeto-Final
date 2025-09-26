import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, hour, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from pyspark.sql.utils import AnalysisException

def log_info(message):
    """Função auxiliar para imprimir logs formatados e fáceis de encontrar."""
    print(f">>> [SPTRANS_LOG]: {message}")

def main():
    if len(sys.argv) != 5:
        print("Erro: Uso incorreto. Forneça <ano> <mes> <dia> <hora>")
        sys.exit(1)
    
    ano = sys.argv[1]
    mes_str = sys.argv[2]
    dia = sys.argv[3]
    hora_str = sys.argv[4]
    mes = int(mes_str)
    hora = int(hora_str)

    print("\n" + "="*80)
    log_info(f"INICIANDO JOB SILVER-PARA-GOLD")
    log_info(f"Período de processamento: {ano}-{mes_str}-{dia} {hora_str}h (UTC)")
    print("="*80 + "\n")

    spark = SparkSession.builder \
        .appName(f"SPTrans Silver to Gold - {ano}-{mes_str}-{dia} {hora}h") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "projetofinal") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    log_info("Sessão Spark iniciada com sucesso!")

    # Definições de caminhos e conexão com o banco de dados
    silver_path = f"s3a://silver/posicoes_onibus/ano={ano}/mes={mes}/dia={dia}/"
    gtfs_routes_path = "s3a://bronze/gtfs/routes.txt"

    log_info(f"Tentando ler dados da camada Silver de: {silver_path}")
    
    try:
        df_posicoes = spark.read.format("parquet").load(silver_path)
        
        if df_posicoes.count() == 0:
            log_info(f"Pasta encontrada em {silver_path}, mas sem dados. Encerrando com sucesso.")
            spark.stop()
            sys.exit(0)

    except AnalysisException as e:
        if "Path does not exist" in str(e):
            log_info(f"Caminho {silver_path} não encontrado na camada Silver.")
            log_info("Isso é esperado se o job Bronze-para-Silver não processou dados. Encerrando com sucesso.")
            spark.stop()
            sys.exit(0)
        else:
            raise e
    
    df_posicoes_hora = df_posicoes.filter(hour(col("timestamp_captura")) == int(hora))

    if df_posicoes_hora.count() == 0:
        log_info(f"Nenhum dado encontrado na camada Silver para a hora {hora}. Encerrando o job.")
        spark.stop()
        sys.exit(0)
    
    schema_routes = StructType([
        StructField("route_id", StringType(), True),
        StructField("agency_id", IntegerType(), True),
        StructField("route_short_name", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_type", IntegerType(), True),
        StructField("route_color", StringType(), True),
        StructField("route_text_color", StringType(), True)
    ])

    log_info(f"Lendo dados de linhas do GTFS de: {gtfs_routes_path}")
    df_routes = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema_routes) \
        .load(gtfs_routes_path)
    
    log_info("Agregando dados para contar ônibus por linha...")
    df_contagem_onibus = df_posicoes_hora.groupBy("codigo_linha", "letreiro_linha") \
        .agg(countDistinct("prefixo_onibus").alias("quantidade_onibus"))

    log_info("Enriquecendo com nomes das linhas...")
    df_gold_new = df_contagem_onibus.join(
        df_routes, df_contagem_onibus.letreiro_linha == df_routes.route_id, "inner"
    ).select(
        to_date(lit(f"{ano}-{mes_str}-{dia}")).alias("data_referencia"), # Coluna de DATA
        lit(hora).alias("hora_referencia"), # Coluna de HORA (inteiro)
        col("codigo_linha"),
        col("letreiro_linha"),
        col("route_long_name").alias("nome_linha"),
        col("quantidade_onibus")
    )
    
    df_gold_new.cache()
    num_registros_salvos = df_gold_new.count()
    log_info("Novos dados calculados para a camada Gold:")
    df_gold_new.show(10, truncate=False)

    if num_registros_salvos == 0:
        log_info("DataFrame final após o join está vazio. Nenhum dado será salvo no PostgreSQL.")
        spark.stop()
        sys.exit(0)

    log_info(f"Total de {num_registros_salvos} registros agregados serão salvos na tabela de staging.")

    db_properties = {
        "user": "admin",
        "password": "projetofinal",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"

    staging_table_name = "staging_dm_onibus_por_linha_hora"

    log_info(f"Salvando dados na tabela de staging '{staging_table_name}'...")
    df_gold_new.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", staging_table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .save()

    print("\n" + "="*80)
    log_info("JOB SILVER-PARA-GOLD CONCLUÍDO COM SUCESSO!")
    print("="*80 + "\n")
    spark.stop()

if __name__ == "__main__":
    main()