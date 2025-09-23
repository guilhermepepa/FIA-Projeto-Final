# Importa as bibliotecas e funções necessárias
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, TimestampType, ArrayType

def main():
    """
    Função principal do nosso job Spark.
    Recebe ano, mes, dia e hora como argumentos para processar uma partição específica.
    """
    # --- Passo 1: Capturar os argumentos da linha de comando ---
    if len(sys.argv) != 5:
        print("Erro: Número incorreto de argumentos.")
        print("Uso: spark-submit bronze_to_silver.py <ano> <mes> <dia> <hora>")
        sys.exit(1)
    
    ano = sys.argv[1]
    mes = sys.argv[2]
    dia = sys.argv[3]
    hora = sys.argv[4]

    # --- Passo 2: Criar a Sessão Spark ---
    spark = SparkSession.builder \
        .appName(f"SPTrans Bronze to Silver - {ano}-{mes}-{dia} {hora}h") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "projetofinal") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    print("Sessão Spark iniciada com sucesso!")

    # --- Passo 3: Definir os caminhos de forma dinâmica ---
    bronze_path = f"s3a://bronze/sptrans/posicao/{ano}/{mes}/{dia}/{hora}/"
    silver_path = "s3a://silver/posicoes_onibus/"
    
    # A definição do Schema continua a mesma
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

    print(f"Lendo dados da camada Bronze de: {bronze_path}")
    df_raw = spark.read.schema(schema_principal).json(bronze_path)

    print(f"Número de registros lidos da camada Bronze: {df_raw.count()}")
    if df_raw.count() == 0:
        print("Nenhum dado encontrado para o período especificado. Encerrando o job.")
        spark.stop()
        sys.exit(0)

    # --- Passo 4: Transformar os dados (mesma lógica de antes) ---
    df_exploded_linhas = df_raw.withColumn("l", explode(col("l")))
    df_exploded_veiculos = df_exploded_linhas.withColumn("vs", explode(col("l.vs")))

    df_silver = df_exploded_veiculos.select(
        col("l.c").alias("letreiro_linha"),
        col("l.cl").alias("codigo_linha"),
        col("vs.p").alias("prefixo_onibus"),
        col("vs.a").alias("acessivel"),
        col("vs.ta").alias("timestamp_captura_str"),
        col("vs.py").alias("latitude"),
        col("vs.px").alias("longitude")
    )
    
    # --- Passo 5: Adicionar colunas de partição e controle ---
    # Convertemos o timestamp e usamos os argumentos para criar as colunas de partição
    df_silver = df_silver.withColumn("timestamp_captura", col("timestamp_captura_str").cast(TimestampType()))
    df_silver = df_silver.withColumn("data_processamento", current_timestamp())
    df_silver = df_silver.withColumn("ano", lit(ano).cast("integer"))
    df_silver = df_silver.withColumn("mes", lit(mes).cast("integer"))
    df_silver = df_silver.withColumn("dia", lit(dia).cast("integer"))

    print("Transformação concluída. Schema final:")
    df_silver.printSchema()

    # --- Passo 6: Salvar na Camada Silver com Overwrite Dinâmico ---
    print(f"Salvando dados transformados na camada Silver em: {silver_path}")
    df_silver.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("ano", "mes", "dia") \
        .save(silver_path)

    print("Job Bronze to Silver concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()