from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName(f"SPTrans Create Dimension Linha") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "projetofinal") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    print(">>> [DIM_LINHA_LOG]: Sessão Spark iniciada.")

    gtfs_routes_path = "s3a://bronze/gtfs/routes.txt"
    
    schema_routes = StructType([
        StructField("route_id", StringType(), True),
        StructField("agency_id", IntegerType(), True),
        StructField("route_short_name", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_type", IntegerType(), True),
        StructField("route_color", StringType(), True),
        StructField("route_text_color", StringType(), True)
    ])

    print(f">>> [DIM_LINHA_LOG]: Lendo dados de {gtfs_routes_path}")
    df_routes = spark.read.format("csv").option("header", "true").schema(schema_routes).load(gtfs_routes_path)

    # Adiciona uma chave primária numérica e única
    df_dim_linha = df_routes.withColumn("id_linha", monotonically_increasing_id()) \
                            .select(
                                col("id_linha"),
                                col("route_id").alias("letreiro_linha"),
                                col("route_long_name").alias("nome_linha")
                            )
    
    print(">>> [DIM_LINHA_LOG]: Schema final da dim_linha:")
    df_dim_linha.printSchema()
    
    db_properties = {"user": "admin", "password": "projetofinal", "driver": "org.postgresql.Driver"}
    db_url = "jdbc:postgresql://postgres:5432/sptrans_dw"
    table_name = "dim_linha"

    print(f">>> [DIM_LINHA_LOG]: Salvando {df_dim_linha.count()} registros na tabela '{table_name}'...")
    df_dim_linha.write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", table_name).option("user", db_properties["user"]).option("password", db_properties["password"]).option("driver", db_properties["driver"]).save()

    print(">>> [DIM_LINHA_LOG]: Job concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()