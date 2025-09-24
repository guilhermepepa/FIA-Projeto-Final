# Projeto Final - Pipeline de Dados SPTrans

Este projeto implementa um pipeline de dados para coletar e analisar dados da API Olho Vivo da SPTrans.

## Arquitetura

- **Ingestão:** NiFi
- **Data Lake:** MinIO
- **Transformação:** Spark ??
- **Data Warehouse:** PostgreSQL ??
- **Visualização:** Metabase ??

nifi: https://127.0.0.1:9443/nifi
minio: http://localhost:9001/
airflow: http://localhost:8081/
metabase: http://localhost:3000

<img width="1165" height="632" alt="image" src="https://github.com/user-attachments/assets/f233835a-2f21-402a-916d-9be741f74fe0" />


docker-compose exec spark-master spark-submit --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/bronze_to_silver.py 2025 09 22 18


docker-compose exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/silver_to_gold.py 2025 09 22 18

select * from dm_onibus_por_linha_hora order by quantidade_onibus de
sc limit 100;
