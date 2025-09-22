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



docker-compose exec spark-master spark-submit --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/bronze_to_silver_incremental.py 2025 09 22 18