# Projeto Final - Pipeline de Dados SPTrans

Este projeto implementa um pipeline de dados para coletar e analisar dados da API Olho Vivo da SPTrans.

## Arquitetura

- **Ingestão:** NiFi
- **Data Lake:** MinIO
- **Streaming:** Kafka
- **Transformação:** PySpark (Batch e Streaming)
- **Data Warehouse:** PostgreSQL 
- **Visualização:** Metabase

<img width="1366" height="727" alt="image" src="https://github.com/user-attachments/assets/7eaae8f2-233c-4402-a73d-6efe775e1ea8" />

<img width="1887" height="625" alt="image" src="https://github.com/user-attachments/assets/4e503fe1-5f33-46db-b90c-824594e867b7" />

<img width="1838" height="454" alt="image" src="https://github.com/user-attachments/assets/ba949d9c-3cbe-4380-95b5-285527a5e59d" />

<img width="1230" height="611" alt="image" src="https://github.com/user-attachments/assets/084151b4-de69-4d3b-97ac-fe94491a3c76" />

<img width="1839" height="614" alt="image" src="https://github.com/user-attachments/assets/23234839-cc17-473b-b48c-fe7745fe20e1" />

- **Camada Bronze (Dados Brutos)**

    * MinIO: Arquivos JSON contendo o retorno completo da API /Posicoes, capturados a cada 2 minutos e particionados por ano/mes/dia/hora. Serve como a fonte da verdade para o pipeline histórico (batch).

    * Apache Kafka: Um tópico (sptrans_posicoes_raw) que recebe as mesmas mensagens JSON da API em tempo real. Serve como um buffer de alta performance para o pipeline de streaming.

- **Camada Silver (Dados Limpos)**
  
    * Arquivos Parquet contendo o histórico detalhado e "achatado" de todas as posições de ônibus, particionados por ano/mes/dia. Utilizada para análises históricas.

- **Camada Gold (Dados Agregados e de Servir)**

    * Tabela dm_onibus_por_linha_hora: Tabela agregada que armazena a contagem de ônibus por linha a cada hora. Otimizada para relatórios e KPIs históricos no Metabase.

    * Tabela fato_posicao_onibus_atual: Tabela de fatos que armazena apenas a última posição conhecida de cada ônibus. Otimizada para visualizações de mapa em tempo real.

**Detalhamento dos Pipelines**

A arquitetura é composta por dois pipelines paralelos que processam os mesmos dados de origem para finalidades diferentes:

**Pipeline de Análise Histórica (Batch)**

    1. Ingestão (API -> Bronze): A API /Posicoes da SPTrans, que devolve uma lista aninhada de linhas e veículos, é consultada a cada 2 minutos por um processo no NiFi. A resposta JSON completa é salva simultaneamente em dois destinos: na camada Bronze do MinIO para armazenamento histórico, e em um tópico do Apache Kafka para processamento em tempo real;
    
    2. Transformação (Bronze -> Silver): Um job Spark (bronze_to_silver.py), orquestrado por uma DAG no Airflow para rodar de hora em hora, lê todos os JSONs da hora anterior na camada Bronze. O script "achata" a estrutura aninhada através de operações de explode nas listas de linhas e veículos. As colunas letreiro_linha, codigo_linha, prefixo_onibus, acessivel e timestamp_captura_str são gravadas em arquivos parquet Parquet na camada Silver, gerando uma tabela "flat" otimizada para análises históricas. A camada silver é particionada por ano/mes/dia;
    
    3. Agregação (Silver -> Gold): Um segundo job Spark (silver_to_gold.py), também orquestrado pelo Airflow para rodar logo após a conclusão do anterior, lê os arquivos Parquet da camada Silver e enriquece-os com os nomes das linhas vindos de um arquivo GTFS (nome da linha). A transformação principal agrupa os dados por linha e conta o número  de ônibus únicos (countDistinct). O DataFrame final é salvo no PostgreSQL através de um processo em duas etapas: o Spark sobrescreve uma tabela temporária, e em seguida uma tarefa SQL no Airflow executa um DELETE e INSERT na tabela final dm_onibus_por_linha_hora, garantindo que o processo seja idempotente e não gere dados duplicados.


**Pipeline de Mapa em Tempo Quase Real (Streaming)**

    1. Processamento Contínuo (Kafka -> Gold): Paralelamente ao fluxo de lote, uma aplicação em PySpark Streaming (kafka_to_gold_posicao_atual_onibus.py) roda de forma contínua. Ela "escuta" o tópico do Kafka que recebe os dados brutos do NiFi. A cada micro-lote de dados, o script realiza as transformações de "flatten" em memória, extraindo as informações de latitude e longitude diretamente do JSON. O resultado é salvo na tabela fato_posicao_onibus_atual no PostgreSQL através de um comando UPSERT (INSERT ... ON CONFLICT), garantindo que a tabela contenha sempre apenas a última localização conhecida de cada veículo, o que é ideal para a visualização no mapa em tempo real.



**Comandos úteis**

nifi: https://127.0.0.1:9443/nifi

minio: http://localhost:9001/

airflow: http://localhost:8081/

metabase: http://localhost:3000

docker-compose exec spark-master spark-submit --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/bronze_to_silver.py 2025 09 26 13

docker-compose exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/silver_to_gold_qtde_onibus_por_linha_hora.py 2025 09 26 13

docker-compose exec airflow-scheduler airflow dags trigger --logical-date 2025-09-26T14:05:00Z bronze_to_silver



**Consultas úteis**

docker-compose exec postgres psql -U admin sptrans_dw

select * from dm_onibus_por_linha_hora order by quantidade_onibus desc limit 100;

select count(1) from dm_onibus_por_linha_hora order by quantidade_onibus desc limit 100;

select * from dm_onibus_por_linha_hora WHERE timestamp_hora = '2025-09-25 15:00:00';

select count(1) from staging_dm_onibus_por_linha_hora WHERE timestamp_hora = '2025-09-25 14:00:00';            

SELECT  data_referencia,  hora_referencia,  COUNT(1) AS total_registros FROM  dm_onibus_por_linha_hora GROUP BY  data_referencia,  hora_referencia ORDER BY  data_referencia DESC, hora_referencia DESC;

SELECT  data_referencia,  hora_referencia,  COUNT(1) AS total_registros FROM  staging_dm_onibus_por_linha_hora GROUP BY  data_referencia,  hora_referencia ORDER BY  data_referencia DESC, hora_referencia DESC;
