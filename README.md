## Projeto Final - Pipeline de Dados SPTrans

Este projeto implementa um pipeline de dados para coletar e analisar dados da API Olho Vivo da SPTrans.




## Arquitetura

- **Ingestão:** NiFi (https://127.0.0.1:9443/nifi)
- **Data Lake:** MinIO (http://localhost:9001/)
- **Streaming:** Kafka
- **Orquestração:** Airflow (http://localhost:8081/)
- **Transformação:** PySpark (Batch e Streaming)
- **Data Warehouse:** PostgreSQL 
- **Visualização:** Metabase (http://localhost:3000)
  
Visão geral:
<img width="1275" height="795" alt="image" src="https://github.com/user-attachments/assets/98d0a0b6-86db-430f-a5dc-8308b12fc196" />




## Detalhamento das Camadas de Dados
O projeto utiliza arquitetura medalhão, com três camadas de dados:
- **Camada Bronze (Dados Brutos)**
    * MinIO: Arquivos JSON contendo o retorno completo da API /Posicoes, capturados a cada 2 minutos e particionados por ano/mes/dia/hora. Serve como a fonte da verdade para o pipeline histórico (batch).
    * Apache Kafka: Um tópico (sptrans_posicoes_raw) que recebe as mesmas mensagens JSON da API em tempo real. Serve como um buffer de alta performance para os pipelines de streaming.

- **Camada Silver (Dados Limpos)**
   - Arquivos Parquet contendo o histórico detalhado e "achatado" de todas as posições de ônibus, particionados por ano/mes/dia. Esta camada foi otimizada para análises de contagem e não contém dados de geolocalização:
     
     <img width="133" height="160" alt="image" src="https://github.com/user-attachments/assets/03b13267-8a3e-4afe-a558-684db45ad540" />

- **Camada Gold (Dados Agregados e de Servir)**
   - Banco de dados no Postgres com tabelas agregadas para facilitar a geração de dashboards:
  
     <img width="691" height="519" alt="image" src="https://github.com/user-attachments/assets/ef9b22c5-49b1-4a59-981d-a16a55aa755a" />

      * Tabelas de Dimensão: **dim_linha** (descreve as linhas de ônibus) e **dim_tempo** (descreve cada hora de cada dia).
      * Tabelas Fato:
         * **fato_operacao_linhas_hora**: Tabela agregada que armazena a contagem de ônibus por linha a cada hora, seguindo um Modelo Estrela.
         * **fato_posicao_onibus_atual**: Tabela que armazena apenas a última posição conhecida de cada ônibus, otimizada para o mapa em tempo real.
         * **fato_velocidade_linha** e **fato_onibus_parados_linha**: Tabelas que armazenam os KPIs operacionais de velocidade e paradas, também seguindo o Modelo Estrela.




## Detalhamento dos Pipelines
A arquitetura é composta por dois pipelines que processam os mesmos dados de origem para finalidades distintas:

   - **Pipeline de Análise Histórica (Batch)**
   
      * **1) Ingestão (API -> Bronze):** A API /Posicoes da SPTrans, que devolve uma lista aninhada de linhas e veículos, é consultada a cada 2 minutos por um processo no NiFi. A resposta JSON completa é salva simultaneamente em dois destinos: na camada Bronze do MinIO para armazenamento histórico e em um tópico do Apache Kafka para processamento em tempo quase real (*detalhado no segundo pipeline*).
   
      * **2) Transformação (Bronze -> Silver):** Um job Spark (bronze_to_silver_incremental.py), orquestrado por uma DAG no Airflow para rodar de hora em hora, lê todos os JSONs da hora anterior na camada Bronze. O script "achata" a estrutura aninhada através de operações de explode. As colunas letreiro_linha, codigo_linha, prefixo_onibus, acessivel e timestamp_captura_str são gravadas em arquivos Parquet na camada Silver, gerando uma tabela "flat" otimizada para análises históricas.
   
      * **3) Agregação (Silver -> Gold):** Um segundo job Spark (silver_to_gold.py), orquestrado pelo Airflow e acionado pela conclusão do job anterior (via Datasets), lê os arquivos Parquet da camada Silver. A transformação principal agrupa os dados por linha e conta o número de ônibus únicos. Em seguida, ele enriquece esses dados fazendo uma junção com as tabelas dim_linha e dim_tempo para obter as chaves id_linha e id_tempo, formando a tabela fato fato_operacao_linhas_hora. O resultado é salvo no PostgreSQL de forma idempotente, usando uma tabela de staging e um processo de DELETE/INSERT orquestrado pelo Airflow.
   
   - **Pipelines de Tempo Quase Real (Streaming)**
   
      * **1) Processamento de Posições (Kafka -> Gold):** Paralelamente ao fluxo de lote, uma aplicação em PySpark Streaming (silver_to_gold_posicao_atual_onibus.py) roda de forma contínua (24/7). Ela lê os dados brutos do tópico do Kafka, extrai a posição mais recente de cada ônibus em cada micro-lote e atualiza a tabela fato_posicao_onibus_atual no PostgreSQL usando um comando UPSERT (INSERT ... ON CONFLICT). Este pipeline garante que o mapa do Metabase tenha sempre a última localização conhecida de cada veículo.
   
      * **2) Processamento de KPIs Operacionais (Kafka -> Gold):** Um segundo aplicativo PySpark Streaming (calculate_speed_streaming.py) também lê os dados do mesmo tópico do Kafka de forma independente. Este processo é "stateful": ele utiliza a tabela fato_posicao_onibus_atual como "memória" para comparar a posição atual de um ônibus com a sua posição anterior. Com base nessa comparação, ele calcula a velocidade média e identifica ônibus parados no trânsito. Os resultados agregados são então enriquecidos com as chaves das dimensões (id_linha, id_tempo) e salvos nas tabelas fato_velocidade_linha e fato_onibus_parados_linha no PostgreSQL, também usando uma lógica de UPSERT.




## Dashboards

<img width="1887" height="625" alt="image" src="https://github.com/user-attachments/assets/4e503fe1-5f33-46db-b90c-824594e867b7" />

<img width="1837" height="454" alt="image" src="https://github.com/user-attachments/assets/25a20654-31ee-4363-8952-6dac377fb4ab" />

<img width="1230" height="611" alt="image" src="https://github.com/user-attachments/assets/084151b4-de69-4d3b-97ac-fe94491a3c76" />

<img width="1824" height="612" alt="image" src="https://github.com/user-attachments/assets/51cd4243-84c7-4c93-b82a-17ff31ce2098" />

<img width="916" height="533" alt="image" src="https://github.com/user-attachments/assets/b6aa1230-c92e-4855-9eb7-6e50102c3ad4" />

