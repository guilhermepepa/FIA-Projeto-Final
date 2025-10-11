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
   - Arquivos Parquet contendo o histórico detalhado e "achatado" de todas as posições de ônibus, particionados por ano/mes/dia. Esta camada foi otimizada para análises de contagem para a camada batch que funciona de hora em hora e não contém dados de geolocalização:
     
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
   
      * **3) Agregação (Silver -> Gold):** Um segundo job Spark (silver_to_gold.py), orquestrado pelo Airflow e acionado pela conclusão do job anterior (via um Dataset), lê os arquivos Parquet da camada Silver. A transformação principal agrupa os dados por linha e conta o número de ônibus únicos. Em seguida, ele enriquece esses dados fazendo uma junção com as tabelas dim_linha e dim_tempo para obter as chaves id_linha e id_tempo, formando a tabela fato fato_operacao_linhas_hora. O resultado é salvo no PostgreSQL de forma idempotente, usando uma tabela de staging e um processo de DELETE/INSERT orquestrado pelo Airflow.
   
   - **Pipelines de Tempo Quase Real (Streaming)**
   
      * **1) Processamento de Posições (Kafka -> Gold):** Paralelamente ao fluxo de batch, uma aplicação em PySpark Streaming (kafka_to_gold_buses_current_position.py) roda de forma contínua (24/7). Ela lê os dados brutos do tópico do Kafka a cada 2 minutos, extrai a posição mais recente de cada ônibus em cada micro-lote e atualiza a tabela fato_posicao_onibus_atual no PostgreSQL usando um comando UPSERT (INSERT ... ON CONFLICT). Este pipeline garante que o mapa do Metabase tenha sempre a última localização conhecida de cada ônibus.
   
      * **2) Processamento de KPIs Operacionais (Kafka -> Gold):** Uma segunda aplicação PySpark Streaming (kafka_to_gold_buses_average_speed.py) também lê os dados do mesmo tópico do Kafka de forma independente. Este processo utiliza a tabela fato_posicao_onibus_atual como "memória" para comparar a posição atual de um ônibus com a sua posição anterior. Com base nessa comparação, ele calcula a velocidade média e identifica ônibus parados no trânsito. Os resultados agregados são então enriquecidos com as chaves das dimensões (id_linha, id_tempo) e salvos nas tabelas fato_velocidade_linha e fato_onibus_parados_linha no PostgreSQL, também usando uma lógica de UPSERT.




## Dashboards
- Batch
  <img width="1862" height="518" alt="image" src="https://github.com/user-attachments/assets/f74e4b5e-8d51-4e21-b2da-278d5c704502" />
  <img width="1846" height="449" alt="image" src="https://github.com/user-attachments/assets/f6f86c2b-714f-4e73-9281-6bb13b132c33" />
  <img width="1109" height="592" alt="image" src="https://github.com/user-attachments/assets/b284b2f4-d443-48b9-9b6c-14dec1775903" />

- Near real time
  <img width="1845" height="671" alt="image" src="https://github.com/user-attachments/assets/e81c4fe5-5c66-468b-b142-025d51e2af03" />
  <img width="1845" height="599" alt="image" src="https://github.com/user-attachments/assets/d3c86efd-dc59-424d-ae11-975b392549b4" />
  <img width="1847" height="602" alt="image" src="https://github.com/user-attachments/assets/e17b8971-e17f-4f52-a791-401307b625f4" />
