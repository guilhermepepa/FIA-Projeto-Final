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
- **API:** FastAPI (http://localhost:8002)
  
Visão geral:
<img width="2043" height="816" alt="image" src="https://github.com/user-attachments/assets/19f462af-a323-4396-8e0f-e8779e7430e5" />




## Detalhamento das Camadas de Dados
O projeto utiliza a Arquitetura Lakehouse Medalhão. A estrutura é dividida em três camadas principais:
- **Camada Bronze (Dados Brutos)**
  
    Esta camada funciona como o repositório de entrada para todos os dados em seu formato original, sem nenhuma limpeza ou transformação.
    * MinIO (Data Lake)
      * Posições da Frota: Arquivos JSON contendo o retorno completo da API /Posicoes, capturados a cada 2 minutos e particionados por ano/mes/dia/hora. Serve como a fonte para o pipeline histórico (batch).
      * Dados Cadastrais: Arquivos estáticos do padrão GTFS (como o routes.txt), que fornecem os nomes e detalhes das linhas de ônibus.
    * Apache Kafka
      * Tópico sptrans_posicoes_raw: Recebe as mesmas mensagens JSON da API em tempo real. Serve como um buffer de alta performance para alimentar os pipelines de streaming.

- **Camada Silver (Dados Limpos)**
  
    A Camada Silver transforma os dados brutos em um formato otimizado e confiável. Todos os dados aqui são armazenados como Tabelas Delta Lake, garantindo transações ACID e qualidade.
    * Tabela posicoes_onibus (Batch)
      Localização: s3a://silver/posicoes_onibus
      Conteúdo: Contém o histórico detalhado e "achatado" de todas as posições de ônibus. Os dados são limpos, com tipos corrigidos, e particionados por ano/mes/dia. É a fonte para o pipeline de lote (batch).

    * Tabela posicoes_onibus_streaming (Streaming)
      Localização: s3a://silver/posicoes_onibus_streaming
      Conteúdo: Versão da tabela de posições otimizada para o fluxo de tempo real. Contém todos os campos necessários, incluindo dados de geolocalização (latitude/longitude), e é particionada por ano/mes/dia/hora para leituras incrementais com maior eficiência.

      <img width="587" height="349" alt="image" src="https://github.com/user-attachments/assets/853912c4-9fa5-4da6-a8c6-570268b9e68a" />


- **Camada Gold (Dados Agregados e de Negócio)**

  A Camada Gold é dividida em duas partes:
    * Lakehouse (MinIO) - A Fonte da Verdade Agregada
      É aqui que os dados de negócio são consolidados e armazenados como Tabelas Delta Lake. Os pipelines Spark executam as operações de agregação e MERGE diretamente nestas tabelas.
      - Tabelas Fato: fato_operacao_linhas_hora, fato_posicao_onibus_atual, fato_velocidade_linha e fato_onibus_parados_linha. Elas contêm os KPIs e métricas consolidadas, servindo como a fonte única da verdade para a camada de servir.

    * Camada de Servir (PostgreSQL) - Otimizada para Consumo
      Este é o nosso Data Warehouse, otimizado para consultas rápidas. As tabelas aqui são cópias dos dados da camada Gold do Lakehouse, carregadas ao final de cada pipeline para alimentar a API e os dashboards no Metabase com baixa latência.
      - Tabelas de Dimensão: dim_linha (descreve as linhas de ônibus) e dim_tempo (descreve cada hora de cada dia).
      - Tabelas Fato: Contêm as mesmas métricas das tabelas do Lakehouse, mas em um formato relacional para acesso rápido.
 
       <img width="691" height="519" alt="image" src="https://github.com/user-attachments/assets/ef9b22c5-49b1-4a59-981d-a16a55aa755a" />


## Detalhamento dos Pipelines
A arquitetura é composta por dois pipelines principais que operam em conjunto: um pipeline de lote (batch) para garantir a precisão histórica e um pipeline de tempo real (streaming) para fornecer dados com baixa latência.

   - **Pipeline de Análise Histórica (Batch)**

      Este pipeline é orquestrado pelo Airflow e roda de hora em hora para processar e consolidar os dados da hora anterior de forma definitiva.
   
      * **1) Ingestão (API SPTrans -> Bronze):** Um processo no NiFi consulta a API /Posicoes a cada 2 minutos. A resposta JSON completa é salva no bucket bronze do MinIO, particionada por ano/mes/dia/hora. Esta camada serve como a fonte de dados imutável para o pipeline de lote.
   
      * **2) Transformação (Bronze -> Silver):** A DAG bronze_to_silver aciona um job Spark (bronze_to_silver.py) que lê todos os JSONs da hora anterior na camada Bronze. O script "achata" a estrutura aninhada e salva os dados limpos como uma Tabela Delta Lake (posicoes_onibus) na camada Silver, particionada por ano/mes/dia
   
      * **3) Agregação (Silver -> Gold):** A DAG silver_to_gold aciona um segundo job Spark (silver_to_gold.py):

          - a. Ele lê os dados da hora correspondente da tabela Delta na camada Silver.
  
          - b. Calcula a contagem definitiva de ônibus únicos por linha.
  
          - c. Usa o comando MERGE para atualizar (ou inserir) a contagem definitiva na tabela fato_operacao_linhas_hora na Camada Gold do Lakehouse (MinIO).
  
          - d. Carga para a Camada de Servir: Como passo final, o job lê a tabela fato_operacao_linhas_hora completa do Lakehouse e a sobrescreve na tabela correspondente no PostgreSQL, garantindo que os dashboards de BI tenham os dados históricos mais precisos.
   
   - **Pipelines de Tempo Quase Real (Streaming)**

      * **1) Ingestão (API SPTrans -> Kafka):** O mesmo processo no NiFi que salva os dados no MinIO também envia, simultaneamente, cada resposta JSON para um tópico no Apache Kafka (sptrans_posicoes_raw), que atua como um buffer para o processamento em tempo real.
        
      * **2) Transformação (Kafka -> Silver Streaming):** Uma aplicação Spark Streaming (bronze_to_silver_streaming.py) consome as mensagens do Kafka. Ela "achata" a estrutura JSON e escreve os dados limpos em uma Tabela Delta Lake (posicoes_onibus_streaming) na camada Silver, otimizada para leituras incrementais.
   
      * **3) Agregação e Atualização (Silver Streaming -> Gold):** Uma segunda aplicação Spark Streaming (silver_to_gold_streaming.py) lê os novos dados da tabela Delta de streaming e executa várias tarefas em cada micro-lote:
    
          - a. KPIs Operacionais: Usa a tabela fato_posicao_onibus_atual como "memória" para calcular a velocidade e identificar ônibus parados. Em seguida, usa MERGE para atualizar as tabelas fato_velocidade_linha e fato_onibus_parados_linha na Camada Gold do Lakehouse (MinIO).
          
          - b. Posição Atual: Atualiza a "memória" de posições, usando MERGE para fazer o UPSERT da última posição conhecida de cada ônibus na tabela fato_posicao_onibus_atual no Lakehouse (MinIO).
          
          - c. Carga para a Camada de Servir: Ao final do lote, o job lê as tabelas de fatos recém-atualizadas do Lakehouse (fato_posicao_onibus_atual, fato_velocidade_linha, etc.) e as sobrescreve no PostgreSQL para consumo imediato pela API e pelos dashboards.


## API
  <img width="1435" height="752" alt="image" src="https://github.com/user-attachments/assets/3e602bf0-f1dd-4a75-a3bd-3abc22041501" />

## Dashboards
- Batch
  <img width="1844" height="377" alt="image" src="https://github.com/user-attachments/assets/0c13efad-0055-4867-9b5b-303be7f04ae6" />
  <img width="1551" height="606" alt="image" src="https://github.com/user-attachments/assets/516103ef-f088-4386-8642-88bd84aa51a8" />

- Near real time
  <img width="1848" height="837" alt="image" src="https://github.com/user-attachments/assets/53b40168-b92f-4a74-96c5-a5b87dd86787" />
  <img width="1845" height="599" alt="image" src="https://github.com/user-attachments/assets/d3c86efd-dc59-424d-ae11-975b392549b4" />
  <img width="1847" height="602" alt="image" src="https://github.com/user-attachments/assets/e17b8971-e17f-4f52-a791-401307b625f4" />
