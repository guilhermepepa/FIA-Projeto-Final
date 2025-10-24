## Projeto Final - Pipeline de Dados SPTrans

Este projeto implementa um pipeline de dados para coletar e analisar dados da API Olho Vivo da SPTrans.




## Arquitetura

- **Ingestão:** NiFi (https://127.0.0.1:9443/nifi)
- **Data Lakehouse:** MinIO e Delta Lake (http://localhost:9001/)
- **Streaming:** Kafka
- **Orquestração:** Airflow (http://localhost:8081/)
- **Transformação:** PySpark (Batch e Streaming)
- **Camada de Entrega de Dados:** PostgreSQL, Metabase (http://localhost:3000) e FastAPI (http://localhost:8002)
  

A solução implementa uma Arquitetura Lakehouse Medalhão que segue o padrão ELT (Extract, Load, Transform), projetada para fornecer tanto insights operacionais com baixa latência (streaming), quanto análises históricas (batch). 

<img width="2147" height="818" alt="image" src="https://github.com/user-attachments/assets/9c21a692-b4f5-4400-ae1d-eef7851555b5" />



## Detalhamento das Camadas de Dados
A estrutura é dividida em três camadas principais:
- **Camada Bronze (Dados Brutos)**
  
    Esta camada funciona como o repositório de entrada para todos os dados em seu formato original, sem nenhuma limpeza ou transformação.
    * MinIO (Data Lake)
      * Posições da Frota: Arquivos JSON contendo o retorno completo da API /Posicoes, capturados a cada 2 minutos e particionados por ano/mes/dia/hora. Serve como a fonte para o pipeline histórico (batch).
      * Dados Cadastrais: Arquivos estáticos do padrão GTFS (como o routes.txt), que fornecem os nomes e detalhes das linhas de ônibus.
    * Apache Kafka
      * Tópico sptrans_posicoes_raw: Recebe as mesmas mensagens JSON da API em tempo real. Serve como um buffer de alta performance para alimentar os pipelines de streaming.

- **Camada Silver (Dados Limpos)**
  
    A Camada Silver transforma os dados brutos em um formato otimizado e confiável. Todos os dados aqui são armazenados como Tabelas Delta Lake no Minio, garantindo transações ACID e qualidade.
  
    * Tabela posicoes_onibus (Batch)
      
      Localização: s3a://silver/posicoes_onibus

      Conteúdo: Contém o histórico detalhado e "achatado" (flat) de todas as posições de ônibus. Os dados são limpos, com tipos corrigidos, e particionados por ano/mes/dia. É a fonte para o pipeline de lote (batch) da Camada Gold.


    * Tabela posicoes_onibus_streaming (Streaming)
    
      Localização: s3a://silver/posicoes_onibus_streaming

      Conteúdo: Versão da tabela de posições otimizada para o fluxo de tempo real. Contém todos os campos necessários, incluindo dados de geolocalização (latitude/longitude), e é particionada por ano/mes/dia/hora para leituras incrementais.


    * Tabela posicoes_onibus_streaming (Streaming)
    
      Localização: s3a://silver/kpis_historicos_para_processar

      Conteúdo: Tabela intermediária que armazena os KPIs (velocidade, ônibus parados) calculados pelo pipeline de streaming. Ela funciona como uma "fila" para ser consumida de forma assíncrona pelo pipeline de lote (batch) da Camada Gold.


- **Camada Gold (Dados Agregados e de Negócio)**

  A Camada Gold é dividida em duas partes:
  
    * Lakehouse (MinIO) - Dados agregados
      
      É aqui que os dados de negócio são consolidados e armazenados como Tabelas Delta Lake. Os pipelines Spark executam as operações de agregação e MERGE diretamente nestas tabelas.
      
      - Tabelas Fato: fato_operacao_linhas_hora, fato_velocidade_linha e fato_onibus_parados_linha. Elas contêm os KPIs e métricas consolidadas, servindo como a fonte única da verdade para camadas de baixa latência (atualmente somente o PostgreSQL).


    * Camada de Entrega de Dados (PostgreSQL) - Otimizada para Consumo
      
      Este é o Data Warehouse, otimizado para consultas rápidas. As tabelas aqui são cópias dos dados da camada Gold do Lakehouse, carregadas ao final de cada pipeline para alimentar a API e os dashboards no Metabase com baixa latência.
      - Tabelas de Dimensão: dim_linha (descreve as linhas de ônibus) e dim_tempo (descreve cada hora de cada dia).
      - Tabelas de Estado e NRT (Near Real-Time): Alimentadas diretamente pelo pipeline de streaming para dashboards em tempo real.
        
          * nrt_posicao_onibus_atual: Tabela de estado com a última posição de cada ônibus (PK: prefixo_onibus).
            
          * nrt_velocidade_linha / nrt_onibus_parados_linha: Snapshots dos KPIs mais recentes, sobrescritos a cada 2 minutos.
            
      - Tabelas de Fato Históricas (Cópia): Cópias das tabelas do Lakehouse (fato_operacao_linhas_hora, etc.), carregadas ao final de cada pipeline de lote para consultas analíticas rápidas.

       <img width="816" height="880" alt="image" src="https://github.com/user-attachments/assets/b809eab9-d06e-4344-bc87-f7501cd3b8b7" />



## Detalhamento dos Pipelines
A arquitetura é composta por dois pipelines principais que operam em conjunto: um pipeline de lote (batch) para garantir a precisão histórica e um pipeline de tempo real (streaming) para fornecer dados com baixa latência.

   - **Pipeline de Análise Histórica (Batch)**

      Este pipeline é orquestrado pelo Airflow e roda de hora em hora para processar e consolidar os dados da hora anterior.
   
      * **1) Ingestão (SPTrans -> Bronze):** Esta etapa é responsável por capturar os dados brutos e armazená-los em nossa camada inicial. O processo é orquestrado pelo Apache NiFi e lida com dois fluxos de dados distintos:
        
          - Dados de Posição (API): A cada 2 minutos, um process group do NiFi consulta a API /Posicoes da SPTrans. A resposta JSON completa é enviada simultaneamente para dois destinos: o bucket bronze do MinIO - particionado por ano/mes/dia/hora, e um tópico do Kafka - que será mencionando abaixo no fluxo do Pipeline de Streaming;
            
          - Dados Cadastrais (Arquivos GTFS): Diariamente, um segundo process group do NiFi copia os arquivos estáticos para uma pasta específica do bucke bronze (bronze/gtfs). Estes arquivos fornecem dados para o enriquecimento, como os novmes das linhas de ônibus, que serão usados nas camadas Silver e Gold.

   
      * **2) Transformação (Bronze -> Silver):** A DAG bronze_to_silver aciona um job Spark (bronze_to_silver_batch.py) que lê todos os JSONs da hora anterior na camada Bronze. O script "achata" a estrutura aninhada e salva os dados limpos como uma Tabela Delta Lake (posicoes_onibus) na camada Silver, particionada por ano/mes/dia
   
      * **3) Agregação (Silver -> Gold):** A DAG silver_to_gold aciona um segundo job Spark (silver_to_gold_batch.py):

          - a. (Tarefa 1 - Operação): Lê os dados da hora da tabela posicoes_onibus (Silver), calcula a contagem de ônibus únicos e usa MERGE para atualizar a tabela fato_operacao_linhas_hora na Camada Gold do Lakehouse (MinIO).

          - b. (Tarefa 2 - KPIs): Lê e processa os dados da tabela de buffer kpis_historicos_para_processar (Silver), agregando e usando MERGE para atualizar as tabelas fato_velocidade_linha e fato_onibus_parados_linha na Camada Gold do     Lakehouse (MinIO).

          - c. (Passo Final): Como passo final, o job lê as tabelas de fatos atualizadas do Lakehouse (todas as três) e as sobrescreve no PostgreSQL, garantindo que os dashboards de BI tenham os dados históricos precisos.
   
   - **Pipelines de Tempo Quase Real (Streaming)**

      * **1) Ingestão (API SPTrans -> Kafka):** O mesmo processo no NiFi que salva os dados no bucket bronze do MinIO também envia, simultaneamente, cada resposta JSON para um tópico no Apache Kafka (sptrans_posicoes_raw), que atua como um buffer para o processamento em tempo real.
        
      * **2) Transformação (Kafka -> Silver Streaming):** Uma aplicação Spark Streaming (bronze_to_silver_streaming.py) consome as mensagens do Kafka. Ela "achata" a estrutura JSON e escreve os dados limpos em uma Tabela Delta Lake (posicoes_onibus_streaming) na camada Silver, otimizada para leituras incrementais.
   
      * **3) Agregação e Atualização (Silver Streaming -> Gold):** Uma segunda aplicação Spark Streaming (silver_to_gold_nrt_streaming.py) lê os novos dados da tabela Delta de streaming e executa várias tarefas em cada micro-lote:

          - a. Atualização de Estado (PostgreSQL): Usa UPSERT (INSERT ... ON CONFLICT) para atualizar a tabela nrt_posicao_onibus_atual no PostgreSQL. Esta tabela serve como a "memória" de estado para os cálculos.
          
          - b. Cálculo e Entrega de KPIs NRT (PostgreSQL): Usa a "memória" para calcular a velocidade e os ônibus parados. Em seguida, escreve (TRUNCATE + INSERT) esses KPIs diretamente nas tabelas nrt_velocidade_linha e nrt_onibus_parados_linha no PostgreSQL para alimentar os dashboards.
          
          - c. Fila para o Histórico (Lakehouse Silver): Os mesmos KPIs calculados são também anexados (appended) à tabela kpis_historicos_para_processar (Delta Lake) na Camada Silver, servindo como uma fila para o pipeline de lote (batch).


## API
  <img width="1435" height="752" alt="image" src="https://github.com/user-attachments/assets/3e602bf0-f1dd-4a75-a3bd-3abc22041501" />

## Dashboards
- Batch
  <img width="1848" height="378" alt="image" src="https://github.com/user-attachments/assets/b705c401-524e-4af0-ad86-353d3af6d9bd" />
  <img width="1551" height="606" alt="image" src="https://github.com/user-attachments/assets/516103ef-f088-4386-8642-88bd84aa51a8" />

- Near real time
  <img width="1852" height="747" alt="image" src="https://github.com/user-attachments/assets/25c3e2fb-a0c2-4bb5-86af-bc4870ed3237" />
  <img width="1845" height="599" alt="image" src="https://github.com/user-attachments/assets/d3c86efd-dc59-424d-ae11-975b392549b4" />
  <img width="1847" height="602" alt="image" src="https://github.com/user-attachments/assets/e17b8971-e17f-4f52-a791-401307b625f4" />
