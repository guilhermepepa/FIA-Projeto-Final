# Projeto Final - Pipeline de Dados SPTrans

Este projeto implementa um pipeline de dados para coletar e analisar dados da API Olho Vivo da SPTrans.

## Arquitetura

- **Ingestão:** NiFi
- **Data Lake:** MinIO
- **Transformação:** PySpark 
- **Data Warehouse:** PostgreSQL 
- **Visualização:** Metabase



<img width="1165" height="632" alt="image" src="https://github.com/user-attachments/assets/f233835a-2f21-402a-916d-9be741f74fe0" />


- camada bronze -> arquivos JSON contendo o retorno da API /Posicoes, capturados a cada 2 minutos via nifi, particionado por ano / mes / dia / hora;

- camada silver -> arquivos Parquet contendo o histórico de posições de todos os onibus de todas as linhas, particionado por ano / mes / dia.




- A api /Posicoes da sptrans devolve uma lista de linhas, sendo que cada lista de linhas possui uma lista de veículos da linha contendo suas posições. Um processor do nifi grava a resposta desta api na camada bronze a cada 2 minutos. A camada bronze está particionada por ano / mes / dia / hora;

- O script bronze_to_silver.py é executado de hora em hora e recebe como entrada os jsons pegando posicões da hora anterior da camada bronze. Ele explode as linhas e os veículos por linha. As colunas letreiro_linha, codigo_linha, prefixo_onibus, acessivel, timestamp_captura_str, latitude e longitude são gravados em um arquivo parquet na camada silver, gerando uma tabela "flat", onde cada linha da tabela corresponde à uma determinada posição a um único ônibus. A camada silver é particionada por ano / mes / dia;

- O script silver_to_gold.py é executado de hora em hora após o processamento do script bronze_to_silver.py, recebendo como entrada os arquivos parquet e também o arquivo routes.txt da camada bronze, para enriquecer os dados com o nome da linha. Ele filtra os registros da camada silver por hora, selecionando apenas os registros da hora que o Airflow mandou processar. Esta transformação tem como objetivo agrupar todas as linhas por letreiro_linha e codigo_linha, sendo que para cada um destes crupos é executado o countDistinct pelo "prefixo_onibus". O resultado é uma tabela resumida com codigo_linha, letreiro_linha, nome_linha e quantidade_onibus. O DataFrame final é salvo no PostgreSQL através de um processo seguro de duas etapas, orquestrado pelo Airflow: primeiro, o Spark sobrescreve uma tabela temporária chamada staging_dm_onibus_por_linha_hora. Em seguida, uma segunda tarefa no Airflow executa um comando SQL que apaga os dados da hora correspondente na tabela final dm_onibus_por_linha_hora e, então, insere os novos dados da tabela de staging. Este padrão de "delete-and-insert" garante que o pipeline seja idempotente, ou seja, que possa ser re-executado para a mesma hora sem gerar dados duplicados.


nifi: https://127.0.0.1:9443/nifi

minio: http://localhost:9001/

airflow: http://localhost:8081/

metabase: http://localhost:3000


docker-compose exec spark-master spark-submit --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/bronze_to_silver.py 2025 09 22 18

docker-compose exec spark-master spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 --conf "spark.driver.extraJavaOptions=-Divy.home=/tmp" --conf "spark.executor.extraJavaOptions=-Divy.home=/tmp" /opt/bitnami/spark/apps/silver_to_gold.py 2025 09 22 18


docker-compose exec postgres psql -U admin sptrans_dw

select * from dm_onibus_por_linha_hora order by quantidade_onibus de
sc limit 100;


select count(1) from dm_onibus_por_linha_hora order by quantidade_onibus de
sc limit 100;



select * from dm_onibus_por_linha_hora
            WHERE timestamp_hora = '2025-09-25 15:00:00';

select count(1) from staging_dm_onibus_por_linha_hora
            WHERE timestamp_hora = '2025-09-25 14:00:00';            





SELECT  data_referencia,  hora_referencia,  COUNT(1) AS total_registros FROM  dm_onibus_por_linha_hora GROUP BY  data_referencia,  hora_referencia ORDER BY  data_referencia DESC, hora_referencia DESC;

SELECT  data_referencia,  hora_referencia,  COUNT(1) AS total_registros FROM  staging_dm_onibus_por_linha_hora GROUP BY  data_referencia,  hora_referencia ORDER BY  data_referencia DESC, hora_referencia DESC;