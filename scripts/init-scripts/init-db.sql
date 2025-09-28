CREATE DATABASE airflow_meta;
CREATE DATABASE metabase_meta;

-- Conecta-se ao nosso Data Warehouse para criar as tabelas
\c sptrans_dw;

-- Cria a tabela para as posições em tempo real
CREATE TABLE fato_posicao_onibus_atual (
    prefixo_onibus BIGINT PRIMARY KEY,
    letreiro_linha VARCHAR,
    codigo_linha BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp_captura TIMESTAMP
);