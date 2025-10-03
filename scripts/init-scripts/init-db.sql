CREATE DATABASE airflow_meta;
CREATE DATABASE metabase_meta;

\c sptrans_dw;

CREATE TABLE fato_posicao_onibus_atual (
    prefixo_onibus BIGINT PRIMARY KEY,
    letreiro_linha VARCHAR,
    codigo_linha BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp_captura TIMESTAMP
);

-- Cria a nova tabela de velocidade com as chaves de dimensão
CREATE TABLE fato_velocidade_linha (
    id_tempo INTEGER,
    id_linha BIGINT,
    velocidade_media_kph DOUBLE PRECISION,
    updated_at TIMESTAMP,
    PRIMARY KEY (id_tempo, id_linha)
);

-- Cria a nova tabela de autocarros parados com as chaves de dimensão
CREATE TABLE fato_onibus_parados_linha (
    id_tempo INTEGER,
    id_linha BIGINT,
    quantidade_onibus_parados BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (id_tempo, id_linha)
);

CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    hora_referencia INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_da_semana VARCHAR(20) NOT NULL,
    fim_de_semana BOOLEAN NOT NULL,
    periodo_do_dia VARCHAR(20) NOT NULL,
    UNIQUE(data_referencia, hora_referencia)
);

INSERT INTO dim_tempo (data_referencia, hora_referencia, ano, mes, dia, dia_da_semana, fim_de_semana, periodo_do_dia)
SELECT
    datum AS data_referencia,
    EXTRACT(hour FROM datum) AS hora_referencia,
    EXTRACT(year FROM datum) AS ano,
    EXTRACT(month FROM datum) AS mes,
    EXTRACT(day FROM datum) AS dia,
    CASE EXTRACT(isodow FROM datum)
        WHEN 1 THEN 'Segunda-feira'
        WHEN 2 THEN 'Terça-feira'
        WHEN 3 THEN 'Quarta-feira'
        WHEN 4 THEN 'Quinta-feira'
        WHEN 5 THEN 'Sexta-feira'
        WHEN 6 THEN 'Sábado'
        WHEN 7 THEN 'Domingo'
    END AS dia_da_semana,
    EXTRACT(isodow FROM datum) IN (6, 7) AS fim_de_semana,
    CASE
        WHEN EXTRACT(hour FROM datum) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN EXTRACT(hour FROM datum) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN EXTRACT(hour FROM datum) BETWEEN 18 AND 23 THEN 'Noite'
        ELSE 'Madrugada'
    END AS periodo_do_dia
FROM generate_series(
    '2024-01-01'::timestamp,
    '2026-12-31 23:00:00'::timestamp,
    '1 hour'::interval
) datum;