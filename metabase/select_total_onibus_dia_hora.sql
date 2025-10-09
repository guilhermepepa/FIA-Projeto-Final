-- Usamos uma CTE para encontrar o primeiro e o último TIMESTAMP com dados na tabela de fatos
WITH time_range AS (
  SELECT
    MIN(dt.data_referencia + dt.hora_referencia * interval '1 hour') as min_ts_utc,
    MAX(dt.data_referencia + dt.hora_referencia * interval '1 hour') as max_ts_utc
  FROM
    fato_operacao_linhas_hora f
    JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
)
-- Agora, construímos a série temporal apenas para o intervalo de tempo exato em que existem dados
SELECT
  -- Formata a data e hora UTC para uma string local legível no formato "DD/MM - HHh"
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora",
  
  -- Garante que, se não houver ônibus em uma determinada hora, o valor seja 0 em vez de nulo
  COALESCE(SUM(f.quantidade_onibus), 0) AS "Total de Ônibus Ativos"
FROM
  dim_tempo dt
  -- Usamos LEFT JOIN para garantir que todas as horas dentro do nosso intervalo de tempo apareçam no resultado
  LEFT JOIN fato_operacao_linhas_hora f ON f.id_tempo = dt.id_tempo
WHERE
  -- Filtra a dim_tempo para incluir apenas as horas entre o primeiro e o último timestamp com dados
  (dt.data_referencia + dt.hora_referencia * interval '1 hour') 
  BETWEEN (SELECT min_ts_utc FROM time_range) AND (SELECT max_ts_utc FROM time_range)
GROUP BY
  -- Agrupamos pela data e hora originais para garantir a agregação correta
  dt.data_referencia,
  dt.hora_referencia
ORDER BY
  -- Ordenamos pela data e hora originais para que a linha do gráfico seja desenhada na ordem cronológica correta
  dt.data_referencia ASC,
  dt.hora_referencia ASC;
