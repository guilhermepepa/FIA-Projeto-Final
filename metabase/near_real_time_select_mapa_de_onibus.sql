-- 1. CTE para encontrar o timestamp UTC mais recente disponível na tabela
WITH latest_timestamp AS (
  SELECT MAX(timestamp_captura) as max_ts_utc
  FROM fato_posicao_onibus_atual
),
-- 2. CTE para identificar as 10 linhas com mais ônibus ativos na janela de 30 minutos que TERMINA no timestamp mais recente
top_10_linhas AS (
  SELECT
    letreiro_linha
  FROM
    fato_posicao_onibus_atual
  WHERE
    timestamp_captura >= ((SELECT max_ts_utc FROM latest_timestamp) - INTERVAL '29 minutes')
  GROUP BY
    letreiro_linha
  ORDER BY
    COUNT(prefixo_onibus) DESC
  LIMIT
    10
)
-- 3. Agora, selecionamos a posição de todos os ônibus que pertencem a essas 10 linhas, dentro da mesma janela de tempo
SELECT
  fpoa.prefixo_onibus,
  fpoa.letreiro_linha,
  ROUND(fpoa.latitude::numeric, 6) AS latitude,
  ROUND(fpoa.longitude::numeric, 6) AS longitude,
  (fpoa.timestamp_captura AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "horario_local_captura"
FROM
  fato_posicao_onibus_atual AS fpoa
WHERE
  -- Garantimos que estamos olhando apenas para os ônibus ativos na janela de tempo mais recente...
  fpoa.timestamp_captura >= ((SELECT max_ts_utc FROM latest_timestamp) - INTERVAL '29 minutes')
  -- ... E que a linha do ônibus está na nossa lista do Top 10
  AND fpoa.letreiro_linha IN (SELECT letreiro_linha FROM top_10_linhas);