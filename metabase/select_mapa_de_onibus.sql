SELECT
  prefixo_onibus,
  letreiro_linha,
  ROUND(latitude::numeric, 6) AS latitude,
  ROUND(longitude::numeric, 6) AS longitude,
  (timestamp_captura AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "horario_local_captura"
FROM
  fato_posicao_onibus_atual
WHERE
  timestamp_captura >= NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes';



WITH top_10_linhas AS (
  SELECT
    letreiro_linha
  FROM
    fato_posicao_onibus_atual
  WHERE
    timestamp_captura >= NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes'
  GROUP BY
    letreiro_linha
  ORDER BY
    COUNT(prefixo_onibus) DESC
  LIMIT
    10
)
SELECT
  fpoa.prefixo_onibus,
  fpoa.letreiro_linha,
  ROUND(fpoa.latitude::numeric, 6) AS latitude,
  ROUND(fpoa.longitude::numeric, 6) AS longitude,
  (fpoa.timestamp_captura AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "horario_local_captura"
FROM
  fato_posicao_onibus_atual AS fpoa
WHERE
  fpoa.timestamp_captura >= NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes'
  AND fpoa.letreiro_linha IN (SELECT letreiro_linha FROM top_10_linhas);  