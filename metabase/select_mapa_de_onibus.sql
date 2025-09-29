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