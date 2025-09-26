WITH latest_hour AS (
  SELECT
    data_referencia,
    hora_referencia
  FROM
    dm_onibus_por_linha_hora
  GROUP BY
    data_referencia,
    hora_referencia
  ORDER BY
    data_referencia DESC,
    hora_referencia DESC
  LIMIT 1 
)
SELECT
  (t.letreiro_linha || ' - ' || t.nome_linha) AS "Linha",
    t.letreiro_linha,
  SUM(t.quantidade_onibus) AS "Quantidade de Onibus"
FROM
  dm_onibus_por_linha_hora AS t
  JOIN latest_hour AS l ON t.data_referencia = l.data_referencia AND t.hora_referencia = l.hora_referencia
GROUP BY
  t.letreiro_linha,
  t.nome_linha
ORDER BY
  "Quantidade de Onibus" DESC
LIMIT
  10;