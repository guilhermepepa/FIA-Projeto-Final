-- Usamos uma CTE para encontrar a data e hora mais recentes na nossa tabela de factos
WITH latest_time AS (
  SELECT
    dt.data_referencia,
    dt.hora_referencia
  FROM
    fato_operacao_linhas_hora f
    JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
  ORDER BY
    dt.data_referencia DESC,
    dt.hora_referencia DESC
  LIMIT 1
)
-- Agora, somamos a quantidade de autocarros para esse período de tempo
SELECT
  SUM(f.quantidade_onibus) AS "Total de Onibus Alocados (Última Hora)"
FROM
  fato_operacao_linhas_hora f
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
WHERE
  dt.data_referencia = (SELECT data_referencia FROM latest_time)
  AND dt.hora_referencia = (SELECT hora_referencia FROM latest_time);