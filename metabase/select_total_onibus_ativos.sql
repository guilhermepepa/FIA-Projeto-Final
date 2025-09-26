WITH latest_hour AS (
  -- Encontra a data e hora mais recentes
  SELECT MAX(data_referencia) AS max_data, MAX(hora_referencia) AS max_hora
  FROM dm_onibus_por_linha_hora
  WHERE data_referencia = (SELECT MAX(data_referencia) FROM dm_onibus_por_linha_hora)
)
-- Soma a quantidade de ônibus apenas para essa hora
SELECT SUM(t.quantidade_onibus) AS "Total de Ônibus em Operação"
FROM dm_onibus_por_linha_hora AS t, latest_hour AS l
WHERE t.data_referencia = l.max_data AND t.hora_referencia = l.max_hora;