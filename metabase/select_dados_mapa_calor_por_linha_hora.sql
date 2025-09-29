WITH latest_day AS (
  SELECT MAX(data_referencia) as dia_recente
  FROM dm_onibus_por_linha_hora
)
SELECT
  (letreiro_linha || ' - ' || nome_linha) AS "Linha",
    EXTRACT(hour FROM (data_referencia + hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "Hora Local",
    SUM(quantidade_onibus) AS "Total de Ônibus"
FROM
  dm_onibus_por_linha_hora
WHERE
  data_referencia = (SELECT dia_recente FROM latest_day)
GROUP BY
  "Linha",
  "Hora Local"
ORDER BY
  "Linha",
  "Hora Local" ASC;