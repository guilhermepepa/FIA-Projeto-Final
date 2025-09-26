WITH latest_day AS (
  -- Primeiro, descobre qual é a data mais recente com dados na tabela
  SELECT MAX(data_referencia) as dia_recente
  FROM dm_onibus_por_linha_hora
)
-- Agora, para essa data, agrupa os dados por linha e pela HORA LOCAL
SELECT
  -- 1. Cria o rótulo completo da linha
  (letreiro_linha || ' - ' || nome_linha) AS "Linha",
  
  -- 2. Converte a hora de UTC para o fuso de São Paulo
  EXTRACT(hour FROM (data_referencia + hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "Hora Local",
  
  -- 3. Soma a quantidade de ônibus
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