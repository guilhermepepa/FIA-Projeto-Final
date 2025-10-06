WITH latest_day AS (
  -- Primeiro, descobre qual é a data mais recente que possui dados na tabela de fatos
  SELECT
    MAX(dt.data_referencia) as dia_recente
  FROM
    fato_operacao_linhas_hora f
    JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
)
-- Agora, para essa data, soma a quantidade de ônibus de todas as linhas para cada hora
SELECT
  -- A conversão de fuso horário continua a mesma para exibir a hora local
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora",
  SUM(f.quantidade_onibus) AS "Total de Ônibus Ativos"
FROM
  fato_operacao_linhas_hora f
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
WHERE
  dt.data_referencia = (
    SELECT
      dia_recente
    FROM
      latest_day
  )
GROUP BY
  "Dia e Hora"
ORDER BY
  "Dia e Hora" ASC
LIMIT 12;