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





-- Usamos uma CTE para encontrar os 12 últimos 'id_tempo' que possuem dados na tabela de fatos
WITH latest_12_times AS (
  SELECT DISTINCT
    id_tempo
  FROM
    fato_operacao_linhas_hora
  ORDER BY
    id_tempo DESC
  LIMIT 12
)
-- Agora, selecionamos e formatamos os dados apenas para esses 12 momentos
SELECT
  -- Formata o timestamp UTC para uma string local legível no formato "DD/MM - HHh"
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora",
  
  SUM(f.quantidade_onibus) AS "Total de Ônibus Ativos"
FROM
  fato_operacao_linhas_hora f
  -- Junta com a dimensão de tempo para obter a data e hora
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
WHERE
  -- A MUDANÇA PRINCIPAL ESTÁ AQUI:
  -- Filtra para pegar apenas os registros cujo id_tempo está na nossa lista dos 12 mais recentes
  f.id_tempo IN (SELECT id_tempo FROM latest_12_times)
GROUP BY
  -- Agrupamos pela data e hora originais para garantir a agregação correta
  dt.data_referencia,
  dt.hora_referencia
ORDER BY
  -- Ordenamos pela data e hora originais para garantir que a linha do gráfico seja desenhada na ordem cronológica correta
  dt.data_referencia ASC,
  dt.hora_referencia ASC;