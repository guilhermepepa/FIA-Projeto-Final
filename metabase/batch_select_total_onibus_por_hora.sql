-- 1. CTE para encontrar o ID da ÚLTIMA HORA COMPLETA (ignora a hora atual que está sendo processada pelo streaming)
WITH latest_complete_time AS (
  SELECT MAX(id_tempo) AS max_id_tempo
  FROM fato_operacao_linhas_hora
),
-- 2. CTE para encontrar os 24 últimos 'id_tempo' JÁ CONSOLIDADOS pelo pipeline de lote
latest_24_consolidated_times AS (
  SELECT DISTINCT
    id_tempo
  FROM
    fato_operacao_linhas_hora
  WHERE
    -- Despreza a hora mais recente para evitar dados parciais do streaming
    id_tempo <= (SELECT max_id_tempo FROM latest_complete_time)
  ORDER BY
    id_tempo DESC
  LIMIT 24
)
-- 3. Agora, selecionamos e formatamos os dados apenas para esses 24 momentos consolidados
SELECT
  -- Formata o timestamp UTC para uma string local legível no formato "DD/MM - HHh"
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora",
  
  -- Garante que o valor seja 0 se não houver ônibus (boa prática)
  COALESCE(SUM(f.quantidade_onibus), 0) AS "Total de Ônibus Ativos"
FROM
  fato_operacao_linhas_hora f
  -- Junta com a dimensão de tempo para obter a data e hora
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
WHERE
  -- Filtra para pegar apenas os registros cujo id_tempo está na nossa lista dos 24 mais recentes consolidados
  f.id_tempo IN (SELECT id_tempo FROM latest_24_consolidated_times)
GROUP BY
  -- Agrupamos pela data e hora originais para garantir a agregação correta
  dt.data_referencia,
  dt.hora_referencia
-- Filtra os resultados DEPOIS da agregação, removendo os grupos
-- cuja soma de ônibus é menor que 10.
HAVING
  SUM(f.quantidade_onibus) >= 10
ORDER BY
  -- Ordenamos pela data e hora originais para que a linha do gráfico seja desenhada na ordem cronológica correta
  dt.data_referencia ASC,
  dt.hora_referencia ASC;

