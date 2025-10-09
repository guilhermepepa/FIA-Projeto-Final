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
-- Agora, para esses 12 momentos, agrupa os dados por linha e pela HORA LOCAL formatada
SELECT
  -- 1. Cria o rótulo completo da linha
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  
  -- 2. Formata o timestamp UTC para uma string local legível no formato "DD/MM - HHh"
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora Local",
  
  -- 3. Soma a quantidade de ônibus da tabela de fatos
  SUM(f.quantidade_onibus) AS "Total de Ônibus"
FROM
  fato_operacao_linhas_hora f
  -- Junta com as dimensões para obter as descrições
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
  JOIN dim_linha dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para pegar apenas os registros cujo id_tempo está na nossa lista dos 12 mais recentes
  f.id_tempo IN (SELECT id_tempo FROM latest_12_times)
GROUP BY
  -- Agrupamos pelas mesmas colunas que criamos
  "Linha",
  "Dia e Hora Local",
  -- Incluímos as colunas de ordenação no GROUP BY
  dt.data_referencia,
  dt.hora_referencia
ORDER BY
  -- Ordenamos pela data e hora originais para garantir que a Tabela Dinâmica tenha as colunas na ordem correta
  dt.data_referencia ASC,
  dt.hora_referencia ASC;