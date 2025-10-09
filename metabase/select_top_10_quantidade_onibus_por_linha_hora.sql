-- Usamos uma CTE para encontrar a data e hora mais recentes com dados na tabela de fatos
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
-- Agora, para essa data e hora, agrupamos e somamos os dados para encontrar o Top 10
SELECT
  -- Criamos o rótulo completo da linha, buscando os nomes da tabela de dimensão 'dim_linha'
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  dl.letreiro_linha,
  
  -- Somamos a quantidade de ônibus da tabela de fatos
  SUM(f.quantidade_onibus) AS "Quantidade de Onibus"
FROM
  fato_operacao_linhas_hora f
  -- Junta com as dimensões para obter as descrições e filtrar pelo tempo
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
  JOIN dim_linha dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  dt.data_referencia = (SELECT data_referencia FROM latest_time)
  AND dt.hora_referencia = (SELECT hora_referencia FROM latest_time)
GROUP BY
  -- Agrupamos pelo rótulo completo e pelo letreiro para poder selecioná-lo
  "Linha",
  dl.letreiro_linha
ORDER BY
  -- Ordenamos pelo resultado da SOMA
  "Quantidade de Onibus" DESC
LIMIT
  10;