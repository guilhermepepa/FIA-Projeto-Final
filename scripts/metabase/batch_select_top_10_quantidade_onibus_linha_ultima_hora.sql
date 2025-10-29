-- Usamos uma CTE para encontrar a data e hora mais recentes com dados na tabela de fatos
WITH latest_time AS (
  SELECT
    MAX(id_tempo) AS latest_id_tempo
  FROM
    fato_operacao_linhas_hora
)
-- Agora, para esse id_tempo, buscamos os dados, juntamos com a dimensão e ordenamos
SELECT
  -- Criamos o rótulo completo da linha para manter o padrão
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  dl.letreiro_linha,
  -- A quantidade de ônibus já está pré-calculada na tabela de fatos
  f.quantidade_onibus AS "Quantidade de Onibus"
FROM
  fato_operacao_linhas_hora AS f
  -- Junta com a dimensão de linha para obter as descrições
  JOIN dim_linha AS dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  f.id_tempo = (SELECT latest_id_tempo FROM latest_time)
ORDER BY
  -- Ordenamos pela quantidade de ônibus
  f.quantidade_onibus DESC
LIMIT 10;





--TRINO
-- Usamos uma CTE para encontrar a data e hora mais recentes com dados na tabela de fatos
WITH latest_time AS (
  SELECT
    MAX(id_tempo) AS latest_id_tempo
  FROM
    delta.gold.fato_operacao_linhas_hora
)
-- Agora, para esse id_tempo, buscamos os dados, juntamos com a dimensão e ordenamos
SELECT
  -- Criamos o rótulo completo da linha para manter o padrão
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  dl.letreiro_linha,
  -- A quantidade de ônibus já está pré-calculada na tabela de fatos
  f.quantidade_onibus AS "Quantidade de Onibus"
FROM
  delta.gold.fato_operacao_linhas_hora AS f
  -- Junta com a dimensão de linha para obter as descrições
  JOIN postgresql.public.dim_linha AS dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  f.id_tempo = (SELECT latest_id_tempo FROM latest_time)
ORDER BY
  -- Ordenamos pela quantidade de ônibus
  f.quantidade_onibus DESC
LIMIT 10