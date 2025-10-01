-- Usamos uma CTE para encontrar a data mais recente com dados na tabela de fatos
WITH latest_day AS (
  SELECT
    MAX(dt.data_referencia) as dia_recente
  FROM
    fato_operacao_linhas_hora f
    JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
)
-- Agora, para essa data, agrupa os dados por linha e pela HORA LOCAL
SELECT
  -- 1. Cria o rótulo completo da linha, buscando os nomes da tabela de dimensão 'dim_linha'
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  
  -- 2. Converte a hora UTC (que vem da 'dim_tempo') para o fuso de São Paulo
  EXTRACT(hour FROM (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "Hora Local",
  
  -- 3. Soma a quantidade de ônibus da tabela de fatos
  SUM(f.quantidade_onibus) AS "Total de Ônibus"
FROM
  fato_operacao_linhas_hora f
  -- Junta com as dimensões para obter as descrições
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
  JOIN dim_linha dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para buscar apenas os dados do dia mais recente
  dt.data_referencia = (SELECT dia_recente FROM latest_day)
GROUP BY
  -- Agrupamos pelas mesmas colunas que criamos
  "Linha",
  "Hora Local"
ORDER BY
  "Linha",
  "Hora Local" ASC;
