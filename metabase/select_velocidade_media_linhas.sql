-- 1. CTE para encontrar o id_tempo mais recente com dados em AMBAS as tabelas de fato
WITH latest_time AS (
  SELECT
    MAX(fvl.id_tempo) AS latest_id_tempo
  FROM
    fato_velocidade_linha AS fvl
    -- Garante que só consideramos os momentos em que temos dados de velocidade E de quantidade
    INNER JOIN fato_operacao_linhas_hora AS folh ON fvl.id_tempo = folh.id_tempo AND fvl.id_linha = folh.id_linha
)
-- 2. Agora, para esse id_tempo, calculamos a média ponderada
SELECT
  -- Garante que o resultado seja 0 se a soma de ônibus for nula ou zero
  COALESCE(
    -- SOMA(velocidade * quantidade)
    SUM(fvl.velocidade_media_kph * folh.quantidade_onibus)
    -- a dividir por SOMA(quantidade)
    / NULLIF(SUM(folh.quantidade_onibus), 0),
    0
  ) AS "Velocidade Média da Frota (km/h)"
FROM
  fato_velocidade_linha AS fvl
  -- Junta com a tabela de operação para obter a quantidade de ônibus (a nossa "ponderação")
  JOIN fato_operacao_linhas_hora AS folh ON fvl.id_tempo = folh.id_tempo AND fvl.id_linha = folh.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  fvl.id_tempo = (SELECT latest_id_tempo FROM latest_time)
  -- Ignora linhas onde a velocidade ou a quantidade não são positivas
  AND fvl.velocidade_media_kph > 0
  AND folh.quantidade_onibus > 0;

