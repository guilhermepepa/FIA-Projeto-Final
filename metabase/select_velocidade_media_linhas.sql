-- Usamos uma CTE para encontrar o id_tempo mais recente com dados
WITH latest_time AS (
  SELECT
    MAX(id_tempo) AS latest_id_tempo
  FROM
    fato_velocidade_linha
)
-- Agora, para esse id_tempo, calculamos a média geral da velocidade
SELECT
  -- Arredondamos para 2 casas decimais para uma melhor visualização
  ROUND(AVG(fvl.velocidade_media_kph)::numeric, 2) AS "velocidade_media_geral"
FROM
  fato_velocidade_linha AS fvl
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  fvl.id_tempo = (SELECT latest_id_tempo FROM latest_time)
  -- Garante que estamos calculando a média apenas para linhas que tiveram movimento
  AND fvl.velocidade_media_kph > 0;