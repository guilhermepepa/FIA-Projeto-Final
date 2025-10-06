-- Usamos uma CTE para encontrar o id_tempo mais recente que possui dados
WITH latest_time AS (
  SELECT
    MAX(id_tempo) AS latest_id_tempo
  FROM
    fato_onibus_parados_linha
)
-- Agora, para esse id_tempo, somamos a quantidade de onibus parados de todas as linhas
SELECT
  SUM(fop.quantidade_onibus_parados) AS "Total de Ônibus Parados"
FROM
  fato_onibus_parados_linha AS fop
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  fop.id_tempo = (SELECT latest_id_tempo FROM latest_time);