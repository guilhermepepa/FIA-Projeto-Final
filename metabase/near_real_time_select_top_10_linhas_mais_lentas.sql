-- Consulta para o Gráfico de Barras
WITH latest_time AS (
  SELECT MAX(id_tempo) AS latest_id_tempo
  FROM fato_velocidade_linha
)
SELECT
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  fvl.velocidade_media_kph AS "Velocidade Média (km/h)"
FROM
  fato_velocidade_linha AS fvl
  JOIN dim_linha AS dl ON fvl.id_linha = dl.id_linha
WHERE
  fvl.id_tempo = (SELECT latest_id_tempo FROM latest_time)
  AND fvl.velocidade_media_kph > 0
ORDER BY
  fvl.velocidade_media_kph ASC
LIMIT 10;