SELECT
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  ROUND(fvl.velocidade_media_kph::numeric, 2) AS "Velocidade Média (km/h)"
FROM
  -- Lê da tabela NRT, que não possui 'id_tempo'
  nrt_velocidade_linha AS fvl
  JOIN dim_linha AS dl ON fvl.id_linha = dl.id_linha
WHERE
  fvl.velocidade_media_kph > 0
ORDER BY
  fvl.velocidade_media_kph ASC
LIMIT 10;