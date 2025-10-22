-- Usamos uma CTE para encontrar o id_tempo mais recente com dados na tabela de fatos
WITH latest_time AS (
  SELECT
    MAX(id_tempo) AS latest_id_tempo
  FROM
    fato_onibus_parados_linha
)
-- Agora, para esse id_tempo, buscamos os nomes, ordenamos e limitamos o resultado
SELECT
  -- Criamos o rótulo completo da linha, como nos outros gráficos
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  fop.quantidade_onibus_parados AS "Ônibus Parados"
FROM
  fato_onibus_parados_linha AS fop
  -- Junta com a dimensão de linha para obter as descrições
  JOIN dim_linha AS dl ON fop.id_linha = dl.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  fop.id_tempo = (SELECT latest_id_tempo FROM latest_time)
  AND fop.quantidade_onibus_parados > 0
ORDER BY
  fop.quantidade_onibus_parados DESC -- Ordena do maior para o menor
LIMIT
  10;