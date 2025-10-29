SELECT
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  fop.quantidade_onibus_parados AS "Total de Ônibus"
FROM
  -- Lê da tabela NRT, que não possui 'id_tempo'
  nrt_onibus_parados_linha AS fop
  JOIN dim_linha AS dl ON fop.id_linha = dl.id_linha
WHERE
  fop.quantidade_onibus_parados > 0
ORDER BY
  fop.quantidade_onibus_parados DESC -- Ordena do maior para o menor
LIMIT 10;