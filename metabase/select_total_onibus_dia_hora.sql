SELECT 
  dt.data_referencia || ' ' || dt.hora_referencia AS data_referencia,
  COALESCE(SUM(folh.quantidade_onibus), 0) AS quantidade_onibus
FROM dim_tempo dt
LEFT JOIN fato_operacao_linhas_hora folh 
       ON folh.id_tempo = dt.id_tempo
LEFT JOIN dim_linha dl 
       ON folh.id_linha = dl.id_linha
WHERE dt.data_referencia BETWEEN '2025-10-01' AND '2025-10-08'
GROUP BY dt.data_referencia, dt.hora_referencia
ORDER BY dt.data_referencia, dt.hora_referencia;