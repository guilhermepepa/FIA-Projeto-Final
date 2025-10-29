-- 1. CTE para calcular a contagem de ônibus ativos por linha (nosso "peso")
-- Lemos da tabela de posições, que é o nosso "estado" mais atual.
WITH onibus_ativos_por_linha AS (
  SELECT
    dl.id_linha,
    COUNT(fpoa.prefixo_onibus) AS quantidade_onibus
  FROM
    nrt_posicao_onibus_atual fpoa
    JOIN dim_linha dl ON fpoa.letreiro_linha = dl.letreiro_linha
  WHERE
    -- Garante que estamos contando apenas ônibus que enviaram sinal recentemente
    fpoa.timestamp_captura >= ((SELECT MAX(timestamp_captura) FROM nrt_posicao_onibus_atual) - INTERVAL '4 minutes')
  GROUP BY
    dl.id_linha
)
-- 2. Agora, calculamos a média ponderada
SELECT
  -- Garante que o resultado seja 0.0 se a divisão for por zero ou nula
  COALESCE(
    ROUND(
      -- SOMA(velocidade * peso)
      (SUM(fvl.velocidade_media_kph * oa.quantidade_onibus)
      -- / SOMA(peso)
      / NULLIF(SUM(oa.quantidade_onibus), 0))::numeric
    , 2)
    , 0.0
  ) AS "Velocidade Média da Frota (km/h)"
FROM
  -- Lemos as velocidades da nossa nova tabela NRT
  nrt_velocidade_linha AS fvl
  -- Junta com a nossa contagem de ônibus ativos em tempo real para obter o "peso"
  JOIN onibus_ativos_por_linha AS oa ON fvl.id_linha = oa.id_linha
WHERE
  -- Ignora linhas onde a velocidade ou a quantidade não são positivas
  fvl.velocidade_media_kph > 0
  AND oa.quantidade_onibus > 0;

