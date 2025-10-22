-- 1. CTE para encontrar o id_tempo mais recente dos KPIs de velocidade
WITH latest_time AS (
    SELECT MAX(id_tempo) as latest_id_tempo
    FROM fato_velocidade_linha
),
-- 2. CTE para encontrar o timestamp mais recente correspondente a esse id_tempo
latest_kpi_timestamp AS (
    SELECT MAX(updated_at) as max_ts_utc
    FROM fato_velocidade_linha
    WHERE id_tempo = (SELECT latest_id_tempo FROM latest_time)
),
-- 3. CTE para calcular a contagem de ônibus ativos por linha em tempo real
onibus_ativos_por_linha AS (
    SELECT
        dl.id_linha,
        COUNT(fpoa.prefixo_onibus) AS quantidade_onibus
    FROM
        fato_posicao_onibus_atual fpoa
    JOIN dim_linha dl ON fpoa.letreiro_linha = dl.letreiro_linha
    WHERE
        -- Garante que estamos contando apenas ônibus que enviaram sinal recentemente
        fpoa.timestamp_captura >= ((SELECT max_ts_utc FROM latest_kpi_timestamp) - INTERVAL '60 minutes')
    GROUP BY
        dl.id_linha
)
-- 4. Agora, para o id_tempo mais recente, calculamos a média ponderada
SELECT
  -- Garante que o resultado seja 0.0 se a divisão for por zero ou nula
  COALESCE(
    ROUND(
        (SUM(fvl.velocidade_media_kph * oa.quantidade_onibus)
        / NULLIF(SUM(oa.quantidade_onibus), 0))::numeric
    , 2)
    , 0.0
  ) AS "Velocidade Média da Frota (km/h)"
FROM
  fato_velocidade_linha AS fvl
  -- Junta com a nossa contagem de ônibus ativos em tempo real para obter o "peso"
  JOIN onibus_ativos_por_linha AS oa ON fvl.id_linha = oa.id_linha
WHERE
  -- Filtra para buscar apenas os dados do período de tempo mais recente
  fvl.id_tempo = (SELECT latest_id_tempo FROM latest_time)
  -- Ignora linhas onde a velocidade ou a quantidade não são positivas
  AND fvl.velocidade_media_kph > 0
  AND oa.quantidade_onibus > 0;