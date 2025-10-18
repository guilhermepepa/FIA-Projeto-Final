-- 1. CTE para encontrar o timestamp mais recente dos KPIs de velocidade
WITH latest_kpi_timestamp AS (
    SELECT MAX(updated_at) as max_ts_utc
    FROM fato_velocidade_linha 
),
-- 2. CTE para calcular a contagem de ônibus ativos por linha em tempo real
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
),
-- 3. CTE para juntar os dados e criar os intervalos de tempo
dados_base_com_intervalo AS (
  SELECT
    -- Cria o intervalo de 10 minutos, arredondando para baixo
    date_trunc('hour', fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') + 
    floor(extract(minute from fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') / 10) * interval '10 minutes' AS "intervalo_de_10_minutos",
    
    -- Calcula o componente ponderado (velocidade * peso) para cada linha
    fvl.velocidade_media_kph * oa.quantidade_onibus AS "velocidade_ponderada_total",
    
    -- A quantidade de ônibus é o nosso "peso"
    oa.quantidade_onibus
  FROM
    fato_velocidade_linha AS fvl 
    -- Junta com a nossa contagem de ônibus ativos em tempo real
    JOIN onibus_ativos_por_linha AS oa ON fvl.id_linha = oa.id_linha
  WHERE
    -- Filtra para as últimas 3 horas para manter a performance
    fvl.updated_at >= ((SELECT max_ts_utc FROM latest_kpi_timestamp) - INTERVAL '3 hours')
    AND fvl.velocidade_media_kph > 0
    AND oa.quantidade_onibus > 0
),
-- 4. CTE para calcular a média ponderada correta para cada intervalo de 10 minutos
media_ponderada_por_intervalo AS (
  SELECT
    "intervalo_de_10_minutos",
    -- SOMA(velocidade * peso) / SOMA(peso)
    SUM("velocidade_ponderada_total") / SUM(quantidade_onibus) AS "velocidade_media_correta"
  FROM
    dados_base_com_intervalo
  GROUP BY
    "intervalo_de_10_minutos"
)
-- 5. Seleção final para calcular a Média Móvel
SELECT
  -- Formata o rótulo do eixo X para exibição
  to_char("intervalo_de_10_minutos", 'DD/MM - HH24:MI') as "Intervalo",
  
  -- Mostra a velocidade média do intervalo de 10 minutos
  ROUND("velocidade_media_correta"::numeric, 2) AS "Velocidade Média (10 min)",
  
  -- Calcula a média móvel sobre a velocidade média correta
  ROUND(
    AVG("velocidade_media_correta") OVER (
      ORDER BY "intervalo_de_10_minutos"
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric,
    2
  ) AS "Tendência (Média Móvel 30 min)"
FROM
  media_ponderada_por_intervalo
ORDER BY
  "Intervalo" ASC;

