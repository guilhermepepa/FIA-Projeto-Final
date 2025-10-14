-- 1. CTE para calcular a média "bruta" para cada intervalo de 10 minutos
WITH media_por_intervalo AS (
  SELECT
    date_trunc('hour', updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') +
    floor(extract(minute from updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') / 10) * interval '10 minutes'
    AS "intervalo_de_10_minutos",
    
    ROUND(AVG(velocidade_media_kph)::numeric, 2) AS "velocidade_media_bruta"
  FROM
    fato_velocidade_linha
  WHERE
    updated_at >= ((SELECT MAX(updated_at) FROM fato_velocidade_linha) - INTERVAL '3 hours')
    AND velocidade_media_kph > 0
  GROUP BY
    1
)
-- 2. Seleção final para calcular a Média Móvel sobre os resultados da CTE
SELECT
  -- Formata o rótulo do eixo X
  to_char("intervalo_de_10_minutos", 'DD/MM - HH24:MI') as "Intervalo",
  "velocidade_media_bruta" AS "Velocidade Média (10 min)",
  
  ROUND(
    AVG("velocidade_media_bruta") OVER (
      ORDER BY "intervalo_de_10_minutos"
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::numeric,
    2
  ) AS "Tendência (Média Móvel 30 min)"
FROM
  media_por_intervalo
ORDER BY
  "Intervalo" ASC;