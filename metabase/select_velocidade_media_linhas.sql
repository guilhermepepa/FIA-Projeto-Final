-- 1. CTE para calcular a média "bruta" para cada intervalo de 10 minutos
WITH media_por_intervalo AS (
  SELECT
    date_trunc('hour', fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') +
    floor(extract(minute from fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') / 10) * interval '10 minutes'
    AS "intervalo_de_10_minutos",
    
    ROUND(AVG(fvl.velocidade_media_kph)::numeric, 2) AS "velocidade_media_bruta"
  FROM
    fato_velocidade_linha fvl
  WHERE
    -- Filtra um período recente para otimizar o cálculo inicial
    fvl.updated_at >= ((SELECT MAX(updated_at) FROM fato_velocidade_linha) - INTERVAL '1 hour')
    AND fvl.velocidade_media_kph > 0
  GROUP BY
    1 -- Agrupa pelo intervalo
)
-- 2. Seleção final para pegar APENAS o valor do último intervalo de tempo
SELECT
  velocidade_media_bruta AS "Velocidade Média da Frota (Agora)"
FROM
  media_por_intervalo
ORDER BY
  intervalo_de_10_minutos DESC -- Ordena do mais recente para o mais antigo
LIMIT 1; -- Pega apenas o primeiro resultado (o mais recente)
