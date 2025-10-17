-- 1. CTE para juntar as tabelas de fato e criar os intervalos de tempo
WITH dados_base_com_intervalo AS (
  SELECT
    -- Cria o intervalo de 10 minutos, arredondando para baixo
    date_trunc('hour', fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') + 
    floor(extract(minute from fvl.updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') / 10) * interval '10 minutes' AS "intervalo_de_10_minutos",
    
    -- Calcula o componente ponderado (velocidade * peso) para cada linha
    fvl.velocidade_media_kph * folh.quantidade_onibus AS "velocidade_ponderada_total",
    
    -- A quantidade de ônibus é o nosso "peso"
    folh.quantidade_onibus
  FROM
    fato_velocidade_linha AS fvl
    -- Junta com a tabela de operação para obter a quantidade de ônibus
    JOIN fato_operacao_linhas_hora AS folh ON fvl.id_tempo = folh.id_tempo AND fvl.id_linha = folh.id_linha
  WHERE
    -- Filtra para as últimas 3 horas para manter a performance
    fvl.updated_at >= ((SELECT MAX(updated_at) FROM fato_velocidade_linha) - INTERVAL '3 hours')
    AND fvl.velocidade_media_kph > 0
    AND folh.quantidade_onibus > 0
),
-- 2. CTE para calcular a média ponderada correta para cada intervalo de 10 minutos
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
-- 3. Seleção final para calcular a Média Móvel sobre os resultados corretos
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
