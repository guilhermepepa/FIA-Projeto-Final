-- 1. CTE para encontrar os 'id_tempo' mais recentes (últimas 3 horas de dados consolidados)
WITH latest_kpi_times AS (
    SELECT DISTINCT id_tempo
    FROM fato_velocidade_linha
    ORDER BY id_tempo DESC
    LIMIT 3 -- Pega as últimas 3 horas de dados que o batch processou
),
-- 2. CTE para juntar as tabelas de fatos HISTÓRICAS
dados_base_historicos AS (
  SELECT
    -- Pega a data e hora da dimensão de tempo
    dt.data_referencia,
    dt.hora_referencia,
    
    -- Calcula o componente ponderado (velocidade * peso) para cada linha
    fvl.velocidade_media_kph * folh.quantidade_onibus AS "velocidade_ponderada_total",
    
    -- A quantidade de ônibus é o nosso "peso"
    folh.quantidade_onibus
  FROM
    -- Fonte 1: Fato histórico de velocidade
    fato_velocidade_linha AS fvl
    -- Fonte 2: Fato histórico de contagem (o "peso")
    JOIN fato_operacao_linhas_hora AS folh 
      ON fvl.id_tempo = folh.id_tempo AND fvl.id_linha = folh.id_linha
    -- Junta com o tempo para obter os carimbos
    JOIN dim_tempo dt ON fvl.id_tempo = dt.id_tempo
  WHERE
    -- Filtra para as últimas 3 horas de dados de KPI HISTÓRICOS
    fvl.id_tempo IN (SELECT id_tempo FROM latest_kpi_times)
    AND fvl.velocidade_media_kph > 0
    AND folh.quantidade_onibus > 0
),
-- 3. CTE para calcular a média ponderada correta para cada HORA
media_ponderada_por_hora AS (
  SELECT
    "data_referencia",
    "hora_referencia",
    -- SOMA(velocidade * peso) / SOMA(peso)
    SUM("velocidade_ponderada_total") / SUM(quantidade_onibus) AS "velocidade_media_correta"
  FROM
    dados_base_historicos
  GROUP BY
    "data_referencia",
    "hora_referencia"
)
-- 4. Seleção final (sem média móvel, pois os dados já são horários)
SELECT
  -- Formata o rótulo do eixo X para exibição
  to_char(
    (data_referencia + hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) as "Hora",
  
  -- Mostra a velocidade média da hora
  ROUND("velocidade_media_correta"::numeric, 2) AS "Velocidade Média (km/h)"
  
FROM
  media_ponderada_por_hora
ORDER BY
  "data_referencia" ASC,
  "hora_referencia" ASC;

