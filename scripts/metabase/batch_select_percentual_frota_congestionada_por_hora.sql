-- 1. CTE para buscar os últimos 48 'id_tempo' consolidados (2 dias de dados)
WITH latest_24_times AS (
    SELECT DISTINCT id_tempo
    FROM fato_operacao_linhas_hora
    ORDER BY id_tempo DESC
    LIMIT 24
),
-- 2. CTE para agregar o total de ônibus ativos por hora
total_por_hora AS (
    SELECT
        id_tempo,
        SUM(quantidade_onibus) AS total_ativos
    FROM
        fato_operacao_linhas_hora
    WHERE
        id_tempo IN (SELECT id_tempo FROM latest_24_times)
    GROUP BY
        id_tempo
	HAVING
        SUM(quantidade_onibus) >= 10
),
-- 3. CTE para agregar o total de ônibus parados por hora
parados_por_hora AS (
    SELECT
        id_tempo,
        SUM(quantidade_onibus_parados) AS total_parados
    FROM
        fato_onibus_parados_linha
    WHERE
        id_tempo IN (SELECT id_tempo FROM latest_24_times)
    GROUP BY
        id_tempo
)
-- 4. Consulta final para calcular o percentual
SELECT
    -- Formata o rótulo do eixo X
    to_char(
        (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
        'DD/MM - HH24h'
    ) AS "Hora",
    
    COALESCE(t.total_ativos, 0) AS "Total de Ônibus Ativos",
    COALESCE(p.total_parados, 0) AS "Total de Ônibus Parados",
    
    -- Calcula o percentual
    ROUND(
        (COALESCE(p.total_parados, 0) * 100.0 / NULLIF(COALESCE(t.total_ativos, 0), 0))::numeric,
        2
    ) AS "Percentual Congestionado (%)"
FROM
    dim_tempo dt
LEFT JOIN
    total_por_hora t ON dt.id_tempo = t.id_tempo
LEFT JOIN
    parados_por_hora p ON dt.id_tempo = p.id_tempo
WHERE
    t.id_tempo IS NOT NULL -- Garante que estamos mostrando apenas horas que têm dados
ORDER BY
    dt.data_referencia ASC,
    dt.hora_referencia ASC;