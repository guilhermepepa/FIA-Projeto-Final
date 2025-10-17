-- 1. CTE para encontrar o ID da ÚLTIMA HORA COMPLETA (ignora a hora atual que está sendo processada pelo streaming)
WITH latest_complete_time AS (
  SELECT MAX(id_tempo) AS max_id_tempo
  FROM fato_operacao_linhas_hora
),
-- 2. CTE para encontrar os últimos 24 'id_tempo' consolidados (aumentamos a janela de busca para garantir que encontraremos 12 períodos válidos)
latest_24_consolidated_times AS (
  SELECT DISTINCT
    id_tempo
  FROM
    fato_operacao_linhas_hora
  WHERE
    id_tempo < (SELECT max_id_tempo FROM latest_complete_time)
  ORDER BY
    id_tempo DESC
  LIMIT 24
),
-- 3. CTE para calcular o total de ônibus POR HORA e filtrar as horas com baixa atividade geral da frota
valid_times AS (
    SELECT
        id_tempo
    FROM
        fato_operacao_linhas_hora
    WHERE
        id_tempo IN (SELECT id_tempo FROM latest_24_consolidated_times)
    GROUP BY
        id_tempo
    HAVING
        SUM(quantidade_onibus) >= 10
),
-- 4. CTE para pegar os 12 'id_tempo' mais recentes DENTRE OS VÁLIDOS
final_12_times AS (
    SELECT
        id_tempo
    FROM
        valid_times
    ORDER BY
        id_tempo DESC
    LIMIT 12
)
-- 5. Consulta final, usando a lista filtrada e consolidada de tempos
SELECT
  -- Cria o rótulo completo da linha
  (dl.letreiro_linha || ' - ' || dl.nome_linha) AS "Linha",
  
  -- Formata o timestamp UTC para uma string local legível no formato "DD/MM - HHh"
  to_char(
    (dt.data_referencia + dt.hora_referencia * interval '1 hour') AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
    'DD/MM - HH24h'
  ) AS "Dia e Hora Local",
  
  -- Soma a quantidade de ônibus da tabela de fatos
  SUM(f.quantidade_onibus) AS "Total de Ônibus"
FROM
  fato_operacao_linhas_hora f
  -- Junta com as dimensões para obter as descrições
  JOIN dim_tempo dt ON f.id_tempo = dt.id_tempo
  JOIN dim_linha dl ON f.id_linha = dl.id_linha
WHERE
  -- Filtra para pegar apenas os registros cujo id_tempo está na nossa lista final dos 12 mais recentes e válidos
  f.id_tempo IN (SELECT id_tempo FROM final_12_times)
GROUP BY
  "Linha",
  "Dia e Hora Local",
  dt.data_referencia,
  dt.hora_referencia
ORDER BY
  -- Ordenamos pela data e hora originais para que as colunas da Tabela Dinâmica fiquem na ordem correta
  dt.data_referencia ASC,
  dt.hora_referencia ASC;

