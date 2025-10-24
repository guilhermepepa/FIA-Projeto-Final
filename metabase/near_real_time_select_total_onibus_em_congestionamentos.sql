SELECT
  -- Como a tabela 'nrt_onibus_parados_linha' contém APENAS o estado mais recente,
  COALESCE(SUM(quantidade_onibus_parados), 0) AS "Total de Ônibus Parados"
FROM
  nrt_onibus_parados_linha;
