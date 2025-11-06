-- Schemas
CREATE SCHEMA IF NOT EXISTS delta.silver
WITH (location = 's3a://silver/');

CREATE SCHEMA IF NOT EXISTS delta.gold
WITH (location = 's3a://gold/');


-- Camada Silver
CALL delta.system.register_table(
  schema_name => 'silver',
  table_name => 'posicoes_onibus',
  table_location => 's3a://silver/posicoes_onibus/'
);

CALL delta.system.register_table(
  schema_name => 'silver',
  table_name => 'posicoes_onibus_streaming',
  table_location => 's3a://silver/posicoes_onibus_streaming/'
);

CALL delta.system.register_table(
  schema_name => 'silver',
  table_name => 'kpis_historicos_para_processar',
  table_location => 's3a://silver/kpis_historicos_para_processar/'
);


-- Camada Gold
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'fato_operacao_linhas_hora',
  table_location => 's3a://gold/fato_operacao_linhas_hora/'
);

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'fato_velocidade_linha',
  table_location => 's3a://gold/fato_velocidade_linha/'
);

CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'fato_onibus_parados_linha',
  table_location => 's3a://gold/fato_onibus_parados_linha/'
);