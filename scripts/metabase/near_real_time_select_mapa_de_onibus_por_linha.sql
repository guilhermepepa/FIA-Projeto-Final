SELECT
  "public"."nrt_posicao_onibus_atual"."prefixo_onibus" AS "prefixo_onibus",
  "public"."nrt_posicao_onibus_atual"."letreiro_linha" AS "letreiro_linha",
  "public"."nrt_posicao_onibus_atual"."latitude" AS "latitude",
  "public"."nrt_posicao_onibus_atual"."longitude" AS "longitude",
  (
    "public"."nrt_posicao_onibus_atual"."timestamp_captura" AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  ) AS "horario_local_captura"
FROM
  "public"."nrt_posicao_onibus_atual"
WHERE
  "public"."nrt_posicao_onibus_atual"."timestamp_captura" >= (NOW() AT TIME ZONE 'UTC' - INTERVAL '30 minutes')
[[AND
  "public"."nrt_posicao_onibus_atual"."letreiro_linha" = {{letreiro_da_linha}}]]