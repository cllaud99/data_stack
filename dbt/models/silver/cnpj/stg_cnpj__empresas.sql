{{ config(materialized='view') }}

SELECT
    TRIM(cnpj_basico)                                                        AS cnpj_basico,
    NULLIF(TRIM(razao_social), '')                                           AS razao_social,
    NULLIF(TRIM(natureza_juridica), '')                                      AS cod_natureza_juridica,
    NULLIF(TRIM(qualificacao_responsavel), '')                               AS cod_qualificacao_responsavel,
    CAST(REPLACE(NULLIF(TRIM(capital_social), ''), ',', '.') AS DOUBLE)     AS capital_social,
    NULLIF(TRIM(porte_empresa), '')                                          AS cod_porte_empresa,
    NULLIF(TRIM(ente_federativo_responsavel), '')                            AS ente_federativo_responsavel,
    _competencia,
    _data_extracao
FROM {{ source('bronze_cnpj', 'empresas') }}
WHERE NULLIF(TRIM(cnpj_basico), '') IS NOT NULL
