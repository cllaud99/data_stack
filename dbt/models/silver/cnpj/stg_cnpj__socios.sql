{{ config(materialized='view') }}

SELECT
    TRIM(cnpj_basico)                                    AS cnpj_basico,
    NULLIF(TRIM(identificador_socio), '')                AS identificador_socio,
    NULLIF(TRIM(nome_socio_razao_social), '')            AS nome_socio_razao_social,
    NULLIF(TRIM(cnpj_cpf_socio), '')                     AS cnpj_cpf_socio,
    NULLIF(TRIM(qualificacao_socio), '')                 AS cod_qualificacao_socio,
    CASE
        WHEN NULLIF(TRIM(data_entrada_sociedade), '00000000') IS NULL THEN NULL
        ELSE TO_DATE(TRIM(data_entrada_sociedade), 'YYYYMMDD')
    END                                                  AS data_entrada_sociedade,
    NULLIF(TRIM(pais), '')                               AS cod_pais,
    NULLIF(TRIM(representante_legal), '')                AS cpf_representante_legal,
    NULLIF(TRIM(nome_representante), '')                 AS nome_representante,
    NULLIF(TRIM(qualificacao_representante_legal), '')   AS cod_qualificacao_representante,
    NULLIF(TRIM(faixa_etaria), '')                       AS cod_faixa_etaria,
    _competencia,
    _data_extracao
FROM {{ source('bronze_cnpj', 'socios') }}
WHERE NULLIF(TRIM(cnpj_basico), '') IS NOT NULL
