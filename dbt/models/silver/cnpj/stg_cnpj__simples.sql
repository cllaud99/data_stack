{{ config(materialized='view') }}

SELECT
    TRIM(cnpj_basico)                        AS cnpj_basico,
    NULLIF(TRIM(opcao_pelo_simples), '')      AS opcao_pelo_simples,
    {{ parse_date_br('data_opcao_simples') }} AS data_opcao_simples,
    {{ parse_date_br('data_exclusao_simples') }} AS data_exclusao_simples,
    NULLIF(TRIM(opcao_pelo_mei), '')          AS opcao_pelo_mei,
    {{ parse_date_br('data_opcao_mei') }}     AS data_opcao_mei,
    {{ parse_date_br('data_exclusao_mei') }}  AS data_exclusao_mei,
    _competencia,
    _data_extracao
FROM {{ source('bronze_cnpj', 'simples') }}
WHERE NULLIF(TRIM(cnpj_basico), '') IS NOT NULL
