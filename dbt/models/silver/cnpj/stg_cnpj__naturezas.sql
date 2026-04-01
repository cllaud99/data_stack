{{ config(materialized='view') }}

SELECT
    TRIM(codigo)    AS codigo,
    TRIM(descricao) AS descricao,
    _competencia,
    _data_extracao
FROM {{ source('bronze_cnpj', 'naturezas') }}
WHERE NULLIF(TRIM(codigo), '') IS NOT NULL
