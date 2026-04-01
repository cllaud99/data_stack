{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='cnpj',
    object_storage_source='nessie',
    object_storage_path='gold',
    dremio_space='data_stack',
    dremio_space_folder='gold'
) }}

-- Estabelecimentos ativos (situação cadastral 02) com domínios desnormalizados.
-- Filtro ativo reduz ~60M → ~20M linhas — viável para materialização Iceberg local.
-- Primeira tabela Iceberg real do domínio CNPJ na camada Gold.

WITH base AS (
    SELECT * FROM {{ ref('int_cnpj__estabelecimentos') }}
    WHERE cod_situacao_cadastral = '02'
    {% if is_incremental() %}
    AND _competencia > (SELECT MAX(_competencia) FROM {{ this }})
    {% endif %}
)

SELECT * FROM base
