{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['cnpj_basico', 'cnpj_cpf_socio'],
    object_storage_source='nessie',
    object_storage_path='silver',
    dremio_space='data_stack',
    dremio_space_folder='silver'
) }}

-- Nota: cnpj_cpf_socio usa '00000000000' como placeholder para sócios anônimos
-- (a Receita Federal não retorna NULL puro), então o unique_key funciona corretamente.

WITH socios AS (
    SELECT * FROM {{ ref('stg_cnpj__socios') }}
    {% if is_incremental() %}
    WHERE _competencia > (SELECT MAX(_competencia) FROM {{ this }})
    {% endif %}
),

paises AS (
    SELECT codigo, descricao AS nome_pais FROM {{ ref('stg_cnpj__paises') }}
),

qualificacoes_socio AS (
    SELECT codigo, descricao AS descricao_qualificacao_socio
    FROM {{ ref('stg_cnpj__qualificacoes') }}
),

qualificacoes_rep AS (
    SELECT codigo, descricao AS descricao_qualificacao_representante
    FROM {{ ref('stg_cnpj__qualificacoes') }}
),

final AS (
    SELECT
        s.cnpj_basico,
        s.identificador_socio,
        s.nome_socio_razao_social,
        s.cnpj_cpf_socio,

        -- Qualificação do sócio
        s.cod_qualificacao_socio,
        qs.descricao_qualificacao_socio,

        s.data_entrada_sociedade,

        -- País do sócio (estrangeiros)
        s.cod_pais,
        p.nome_pais,

        -- Representante legal (para sócios PJ)
        s.cpf_representante_legal,
        s.nome_representante,
        s.cod_qualificacao_representante,
        qr.descricao_qualificacao_representante,

        s.cod_faixa_etaria,

        -- Metadados
        s._competencia,
        s._data_extracao

    FROM socios s
    LEFT JOIN paises              p  ON s.cod_pais                    = p.codigo
    LEFT JOIN qualificacoes_socio qs ON s.cod_qualificacao_socio      = qs.codigo
    LEFT JOIN qualificacoes_rep   qr ON s.cod_qualificacao_representante = qr.codigo
)

SELECT * FROM final
