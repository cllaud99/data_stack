{{ config(
    materialized='view',
    dremio_space='data_stack',
    dremio_space_folder='silver'
) }}

-- View de join das tabelas FATO. Materialização (Iceberg/Nessie) acontece nos mrt_*
-- onde os dados são filtrados por negócio (ativos, por UF, por CNAE, etc.).
-- Dataset nacional completo (~60M estabelecimentos) não cabe em hash join local.

WITH estabelecimentos AS (
    SELECT * FROM {{ ref('stg_cnpj__estabelecimentos') }}
    {% if is_incremental() %}
    WHERE _competencia > (SELECT MAX(_competencia) FROM {{ this }})
    {% endif %}
),

empresas AS (
    SELECT * FROM {{ ref('stg_cnpj__empresas') }}
),

simples AS (
    SELECT * FROM {{ ref('stg_cnpj__simples') }}
),

final AS (
    SELECT
        -- Identificação
        e.cnpj,
        e.cnpj_basico,
        e.cnpj_ordem,
        e.cnpj_dv,
        e.identificador_matriz_filial,

        -- Empresa (nível CNPJ básico — 8 dígitos)
        emp.razao_social,
        e.nome_fantasia,
        emp.capital_social,
        emp.cod_porte_empresa,
        emp.ente_federativo_responsavel,
        emp.cod_natureza_juridica,
        emp.cod_qualificacao_responsavel,

        -- Situação cadastral
        e.cod_situacao_cadastral,
        e.data_situacao_cadastral,
        e.cod_motivo_situacao_cadastral,
        e.situacao_especial,
        e.data_situacao_especial,

        -- CNAE
        e.cod_cnae_principal,
        e.cnae_fiscal_secundaria,

        -- Localização
        e.tipo_logradouro,
        e.logradouro,
        e.numero,
        e.complemento,
        e.bairro,
        e.cep,
        e.uf,
        e.cod_municipio,
        e.nome_cidade_exterior,
        e.cod_pais,

        -- Contato
        e.ddd_1,
        e.telefone_1,
        e.ddd_2,
        e.telefone_2,
        e.email,

        -- Datas
        e.data_inicio_atividade,

        -- Simples Nacional / MEI
        smp.opcao_pelo_simples,
        smp.data_opcao_simples,
        smp.data_exclusao_simples,
        smp.opcao_pelo_mei,
        smp.data_opcao_mei,
        smp.data_exclusao_mei,

        -- Metadados
        e._competencia,
        e._data_extracao

    FROM estabelecimentos e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples  smp ON e.cnpj_basico = smp.cnpj_basico
)

SELECT * FROM final
