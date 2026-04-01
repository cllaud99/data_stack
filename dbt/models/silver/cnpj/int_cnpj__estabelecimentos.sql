{{ config(materialized='view') }}

-- Join de FATO (estabelecimentos + empresas + simples) com domínios desnormalizados.
-- Mantido como view para aproveitar filter pushdown do Dremio — o dataset nacional (~60M)
-- não cabe em hash join local. Materialização Iceberg começa nos mrt_* com filtros de negócio.

WITH estabelecimentos AS (
    SELECT * FROM {{ ref('stg_cnpj__estabelecimentos') }}
),

empresas AS (
    SELECT * FROM {{ ref('stg_cnpj__empresas') }}
),

simples AS (
    SELECT * FROM {{ ref('stg_cnpj__simples') }}
),

municipios AS (
    SELECT codigo, descricao AS nome_municipio FROM {{ ref('stg_cnpj__municipios') }}
),

cnaes AS (
    SELECT codigo, descricao AS descricao_cnae_principal FROM {{ ref('stg_cnpj__cnaes') }}
),

naturezas AS (
    SELECT codigo, descricao AS descricao_natureza_juridica FROM {{ ref('stg_cnpj__naturezas') }}
),

motivos AS (
    SELECT codigo, descricao AS descricao_motivo_situacao_cadastral FROM {{ ref('stg_cnpj__motivos') }}
),

paises AS (
    SELECT codigo, descricao AS nome_pais FROM {{ ref('stg_cnpj__paises') }}
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
        nat.descricao_natureza_juridica,
        emp.cod_qualificacao_responsavel,

        -- Situação cadastral
        e.cod_situacao_cadastral,
        e.data_situacao_cadastral,
        e.cod_motivo_situacao_cadastral,
        mot.descricao_motivo_situacao_cadastral,
        e.situacao_especial,
        e.data_situacao_especial,

        -- CNAE
        e.cod_cnae_principal,
        cnae.descricao_cnae_principal,
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
        mun.nome_municipio,
        e.nome_cidade_exterior,
        e.cod_pais,
        pais.nome_pais,

        -- Contato
        e.ddd_1,
        e.telefone_1,
        e.ddd_2,
        e.telefone_2,
        e.ddd_fax,
        e.fax,
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
    LEFT JOIN empresas  emp  ON e.cnpj_basico             = emp.cnpj_basico
    LEFT JOIN simples   smp  ON e.cnpj_basico             = smp.cnpj_basico
    LEFT JOIN municipios mun ON e.cod_municipio           = mun.codigo
    LEFT JOIN cnaes     cnae ON e.cod_cnae_principal      = cnae.codigo
    LEFT JOIN naturezas  nat ON emp.cod_natureza_juridica = nat.codigo
    LEFT JOIN motivos    mot ON e.cod_motivo_situacao_cadastral = mot.codigo
    LEFT JOIN paises    pais ON e.cod_pais                = pais.codigo
)

SELECT * FROM final
