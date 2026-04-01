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
--
-- Estratégia anti-OOM: filtro de negócio aplicado ANTES dos JOINs.
-- Reduz 60M → ~20M linhas no CTE base antes de qualquer hash join.
--
-- Suporte a batch por UF via variável dbt:
--   dbt run --select mrt_cnpj__estabelecimentos_ativos --vars '{"uf": "SP"}'
-- Sem variável: processa todas as UFs de uma vez.

WITH ativos AS (
    SELECT * FROM {{ ref('stg_cnpj__estabelecimentos') }}
    WHERE cod_situacao_cadastral = '02'
    {% if var('uf', none) is not none %}
    AND uf = '{{ var("uf") }}'
    {% endif %}
    {% if is_incremental() %}
    AND _competencia > (SELECT MAX(_competencia) FROM {{ this }})
    {% endif %}
),

empresas AS (
    SELECT
        cnpj_basico,
        razao_social,
        capital_social,
        cod_porte_empresa,
        ente_federativo_responsavel,
        cod_natureza_juridica,
        cod_qualificacao_responsavel
    FROM {{ ref('stg_cnpj__empresas') }}
),

simples AS (
    SELECT
        cnpj_basico,
        opcao_pelo_simples,
        data_opcao_simples,
        data_exclusao_simples,
        opcao_pelo_mei,
        data_opcao_mei,
        data_exclusao_mei
    FROM {{ ref('stg_cnpj__simples') }}
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
        a.cnpj,
        a.cnpj_basico,
        a.cnpj_ordem,
        a.cnpj_dv,
        a.identificador_matriz_filial,

        -- Empresa (nível CNPJ básico)
        emp.razao_social,
        a.nome_fantasia,
        emp.capital_social,
        emp.cod_porte_empresa,
        CASE emp.cod_porte_empresa
            WHEN '00' THEN 'Não Informado'
            WHEN '01' THEN 'Micro Empresa'
            WHEN '03' THEN 'Empresa de Pequeno Porte'
            WHEN '05' THEN 'Demais'
        END                                  AS descricao_porte_empresa,
        emp.ente_federativo_responsavel,
        emp.cod_natureza_juridica,
        nat.descricao_natureza_juridica,
        emp.cod_qualificacao_responsavel,

        -- Situação cadastral
        a.cod_situacao_cadastral,
        a.data_situacao_cadastral,
        a.cod_motivo_situacao_cadastral,
        mot.descricao_motivo_situacao_cadastral,
        a.situacao_especial,
        a.data_situacao_especial,

        -- CNAE
        a.cod_cnae_principal,
        cnae.descricao_cnae_principal,
        a.cnae_fiscal_secundaria,

        -- Localização
        a.tipo_logradouro,
        a.logradouro,
        a.numero,
        a.complemento,
        a.bairro,
        a.cep,
        a.uf,
        a.cod_municipio,
        mun.nome_municipio,
        a.nome_cidade_exterior,
        a.cod_pais,
        pais.nome_pais,

        -- Contato
        a.ddd_1,
        a.telefone_1,
        a.ddd_2,
        a.telefone_2,
        a.ddd_fax,
        a.fax,
        a.email,

        -- Datas
        a.data_inicio_atividade,
        YEAR(a.data_inicio_atividade)  AS ano_abertura,
        MONTH(a.data_inicio_atividade) AS mes_abertura,

        -- Simples Nacional / MEI
        smp.opcao_pelo_simples,
        CASE WHEN smp.opcao_pelo_simples = 'S' THEN TRUE ELSE FALSE END AS is_simples,
        smp.data_opcao_simples,
        smp.data_exclusao_simples,
        smp.opcao_pelo_mei,
        CASE WHEN smp.opcao_pelo_mei = 'S' THEN TRUE ELSE FALSE END     AS is_mei,
        smp.data_opcao_mei,
        smp.data_exclusao_mei,

        -- Metadados
        a._competencia,
        a._data_extracao

    FROM ativos a
    LEFT JOIN empresas  emp  ON a.cnpj_basico             = emp.cnpj_basico
    LEFT JOIN simples   smp  ON a.cnpj_basico             = smp.cnpj_basico
    LEFT JOIN municipios mun ON a.cod_municipio           = mun.codigo
    LEFT JOIN cnaes     cnae ON a.cod_cnae_principal      = cnae.codigo
    LEFT JOIN naturezas  nat ON emp.cod_natureza_juridica = nat.codigo
    LEFT JOIN motivos    mot ON a.cod_motivo_situacao_cadastral = mot.codigo
    LEFT JOIN paises    pais ON a.cod_pais                = pais.codigo
)

SELECT * FROM final
