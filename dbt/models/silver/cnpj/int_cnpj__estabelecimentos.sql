{{ config(materialized='view') }}

-- Passthrough limpo da stg com CNPJ completo derivado.
-- Sem joins — mantém o modelo leve para filter pushdown do Dremio.
-- Os joins de domínio (empresas, simples, municipios, cnaes etc.)
-- acontecem nos mrt_* APÓS o filtro de negócio, evitando OOM no hash join.

SELECT
    -- Identificação
    cnpj,
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    identificador_matriz_filial,

    -- Situação cadastral
    cod_situacao_cadastral,
    data_situacao_cadastral,
    cod_motivo_situacao_cadastral,
    situacao_especial,
    data_situacao_especial,

    -- CNAE
    cod_cnae_principal,
    cnae_fiscal_secundaria,

    -- Localização
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    cep,
    uf,
    cod_municipio,
    nome_cidade_exterior,
    cod_pais,

    -- Contato
    nome_fantasia,
    ddd_1,
    telefone_1,
    ddd_2,
    telefone_2,
    ddd_fax,
    fax,
    email,

    -- Datas
    data_inicio_atividade,

    -- Metadados
    _competencia,
    _data_extracao

FROM {{ ref('stg_cnpj__estabelecimentos') }}
