{{ config(
    materialized='table',
    object_storage_source='nessie',
    object_storage_path='gold',
    dremio_space='data_stack',
    dremio_space_folder='gold',
    partition_by=['uf'],
    localsort_by=['mes_referencia']
) }}

-- Dinâmica mensal de abertura e fechamento de estabelecimentos por UF e CNAE.
-- Permite análise de tendência, sazonalidade e saldo líquido (aberturas - baixadas).
--
-- Granularidade: 1 linha por (mes_referencia, uf, cod_cnae_principal).
-- Fechamento = situação BAIXADA (08). Inaptas (04) e suspensas (03) ficam em colunas
-- separadas pois são estados reversíveis — não equivalem a encerramento definitivo.
--
-- Fonte: int_cnpj__estabelecimentos (view sobre 60M linhas).
-- Saída: ~100K-500K linhas após agregação — viável como table full-refresh.

WITH aberturas AS (
    SELECT
        DATE_TRUNC('month', data_inicio_atividade) AS mes_referencia,
        uf,
        cod_cnae_principal,
        COUNT(*)                                   AS qtd_aberturas
    FROM {{ ref('int_cnpj__estabelecimentos') }}
    WHERE data_inicio_atividade IS NOT NULL
      AND uf IS NOT NULL
    GROUP BY 1, 2, 3
),

fechamentos AS (
    SELECT
        DATE_TRUNC('month', data_situacao_cadastral) AS mes_referencia,
        uf,
        cod_cnae_principal,
        COUNT(*) FILTER (WHERE cod_situacao_cadastral = '08') AS qtd_baixadas,
        COUNT(*) FILTER (WHERE cod_situacao_cadastral = '04') AS qtd_inaptadas,
        COUNT(*) FILTER (WHERE cod_situacao_cadastral = '03') AS qtd_suspensas
    FROM {{ ref('int_cnpj__estabelecimentos') }}
    WHERE cod_situacao_cadastral IN ('03', '04', '08')
      AND data_situacao_cadastral IS NOT NULL
      AND uf IS NOT NULL
    GROUP BY 1, 2, 3
),

cnaes AS (
    SELECT codigo, descricao AS descricao_cnae_principal FROM {{ ref('stg_cnpj__cnaes') }}
),

agregado AS (
    SELECT
        COALESCE(a.mes_referencia,       f.mes_referencia)       AS mes_referencia,
        COALESCE(a.uf,                   f.uf)                   AS uf,
        COALESCE(a.cod_cnae_principal,   f.cod_cnae_principal)   AS cod_cnae_principal,

        COALESCE(a.qtd_aberturas, 0)  AS qtd_aberturas,
        COALESCE(f.qtd_baixadas,  0)  AS qtd_baixadas,
        COALESCE(f.qtd_inaptadas, 0)  AS qtd_inaptadas,
        COALESCE(f.qtd_suspensas, 0)  AS qtd_suspensas,

        -- Saldo líquido: positivo = crescimento, negativo = retração
        COALESCE(a.qtd_aberturas, 0) - COALESCE(f.qtd_baixadas, 0) AS saldo_liquido

    FROM aberturas a
    FULL OUTER JOIN fechamentos f
        ON  a.mes_referencia     = f.mes_referencia
        AND a.uf                 = f.uf
        AND a.cod_cnae_principal = f.cod_cnae_principal
),

final AS (
    SELECT
        a.*,
        c.descricao_cnae_principal
    FROM agregado a
    LEFT JOIN cnaes c ON a.cod_cnae_principal = c.codigo
)

SELECT * FROM final
WHERE mes_referencia IS NOT NULL
ORDER BY mes_referencia, uf, cod_cnae_principal
