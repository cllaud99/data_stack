#!/usr/bin/env bash
# Materializa mrt_cnpj__estabelecimentos_ativos batch por UF.
# Cada UF é um MERGE incremental no Iceberg — seguro re-rodar (idempotente).
# Uso: ./scripts/run_mrt_por_uf.sh [modelo]
# Ex:  ./scripts/run_mrt_por_uf.sh mrt_cnpj__estabelecimentos_ativos

set -euo pipefail

MODEL="${1:-mrt_cnpj__estabelecimentos_ativos}"
DBT="../.venv/bin/dbt"
PROFILES_DIR="."

UFS=(AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO)

TOTAL=${#UFS[@]}
OK=0
FAIL=0
FAILED_UFS=()

echo "========================================"
echo " dbt batch por UF — modelo: $MODEL"
echo " Total de UFs: $TOTAL"
echo "========================================"

for UF in "${UFS[@]}"; do
    echo ""
    echo "→ [$((OK + FAIL + 1))/$TOTAL] Rodando UF: $UF"

    if $DBT run \
        --select "$MODEL" \
        --vars "{\"uf\": \"$UF\"}" \
        --profiles-dir "$PROFILES_DIR" \
        --no-populate-cache \
        2>&1 | grep -E "(OK|ERROR|WARN|rows)"; then
        OK=$((OK + 1))
        echo "  ✓ $UF OK"
    else
        FAIL=$((FAIL + 1))
        FAILED_UFS+=("$UF")
        echo "  ✗ $UF FALHOU"
    fi
done

echo ""
echo "========================================"
echo " Resultado: $OK/$TOTAL OK | $FAIL falhas"
if [ ${#FAILED_UFS[@]} -gt 0 ]; then
    echo " UFs com falha: ${FAILED_UFS[*]}"
fi
echo "========================================"
