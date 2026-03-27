"""
DAG: cnpj_ingestao_receita_federal

Fonte:    Receita Federal — Base pública de dados do CNPJ
          https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9

Destino:
  Landing  → landing/receita_federal/cnpj/competencia=YYYY-MM/<arquivo>.zip
  Bronze   → bronze/receita_federal/cnpj/tabela=<tabela>/competencia=YYYY-MM/part=NNNN/data.parquet

Frequência: mensal — dia 20 às 10:00 UTC
Camadas: landing (zip original, audit trail) → bronze (Parquet + Snappy + metadados)

Lógica de data:
  A competência é derivada diretamente da logical_date de cada execução.
  Ex: run de 2026-03-20 → competencia = "2026-03"

  Para processar meses históricos, use backfill:
    airflow dags backfill -s 2025-01-20 -e 2025-06-20 cnpj_ingestao_receita_federal

Arquivos ingeridos por competência:
  Tabelas fato (shardadas em 10 partes cada):
    Empresas0-9         → tabela: empresas           (7 colunas)
    Estabelecimentos0-9 → tabela: estabelecimentos   (30 colunas)
    Socios0-9           → tabela: socios             (11 colunas)
  Tabelas fato (arquivo único):
    Simples             → tabela: simples            (7 colunas)
  Tabelas de domínio:
    Cnaes, Motivos, Municipios, Naturezas, Paises, Qualificacoes (2 colunas)

Metadados adicionados no Bronze:
  _fonte          — identificador da fonte ("receita_federal_cnpj")
  _competencia    — período de referência YYYY-MM
  _arquivo_origem — nome do zip de origem
  _data_extracao  — data ISO da extração

Aviso de performance:
  Os arquivos de Estabelecimentos e Empresas podem ultrapassar 1 GB comprimidos.
  O processamento utiliza chunks de 100k linhas para controlar uso de memória.
  Tempo estimado por arquivo grande: 10–30 min (depende do hardware local).
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from airflow.sdk import dag, task

from connectors.receita_federal import SCHEMAS, listar_arquivos
from utils.storage import (
    bronze_key_tabela,
    download_file,
    ensure_bucket,
    landing_key,
    upload_file,
    write_parquet_df,
)

log = logging.getLogger(__name__)

# ─── Constantes ───────────────────────────────────────────────────────────────
_LANDING_BUCKET = "landing"
_BRONZE_BUCKET = "bronze"
_DOWNLOAD_CHUNK = 8 * 1024 * 1024       # 8 MB por chunk de download HTTP
_CSV_CHUNKSIZE = 100_000                 # linhas por chunk de leitura CSV
_MAX_CONCURRENT = 2                      # max tasks paralelas — limitado pelo rate limit do servidor RF
_LOG_PROGRESS_BYTES = 100 * 1024 * 1024  # loga progresso a cada 100 MB

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="cnpj_ingestao_receita_federal",
    description="Ingestão mensal da base pública de CNPJ da Receita Federal",
    schedule="0 10 20 * *",  # dia 20 de cada mês às 10:00 UTC
    start_date=datetime(2023, 5, 20, tzinfo=timezone.utc),  # primeira competência disponível
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["landing", "bronze", "receita_federal", "cnpj"],
    max_active_tasks=_MAX_CONCURRENT * 2,
)
def cnpj_ingestao_receita_federal():

    @task
    def listar_arquivos_da_competencia(competencia: str) -> list[dict]:
        """Lista todos os arquivos zip disponíveis para a competência."""
        log.info("── Listando arquivos para competência %s ...", competencia)
        arquivos = listar_arquivos(competencia)
        total_gb = sum(a["size_bytes"] for a in arquivos) / 1024**3
        log.info(
            "✔ %d arquivos encontrados | %.2f GB comprimidos total",
            len(arquivos),
            total_gb,
        )
        for a in arquivos:
            log.info(
                "  • %-30s %6.0f MB  tabela=%-20s",
                a["nome"],
                a["size_bytes"] / 1024**2,
                a["tabela"],
            )
        return arquivos

    @task(
        max_active_tis_per_dagrun=_MAX_CONCURRENT,
        execution_timeout=timedelta(hours=3),
    )
    def baixar_para_landing(arquivo: dict) -> dict:
        """Faz download do zip via HTTP e armazena no bucket landing.

        Usa streaming chunk-by-chunk para evitar carregar o arquivo inteiro em RAM.
        O zip original é preservado sem modificação (audit trail).
        """
        nome = arquivo["nome"]
        pasta = arquivo["pasta"]
        url = arquivo["url"]
        auth = (arquivo["auth_user"], arquivo["auth_pass"])
        size_bytes = arquivo["size_bytes"]
        size_mb = size_bytes / 1024 / 1024

        log.info("━━━ DOWNLOAD: %s (%.1f MB esperados) ━━━", nome, size_mb)
        time.sleep(2)  # pequena pausa para não sobrecarregar o servidor RF
        t0 = time.monotonic()

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip", prefix=f"rf_{nome}_")
        os.close(tmp_fd)

        try:
            with requests.get(url, auth=auth, stream=True, timeout=3600) as resp:
                resp.raise_for_status()
                bytes_downloaded = 0
                next_log_at = _LOG_PROGRESS_BYTES

                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=_DOWNLOAD_CHUNK):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                        if bytes_downloaded >= next_log_at:
                            pct = (bytes_downloaded / size_bytes * 100) if size_bytes else 0
                            elapsed = time.monotonic() - t0
                            speed = bytes_downloaded / elapsed / 1024 / 1024
                            log.info(
                                "  ↓ %s — %.0f MB / %.0f MB (%.0f%%) @ %.1f MB/s",
                                nome,
                                bytes_downloaded / 1024 / 1024,
                                size_mb,
                                pct,
                                speed,
                            )
                            next_log_at += _LOG_PROGRESS_BYTES

            elapsed = time.monotonic() - t0
            speed = bytes_downloaded / elapsed / 1024 / 1024
            log.info(
                "✔ Download concluído: %s — %.1f MB em %.0fs (%.1f MB/s)",
                nome,
                bytes_downloaded / 1024 / 1024,
                elapsed,
                speed,
            )

            ensure_bucket(_LANDING_BUCKET)
            log.info("  → Enviando para landing zone (MinIO) ...")
            l_key = landing_key("receita_federal", "cnpj", pasta, nome)
            path = upload_file(tmp_path, bucket=_LANDING_BUCKET, key=l_key)
            log.info("✔ Landing gravada: %s", path)

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                log.info("  🗑 Temp file removido: %s", tmp_path)

        return {
            "nome": arquivo["nome"],
            "tabela": arquivo["tabela"],
            "pasta": pasta,
            "landing_key": l_key,
        }

    @task(
        max_active_tis_per_dagrun=_MAX_CONCURRENT,
        execution_timeout=timedelta(hours=4),
    )
    def processar_para_bronze(info: dict) -> list[str]:
        """Extrai o CSV do zip (landing) e grava em Parquet particionado no Bronze.

        Pipeline por arquivo:
          1. Baixa o zip do MinIO landing para /tmp
          2. Abre o zip e lê o CSV interno em chunks de 100k linhas
          3. Adiciona colunas de metadados em cada chunk
          4. Grava cada chunk como um arquivo Parquet separado no Bronze
        """
        nome = info["nome"]
        tabela = info["tabela"]
        competencia = info["pasta"]
        l_key = info["landing_key"]
        extraction_date = date.today().isoformat()

        log.info("━━━ PROCESSAMENTO: %s → tabela=%s ━━━", nome, tabela)
        t0 = time.monotonic()

        columns = SCHEMAS.get(tabela)
        if columns is None:
            raise ValueError(f"Schema não definido para tabela '{tabela}' (arquivo: {nome})")

        log.info("  Schema: %d colunas — %s ...", len(columns), columns[:3])

        tmp_fd, tmp_zip = tempfile.mkstemp(suffix=".zip", prefix=f"rf_proc_{nome}_")
        os.close(tmp_fd)
        parquet_paths = []
        total_rows = 0

        ensure_bucket(_BRONZE_BUCKET)

        try:
            log.info("  ← Baixando zip da landing zone ...")
            download_file(bucket=_LANDING_BUCKET, key=l_key, local_path=tmp_zip)
            log.info("  ✔ Zip baixado: %.1f MB", os.path.getsize(tmp_zip) / 1024 / 1024)

            with zipfile.ZipFile(tmp_zip, "r") as zf:
                inner_files = zf.namelist()
                if not inner_files:
                    raise RuntimeError(f"Zip vazio: {nome}")
                csv_name = inner_files[0]
                log.info("  CSV interno: %s  (total de arquivos no zip: %d)", csv_name, len(inner_files))

                with zf.open(csv_name) as raw_stream:
                    text_io = io.TextIOWrapper(raw_stream, encoding="iso-8859-1")

                    reader = pd.read_csv(
                        text_io,
                        sep=";",
                        header=None,
                        names=columns,
                        dtype=str,
                        chunksize=_CSV_CHUNKSIZE,
                        quotechar='"',
                        on_bad_lines="warn",
                        low_memory=False,
                    )

                    for part_idx, chunk in enumerate(reader):
                        chunk["_fonte"] = "receita_federal_cnpj"
                        chunk["_competencia"] = competencia
                        chunk["_arquivo_origem"] = nome
                        chunk["_data_extracao"] = extraction_date

                        b_key = bronze_key_tabela(
                            dominio="receita_federal",
                            fonte="cnpj",
                            tabela=tabela,
                            competencia=competencia,
                            part=part_idx,
                        )
                        path = write_parquet_df(chunk, bucket=_BRONZE_BUCKET, key=b_key)
                        parquet_paths.append(path)
                        total_rows += len(chunk)

                        log.info(
                            "  part %04d → %d linhas | acumulado: %d linhas | %s",
                            part_idx,
                            len(chunk),
                            total_rows,
                            path,
                        )

        finally:
            if os.path.exists(tmp_zip):
                os.unlink(tmp_zip)
                log.info("  🗑 Temp file removido: %s", tmp_zip)

        elapsed = time.monotonic() - t0
        log.info(
            "✔ Processamento concluído: %s — %d linhas em %d partes | %.0fs",
            nome,
            total_rows,
            len(parquet_paths),
            elapsed,
        )
        return parquet_paths

    @task
    def resumo(resultados: list[list[str]], competencia: str) -> None:
        """Loga o resumo consolidado da ingestão."""
        total_partes = sum(len(r) for r in resultados)
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        log.info("✅ INGESTÃO CNPJ CONCLUÍDA")
        log.info("   Competência   : %s", competencia)
        log.info("   Arquivos      : %d", len(resultados))
        log.info("   Partes Parquet: %d", total_partes)
        log.info("   Landing : s3://%s/receita_federal/cnpj/competencia=%s/", _LANDING_BUCKET, competencia)
        log.info("   Bronze  : s3://%s/receita_federal/cnpj/tabela=*/competencia=%s/", _BRONZE_BUCKET, competencia)
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # ─── Orquestração ─────────────────────────────────────────────────────────
    # A competência vem direto da logical_date — sem params, sem override manual.
    # Backfill de 2025-01 a 2025-06:
    #   airflow dags backfill -s 2025-01-20 -e 2025-06-20 cnpj_ingestao_receita_federal
    competencia = "{{ logical_date.strftime('%Y-%m') }}"

    arquivos = listar_arquivos_da_competencia(competencia)
    landing_infos = baixar_para_landing.expand(arquivo=arquivos)
    resultados = processar_para_bronze.expand(info=landing_infos)
    resumo(resultados, competencia)


cnpj_ingestao_receita_federal()
