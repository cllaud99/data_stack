"""
DAG: cnpj_ingestao_receita_federal

Fonte:    Receita Federal — Base pública de dados do CNPJ
          https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9

Destino:
  Landing  → landing/receita_federal/cnpj/competencia=YYYY-MM/<arquivo>.zip
  Bronze   → bronze/receita_federal/cnpj/tabela=<tabela>/competencia=YYYY-MM/part=NNNN/data.parquet

Frequência: mensal — dia 20 às 10:00 UTC (dados geralmente disponíveis na 2ª semana do mês)
Camadas: landing (zip original, audit trail) → bronze (Parquet + Snappy + metadados)

Arquivos ingeridos por competência:
  Tabelas fato (shardadas em 10 partes cada):
    Empresas0-9        → tabela: empresas          (7 colunas)
    Estabelecimentos0-9 → tabela: estabelecimentos (30 colunas)
    Socios0-9          → tabela: socios            (11 colunas)
  Tabelas fato (arquivo único):
    Simples            → tabela: simples           (7 colunas)
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
import zipfile
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.sdk import Asset

from connectors.receita_federal import SCHEMAS, listar_arquivos, pasta_mais_recente
from utils.storage import (
    bronze_key_tabela,
    download_file,
    landing_key,
    upload_file,
    write_parquet_df,
)

log = logging.getLogger(__name__)

# ─── Assets (Airflow 3) ────────────────────────────────────────────────────────
bronze_cnpj = Asset("s3://bronze/receita_federal/cnpj")

# ─── Constantes ───────────────────────────────────────────────────────────────
_LANDING_BUCKET = "landing"
_BRONZE_BUCKET = "bronze"
_DOWNLOAD_CHUNK = 8 * 1024 * 1024   # 8 MB por chunk de download HTTP
_CSV_CHUNKSIZE = 100_000             # linhas por chunk de leitura CSV
_MAX_CONCURRENT = 4                  # max tasks paralelas (poupa RAM/disco)

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
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["landing", "bronze", "receita_federal", "cnpj"],
    outlets=[bronze_cnpj],
    max_active_tasks=_MAX_CONCURRENT * 2,
)
def cnpj_ingestao_receita_federal():

    @task
    def descobrir_competencia() -> str:
        """Identifica a pasta mais recente no repositório da Receita Federal."""
        competencia = pasta_mais_recente()
        log.info("Competência mais recente encontrada: %s", competencia)
        return competencia

    @task
    def listar_arquivos_da_competencia(competencia: str) -> list[dict]:
        """Lista todos os arquivos zip disponíveis para a competência."""
        arquivos = listar_arquivos(competencia)
        log.info(
            "Arquivos encontrados para %s: %d (%.1f GB comprimidos)",
            competencia,
            len(arquivos),
            sum(a["size_bytes"] for a in arquivos) / 1024**3,
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

        Retorna dict com informações para a etapa de processamento.
        """
        nome = arquivo["nome"]
        pasta = arquivo["pasta"]
        url = arquivo["url"]
        auth = (arquivo["auth_user"], arquivo["auth_pass"])
        size_mb = arquivo["size_bytes"] / 1024 / 1024

        log.info("Iniciando download: %s (%.1f MB)", nome, size_mb)

        # Baixa para arquivo temporário (streaming — não carrega tudo em RAM)
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip", prefix=f"rf_{nome}_")
        os.close(tmp_fd)

        try:
            with requests.get(url, auth=auth, stream=True, timeout=3600) as resp:
                resp.raise_for_status()
                bytes_downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=_DOWNLOAD_CHUNK):
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

            log.info("Download concluído: %s (%.1f MB recebidos)", nome, bytes_downloaded / 1024 / 1024)

            # Upload para MinIO landing
            l_key = landing_key("receita_federal", "cnpj", pasta, nome)
            path = upload_file(tmp_path, bucket=_LANDING_BUCKET, key=l_key)
            log.info("Arquivo gravado na landing: %s", path)

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

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

        Todos os campos são lidos como string (dtype=str) — camada raw/bronze
        não aplica transformações de tipo.
        """
        nome = info["nome"]
        tabela = info["tabela"]
        competencia = info["pasta"]
        l_key = info["landing_key"]
        extraction_date = date.today().isoformat()

        log.info("Iniciando processamento: %s → tabela=%s", nome, tabela)

        columns = SCHEMAS.get(tabela)
        if columns is None:
            raise ValueError(f"Schema não definido para tabela '{tabela}' (arquivo: {nome})")

        tmp_fd, tmp_zip = tempfile.mkstemp(suffix=".zip", prefix=f"rf_proc_{nome}_")
        os.close(tmp_fd)
        parquet_paths = []

        try:
            # 1. Baixa zip do MinIO landing
            download_file(bucket=_LANDING_BUCKET, key=l_key, local_path=tmp_zip)

            # 2. Abre o zip e localiza o CSV interno (geralmente único arquivo)
            with zipfile.ZipFile(tmp_zip, "r") as zf:
                inner_files = zf.namelist()
                if not inner_files:
                    raise RuntimeError(f"Zip vazio: {nome}")
                csv_name = inner_files[0]
                log.info("CSV interno: %s (arquivos no zip: %s)", csv_name, inner_files)

                with zf.open(csv_name) as raw_stream:
                    # Wrap com TextIOWrapper para pandas aceitar encoding
                    text_io = io.TextIOWrapper(raw_stream, encoding="iso-8859-1")

                    reader = pd.read_csv(
                        text_io,
                        sep=";",
                        header=None,
                        names=columns,
                        dtype=str,           # bronze = raw, sem cast de tipos
                        chunksize=_CSV_CHUNKSIZE,
                        quotechar='"',
                        on_bad_lines="warn",
                        low_memory=False,
                    )

                    # 3. Processa e grava cada chunk
                    for part_idx, chunk in enumerate(reader):
                        # Metadados de rastreabilidade
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

                        if part_idx % 10 == 0:
                            log.info("  part %04d gravada: %s", part_idx, path)

        finally:
            if os.path.exists(tmp_zip):
                os.unlink(tmp_zip)

        log.info(
            "Processamento concluído: %s → %d partes Parquet gravadas",
            nome,
            len(parquet_paths),
        )
        return parquet_paths

    @task
    def resumo(resultados: list[list[str]], competencia: str) -> None:
        """Loga o resumo da ingestão."""
        total_partes = sum(len(r) for r in resultados)
        total_arquivos = len(resultados)
        log.info("=== Ingestão CNPJ concluída ===")
        log.info("  Competência: %s", competencia)
        log.info("  Arquivos processados: %d", total_arquivos)
        log.info("  Partes Parquet gravadas: %d", total_partes)

    # ─── Orquestração ─────────────────────────────────────────────────────────
    competencia = descobrir_competencia()
    arquivos = listar_arquivos_da_competencia(competencia)

    # Fan-out: download paralelo (limitado a _MAX_CONCURRENT simultâneos)
    landing_infos = baixar_para_landing.expand(arquivo=arquivos)

    # Fan-out: processamento paralelo (cada task aguarda seu landing_info)
    resultados = processar_para_bronze.expand(info=landing_infos)

    resumo(resultados, competencia)


cnpj_ingestao_receita_federal()
