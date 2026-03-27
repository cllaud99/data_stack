"""
Utilitários para escrita no MinIO (S3-compatible).

Convenção de paths por camada:

  Landing (arquivos brutos, formato original):
    landing/{dominio}/{fonte}/competencia={YYYY-MM}/{arquivo}
    Ex: landing/receita_federal/cnpj/competencia=2026-03/Empresas0.zip

  Bronze (Hive-style, formato analítico):
    bronze/{dominio}/{fonte}/serie={nome}/data_extracao={YYYY-MM-DD}/data.{ext}
    bronze/{dominio}/{fonte}/tabela={nome}/competencia={YYYY-MM}/part={NNNN}/data.parquet
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import date
from typing import Any

import boto3
import pandas as pd
from botocore.client import Config

log = logging.getLogger(__name__)

# ─── Configuração via variáveis de ambiente ────────────────────────────────────
_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "changeme")


def _get_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def write_json(
    data: list[dict[str, Any]],
    bucket: str,
    key: str,
) -> str:
    """Grava lista de dicts como JSON no MinIO. Retorna o path s3://bucket/key."""
    client = _get_client()
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    path = f"s3://{bucket}/{key}"
    log.info("JSON gravado: %s (%d bytes)", path, len(body))
    return path


def write_parquet(
    records: list[dict[str, Any]],
    bucket: str,
    key: str,
) -> str:
    """Converte lista de dicts para DataFrame e grava como Parquet no MinIO."""
    client = _get_client()
    df = pd.DataFrame(records)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    path = f"s3://{bucket}/{key}"
    log.info("Parquet gravado: %s (%d linhas)", path, len(df))
    return path


def write_parquet_df(df: pd.DataFrame, bucket: str, key: str) -> str:
    """Grava um DataFrame Pandas como Parquet no MinIO (Snappy). Retorna s3://bucket/key."""
    client = _get_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    path = f"s3://{bucket}/{key}"
    log.info("Parquet gravado: %s (%d linhas)", path, len(df))
    return path


def upload_file(local_path: str, bucket: str, key: str) -> str:
    """Faz upload de arquivo local para MinIO com multipart automático (boto3).

    Indicado para arquivos grandes (zip, CSV, etc.). Retorna s3://bucket/key.
    """
    client = _get_client()
    file_size = os.path.getsize(local_path)
    client.upload_file(local_path, bucket, key)
    path = f"s3://{bucket}/{key}"
    log.info("Arquivo enviado: %s (%.1f MB)", path, file_size / 1024 / 1024)
    return path


def download_file(bucket: str, key: str, local_path: str) -> str:
    """Baixa arquivo do MinIO para o sistema de arquivos local. Retorna local_path."""
    client = _get_client()
    client.download_file(bucket, key, local_path)
    size = os.path.getsize(local_path)
    log.info("Arquivo baixado: s3://%s/%s → %s (%.1f MB)", bucket, key, local_path, size / 1024 / 1024)
    return local_path


def bronze_key(dominio: str, fonte: str, serie: str, ext: str, extraction_date: date) -> str:
    """
    Monta o path Hive-style para a camada Bronze (séries temporais de API).
    Exemplo: financeiro/bcb/serie=selic_diaria/data_extracao=2025-03-26/data.parquet
    """
    return (
        f"{dominio}/{fonte}"
        f"/serie={serie}"
        f"/data_extracao={extraction_date.isoformat()}"
        f"/data.{ext}"
    )


def bronze_key_tabela(
    dominio: str,
    fonte: str,
    tabela: str,
    competencia: str,
    part: int,
) -> str:
    """
    Monta o path Hive-style para Bronze de dados em bulk (arquivos mensais/particionados).

    Args:
        dominio:     domínio de negócio (ex: 'receita_federal')
        fonte:       nome da fonte (ex: 'cnpj')
        tabela:      nome da tabela (ex: 'empresas', 'estabelecimentos')
        competencia: período de referência YYYY-MM (ex: '2026-03')
        part:        índice da parte (para arquivos grandes divididos em chunks)

    Exemplo:
        receita_federal/cnpj/tabela=empresas/competencia=2026-03/part=0000/data.parquet
    """
    return (
        f"{dominio}/{fonte}"
        f"/tabela={tabela}"
        f"/competencia={competencia}"
        f"/part={part:04d}/data.parquet"
    )


def landing_key(dominio: str, fonte: str, competencia: str, arquivo: str) -> str:
    """
    Monta o path para a camada Landing (arquivo bruto, formato original).
    Exemplo: receita_federal/cnpj/competencia=2026-03/Empresas0.zip
    """
    return f"{dominio}/{fonte}/competencia={competencia}/{arquivo}"
