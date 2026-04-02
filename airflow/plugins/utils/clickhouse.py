import os
import logging
import clickhouse_connect
from typing import Any, Optional
import pandas as pd

log = logging.getLogger(__name__)

_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
_USER = os.environ.get("CLICKHOUSE_USER", "default")
_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "changeme")
_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "datastack")

def get_client():
    return clickhouse_connect.get_client(
        host=_HOST,
        port=_PORT,
        username=_USER,
        password=_PASSWORD,
        database=_DATABASE
    )

def execute(query: str, params: Optional[dict[str, Any]] = None):
    client = get_client()
    log.info("Executando query no ClickHouse: %s", query)
    return client.command(query, parameters=params)

def insert_df(table_name: str, df: pd.DataFrame):
    client = get_client()
    log.info("Inserindo %d linhas na tabela %s", len(df), table_name)
    return client.insert_df(table_name, df)
