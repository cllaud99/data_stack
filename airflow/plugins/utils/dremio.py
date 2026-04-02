import os
import logging
import requests
import pyarrow.flight as flight
import pandas as pd

log = logging.getLogger(__name__)

_URL = os.environ.get("DREMIO_HOST", "http://dremio:9047")
_HOST = _URL.split("://")[-1].split(":")[0]
_FLIGHT_PORT = int(os.environ.get("DREMIO_FLIGHT_PORT", "32010"))
_USER = os.environ.get("DREMIO_ADMIN_USER", "admin")
_PASSWORD = os.environ.get("DREMIO_ADMIN_PASSWORD", "changeme")


def _get_token() -> str:
    """Autentica via REST e retorna o token Bearer."""
    resp = requests.post(
        f"{_URL}/apiv2/login",
        json={"userName": _USER, "password": _PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def query_to_df(query: str) -> pd.DataFrame:
    """Executa SQL no Dremio via Arrow Flight e retorna DataFrame Pandas."""
    token = _get_token()
    location = flight.Location.for_grpc_tcp(_HOST, _FLIGHT_PORT)
    client = flight.FlightClient(location)

    options = flight.FlightCallOptions(
        headers=[(b"authorization", f"Bearer {token}".encode())]
    )
    descriptor = flight.FlightDescriptor.for_command(query.encode())

    log.info("Executando query no Dremio (Flight): %s", query)
    info = client.get_flight_info(descriptor, options)
    reader = client.do_get(info.endpoints[0].ticket, options)
    return reader.read_all().to_pandas()
