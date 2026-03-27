from datetime import datetime, timezone
from airflow.sdk import dag, task

@dag(
    dag_id="hello_world",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["debug"],
)
def hello_world():

    @task
    def hello() -> str:
        import logging
        log = logging.getLogger(__name__)
        log.info("Hello, World!")
        return "ok"

    hello()

hello_world()
