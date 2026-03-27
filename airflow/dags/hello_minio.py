from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="hello_minio",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["debug"],
)
def hello_minio():

    @task
    def gerar_e_salvar_csv() -> str:
        import csv
        import io
        from utils.storage import _get_client

        conteudo = [
            {"nome": "Alice", "idade": 30},
            {"nome": "Bob",   "idade": 25},
            {"nome": "Carol", "idade": 35},
        ]

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=["nome", "idade"])
        writer.writeheader()
        writer.writerows(conteudo)

        client = _get_client()
        key = "debug/hello_minio.csv"
        client.put_object(
            Bucket="bronze",
            Key=key,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

        path = f"s3://bronze/{key}"
        print(f"CSV salvo em: {path}")
        return path

    gerar_e_salvar_csv()


hello_minio()
