"""
DAG 1 — Ingestão de NF-e XML para o Kafka

Orquestra a execução do script PySpark que lê os arquivos XML
e publica as notas fiscais no tópico Kafka 'nfe-raw'.

Fluxo:
    xml_to_kafka → validar_kafka
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def validar_mensagens_kafka():
    """
    Valida se as mensagens chegaram no tópico 'nfe-raw' do Kafka.
    Falha a task se nenhuma mensagem for encontrada.
    """
    from kafka import KafkaConsumer
    import json

    consumer = KafkaConsumer(
        "nfe-raw",
        bootstrap_servers="kafka:29092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    mensagens = [msg for msg in consumer]
    consumer.close()

    total = len(mensagens)
    print(f"[INFO] Total de mensagens encontradas no tópico 'nfe-raw': {total}")

    if total == 0:
        raise ValueError("Nenhuma mensagem encontrada no tópico 'nfe-raw'. Verifique o job de ingestão.")

    print(f"[INFO] Validação concluída — {total} NF-es publicadas com sucesso.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_xml_streaming",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
    description="Lê XMLs de NF-e, publica no Kafka e valida o envio",
) as dag:

    xml_to_kafka = BashOperator(
        task_id="xml_to_kafka",
        bash_command=(
            "python3 -c \"import shutil; shutil.make_archive('/tmp/engines', 'zip', '/opt/airflow', 'engines')\" && "
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--num-executors 2 "
            "--executor-cores 2 "
            "--executor-memory 512m "
            "--driver-memory 512m "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "--py-files /tmp/engines.zip "
            "/opt/airflow/scripts/ingest_xml_to_kafka.py"
        ),
    )

    validar_kafka = PythonOperator(
        task_id="validar_kafka",
        python_callable=validar_mensagens_kafka,
    )

    xml_to_kafka >> validar_kafka
