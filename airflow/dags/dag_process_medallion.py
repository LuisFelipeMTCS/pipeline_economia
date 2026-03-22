"""
DAG 2 — Processamento Medalhão: Kafka → Bronze → Silver → Gold

Orquestra o pipeline de processamento dos dados de NF-e,
implementando a arquitetura medalhão sobre o HDFS.

Fluxo atual:
    kafka_to_bronze
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


SPARK_SUBMIT = (
    "python3 -c \"import shutil; shutil.make_archive('/tmp/engines', 'zip', '/opt/airflow', 'engines')\" && "
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--num-executors 2 "
    "--executor-cores 2 "
    "--executor-memory 512m "
    "--driver-memory 512m "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
    "--py-files /tmp/engines.zip "
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_medallion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Pipeline medalhão: Kafka → Bronze → Silver → Gold",
) as dag:

    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=SPARK_SUBMIT + "/opt/airflow/scripts/kafka_to_bronze.py",
    )

    kafka_to_bronze
