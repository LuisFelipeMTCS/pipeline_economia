"""
DAG 2 — Processamento Medalhão: Kafka → Bronze → Silver → Gold

Orquestra o pipeline de processamento dos dados de NF-e,
implementando a arquitetura medalhão sobre o HDFS.

Fluxo:
    kafka_to_bronze >> bronze_to_silver >> validate_silver >> silver_to_gold
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

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=SPARK_SUBMIT + "/opt/airflow/scripts/bronze_to_silver.py",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=SPARK_SUBMIT + "/opt/airflow/scripts/validate_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_SUBMIT + "/opt/airflow/scripts/silver_to_gold.py",
    )

    kafka_to_bronze >> bronze_to_silver >> validate_silver >> silver_to_gold
