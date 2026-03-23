"""
Script PySpark — Consumo do Kafka e persistência na camada Bronze

Executado pelo Airflow via BashOperator (DAG: process_medallion)
"""

import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from engines.processing.bronze import load_bronze


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToBronze")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    total = load_bronze(spark)
    print(f"[INFO] Camada Bronze concluída — {total} registros gravados no HDFS")

    spark.stop()


if __name__ == "__main__":
    main()
