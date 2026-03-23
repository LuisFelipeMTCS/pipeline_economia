"""
Script PySpark — Transformação Bronze → Silver

Executado pelo Airflow via BashOperator (DAG: process_medallion)
"""

import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from engines.processing.silver import load_silver


def main():
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    total = load_silver(spark)
    print(f"[INFO] Camada Silver concluída — {total} registros gravados no HDFS")

    spark.stop()


if __name__ == "__main__":
    main()
