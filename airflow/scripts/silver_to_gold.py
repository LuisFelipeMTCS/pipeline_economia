"""
Script PySpark — Transformação Silver → Gold

Executado pelo Airflow via BashOperator (DAG: process_medallion)
"""

import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import SparkSession
from engines.processing.gold import load_gold


def main():
    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    totais = load_gold(spark)
    for tabela, total in totais.items():
        print(f"[INFO] gold.{tabela} — {total} registros gravados no HDFS")

    spark.stop()


if __name__ == "__main__":
    main()
