"""
Camada Bronze — Ingestão bruta do Kafka para o HDFS

Responsável por:
1. Consumir mensagens do tópico Kafka 'nfe-raw'
2. Salvar os dados brutos em formato JSON no HDFS
   Caminho: /data/bronze/nfe/

Sem transformações — os dados chegam exatamente como foram publicados.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BROKERS = "kafka:29092,kafka-2:29093"
KAFKA_TOPIC   = "nfe-raw"
BRONZE_PATH   = "hdfs://namenode:8020/data/bronze/nfe"


def load_bronze(spark: SparkSession) -> int:
    """
    Consome todas as mensagens do tópico Kafka e salva em JSON no HDFS.

    Returns:
        Total de registros gravados na camada Bronze
    """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_bronze = df.select(
        col("value").cast("string").alias("payload"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    df_bronze.write.mode("overwrite").json(BRONZE_PATH)

    total = df_bronze.count()
    print(f"[BRONZE] {total} registros gravados em {BRONZE_PATH}")
    return total
