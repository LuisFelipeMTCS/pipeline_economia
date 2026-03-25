"""
Camada Bronze — Ingestão bruta do Kafka para o HDFS

Responsável por:
1. Consumir mensagens do tópico Kafka 'nfe-raw'
2. Salvar os dados brutos em formato JSON no HDFS
   Caminho: /data/bronze/nfe/

Sem transformações — os dados chegam exatamente como foram publicados.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

log = logging.getLogger(__name__)

KAFKA_BROKERS = "kafka:29092,kafka-2:29093"
KAFKA_TOPIC   = "nfe-raw"
BRONZE_PATH   = "hdfs://namenode:8020/data/bronze/nfe"


def load_bronze(spark: SparkSession) -> int:
    """
    Consome todas as mensagens do tópico Kafka e salva em JSON no HDFS.

    Returns:
        Total de registros gravados na camada Bronze
    """
    try:
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

        # Cache antes de escrever — evita segunda leitura do Kafka no count()
        df_bronze.cache()

        try:
            df_bronze.write.mode("overwrite").json(BRONZE_PATH)
            total = df_bronze.count()
            log.info("[BRONZE] %d registros gravados em %s", total, BRONZE_PATH)
            return total
        finally:
            df_bronze.unpersist()

    except Exception as e:
        log.error("[BRONZE] Falha na ingestão do Kafka: %s", str(e))
        raise
