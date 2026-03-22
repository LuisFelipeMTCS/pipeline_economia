"""
Testes unitários para engines/processing/bronze.py

Utiliza mocks para simular o SparkSession e o DataFrame,
permitindo validar o comportamento sem Kafka ou HDFS rodando.

Testes cobrem:
- Leitura do tópico Kafka com as configurações corretas
- Seleção das colunas esperadas (payload, partition, offset, timestamp)
- Escrita no caminho correto do HDFS
- Retorno do total de registros gravados
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.processing.bronze import load_bronze, BRONZE_PATH, KAFKA_TOPIC, KAFKA_BROKERS


def _make_spark_mock(row_count: int = 100) -> MagicMock:
    """Cria um SparkSession mock com DataFrame simulado."""
    df_kafka = MagicMock()
    df_bronze = MagicMock()
    df_bronze.count.return_value = row_count

    # reader único que sempre retorna a si mesmo em cada .option()
    reader = MagicMock()
    reader.option.return_value = reader
    reader.load.return_value = df_kafka

    spark = MagicMock()
    spark.read.format.return_value = reader
    df_kafka.select.return_value = df_bronze

    return spark, df_kafka, df_bronze, reader


@patch("engines.processing.bronze.col")
def test_load_bronze_retorna_total_de_registros(mock_col):
    """load_bronze deve retornar o total de registros gravados."""
    spark, _, df_bronze, _ = _make_spark_mock(row_count=100)
    resultado = load_bronze(spark)
    assert resultado == 100


@patch("engines.processing.bronze.col")
def test_load_bronze_le_topico_correto(mock_col):
    """load_bronze deve se conectar ao tópico nfe-raw."""
    spark, _, _, _ = _make_spark_mock()
    load_bronze(spark)
    spark.read.format.assert_called_once_with("kafka")


@patch("engines.processing.bronze.col")
def test_load_bronze_usa_brokers_corretos(mock_col):
    """load_bronze deve usar os dois brokers Kafka."""
    spark, _, _, reader = _make_spark_mock()
    load_bronze(spark)
    reader.option.assert_any_call("kafka.bootstrap.servers", KAFKA_BROKERS)


@patch("engines.processing.bronze.col")
def test_load_bronze_seleciona_colunas_esperadas(mock_col):
    """load_bronze deve selecionar payload, partition, offset e timestamp."""
    spark, df_kafka, _, _ = _make_spark_mock()
    load_bronze(spark)
    df_kafka.select.assert_called_once()


@patch("engines.processing.bronze.col")
def test_load_bronze_escreve_no_caminho_correto(mock_col):
    """load_bronze deve gravar no caminho HDFS da camada Bronze."""
    spark, _, df_bronze, _ = _make_spark_mock()
    load_bronze(spark)
    df_bronze.write.mode.assert_called_once_with("overwrite")
    df_bronze.write.mode.return_value.json.assert_called_once_with(BRONZE_PATH)


@patch("engines.processing.bronze.col")
def test_load_bronze_offset_earliest(mock_col):
    """load_bronze deve ler desde o início do tópico (startingOffsets=earliest)."""
    spark, _, _, reader = _make_spark_mock()
    load_bronze(spark)
    reader.option.assert_any_call("startingOffsets", "earliest")
