"""
Testes unitários para engines/processing/silver.py

Testes cobrem:
- Leitura do caminho Bronze correto
- Extração dos campos do payload JSON
- Remoção de campos obrigatórios nulos
- Deduplicação por id_nfe
- Filtro de valores monetários inválidos
- Escrita no caminho Silver em formato Parquet
- Retorno do total de registros gravados
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.processing.silver import (
    load_silver, BRONZE_PATH, SILVER_PATH, CAMPOS_OBRIGATORIOS
)


def _make_spark_mock(row_count: int = 100) -> tuple:
    """Cria SparkSession mock com pipeline de DataFrame simulado."""
    df_bronze   = MagicMock()
    df_extraido = MagicMock()
    df_valido   = MagicMock()
    df_unico    = MagicMock()
    df_valores  = MagicMock()
    df_final    = MagicMock()
    df_final.count.return_value = row_count

    # pipeline: select → filter → dropDuplicates → filter → withColumn (x4)
    df_bronze.count.return_value = row_count + 50
    df_extraido.count.return_value = row_count + 20
    df_valido.count.return_value = row_count + 10
    df_unico.count.return_value = row_count + 5
    df_valores.count.return_value = row_count

    df_bronze.select.return_value = df_extraido
    df_extraido.filter.return_value = df_valido
    df_valido.dropDuplicates.return_value = df_unico
    df_unico.filter.return_value = df_valores

    # normalizar strings: withColumn encadeado
    df_valores.withColumn.return_value = df_valores
    df_valores.count.return_value = row_count
    df_valores.write = df_final.write

    # ajusta final para write
    df_final.count.return_value = row_count
    df_valores.count.return_value = row_count

    spark = MagicMock()
    spark.read.json.return_value = df_bronze

    return spark, df_bronze, df_extraido, df_valido, df_unico, df_valores


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_le_caminho_bronze(mock_trim, mock_col, mock_gjson):
    spark, *_ = _make_spark_mock()
    load_silver(spark)
    spark.read.json.assert_called_once_with(BRONZE_PATH)


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_extrai_campos_do_payload(mock_trim, mock_col, mock_gjson):
    spark, df_bronze, *_ = _make_spark_mock()
    load_silver(spark)
    df_bronze.select.assert_called_once()


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_valida_campos_obrigatorios(mock_trim, mock_col, mock_gjson):
    spark, _, df_extraido, *_ = _make_spark_mock()
    load_silver(spark)
    df_extraido.filter.assert_called_once()


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_deduplica_por_id_nfe(mock_trim, mock_col, mock_gjson):
    spark, _, _, df_valido, *_ = _make_spark_mock()
    load_silver(spark)
    df_valido.dropDuplicates.assert_called_once_with(["id_nfe"])


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_filtra_valores_invalidos(mock_trim, mock_col, mock_gjson):
    spark, _, _, _, df_unico, *_ = _make_spark_mock()
    load_silver(spark)
    df_unico.filter.assert_called_once()


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_escreve_parquet_na_silver(mock_trim, mock_col, mock_gjson):
    spark, _, _, _, _, df_valores = _make_spark_mock()
    load_silver(spark)
    df_valores.write.mode.assert_called_with("overwrite")
    df_valores.write.mode.return_value.parquet.assert_called_with(SILVER_PATH)


@patch("engines.processing.silver.get_json_object")
@patch("engines.processing.silver.col")
@patch("engines.processing.silver.trim")
def test_campos_obrigatorios_definidos(mock_trim, mock_col, mock_gjson):
    assert "id_nfe" in CAMPOS_OBRIGATORIOS
    assert "cnpj_emitente" in CAMPOS_OBRIGATORIOS
    assert "valor_total_nf" in CAMPOS_OBRIGATORIOS
    assert "data_emissao" in CAMPOS_OBRIGATORIOS
