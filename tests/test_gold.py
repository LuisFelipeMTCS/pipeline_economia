"""
Testes unitários para engines/processing/gold.py

Testes cobrem:
- Construção da dim_emitente com campos corretos
- Construção da dim_data com campos de calendário
- Construção da dim_localidade com região e estado completo
- Construção da fato_vendas com FKs e métricas
- Escrita das 4 tabelas no HDFS em formato Parquet
- Retorno do dict de totais por tabela
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.processing.gold import (
    load_gold, SILVER_PATH, GOLD_PATH,
    REGIOES, ESTADOS,
)


def _make_df_mock(count=100):
    """Cria um DataFrame mock que suporta o pipeline Gold."""
    df = MagicMock()
    df.count.return_value = count

    # encadeia select/dropDuplicates/withColumn/join
    df.select.return_value = df
    df.dropDuplicates.return_value = df
    df.withColumn.return_value = df
    df.join.return_value = df
    df.write.mode.return_value.parquet = MagicMock()

    return df


def _make_spark_mock(count=100):
    spark = MagicMock()
    df = _make_df_mock(count)
    spark.read.parquet.return_value = df
    return spark, df


# Decorator que mocka todas as dependências PySpark que exigem SparkContext
def _patch_spark(func):
    for decorator in [
        patch("engines.processing.gold.Window"),
        patch("engines.processing.gold.dense_rank"),
        patch("engines.processing.gold.monotonically_increasing_id"),
        patch("engines.processing.gold.col"),
        patch("engines.processing.gold.year"),
        patch("engines.processing.gold.month"),
        patch("engines.processing.gold.dayofmonth"),
        patch("engines.processing.gold.quarter"),
        patch("engines.processing.gold.concat_ws"),
        patch("engines.processing.gold.lpad"),
        patch("engines.processing.gold.when"),
        patch("engines.processing.gold.spark_round"),
    ]:
        func = decorator(func)
    return func


# ---------------------------------------------------------------------------
# dim_emitente
# ---------------------------------------------------------------------------

@_patch_spark
def test_dim_emitente_le_campos_corretos(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    calls_args = [str(c) for c in df.select.call_args_list]
    assert any("cnpj_emitente" in a for a in calls_args)


@_patch_spark
def test_dim_emitente_deduplica_por_cnpj(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    args_list = [c.args[0] for c in df.dropDuplicates.call_args_list]
    assert ["cnpj_emitente"] in args_list


# ---------------------------------------------------------------------------
# dim_data
# ---------------------------------------------------------------------------

@_patch_spark
def test_dim_data_deduplica_por_ts(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    args_list = [c.args[0] for c in df.dropDuplicates.call_args_list]
    assert ["ts"] in args_list


@_patch_spark
def test_dim_data_gera_campos_calendario(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    campos_chamados = [c.args[0] for c in df.withColumn.call_args_list]
    for campo in ["data", "ano", "mes", "dia", "trimestre", "id_data"]:
        assert campo in campos_chamados, f"Campo '{campo}' não foi gerado na dim_data"


# ---------------------------------------------------------------------------
# dim_localidade
# ---------------------------------------------------------------------------

@_patch_spark
def test_dim_localidade_deduplica_por_uf(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    args_list = [c.args[0] for c in df.dropDuplicates.call_args_list]
    assert ["uf_emitente"] in args_list


@_patch_spark
def test_dim_localidade_gera_regiao_e_estado(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    campos_chamados = [c.args[0] for c in df.withColumn.call_args_list]
    assert "regiao" in campos_chamados
    assert "estado_completo" in campos_chamados


# ---------------------------------------------------------------------------
# fato_vendas
# ---------------------------------------------------------------------------

@_patch_spark
def test_fato_vendas_gera_id_venda(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    campos_chamados = [c.args[0] for c in df.withColumn.call_args_list]
    assert "id_venda" in campos_chamados


@_patch_spark
def test_fato_vendas_faz_joins_com_dimensoes(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    assert df.join.call_count >= 3


# ---------------------------------------------------------------------------
# load_gold — escrita e retorno
# ---------------------------------------------------------------------------

@_patch_spark
def test_load_gold_escreve_4_tabelas(*_):
    spark, df = _make_spark_mock()
    load_gold(spark)
    assert df.write.mode.call_count == 4
    paths_escritos = [c.args[0] for c in df.write.mode.return_value.parquet.call_args_list]
    assert any("dim_emitente"   in p for p in paths_escritos)
    assert any("dim_data"       in p for p in paths_escritos)
    assert any("dim_localidade" in p for p in paths_escritos)
    assert any("fato_vendas"    in p for p in paths_escritos)


@_patch_spark
def test_load_gold_retorna_dict_com_totais(*_):
    spark, df = _make_spark_mock(count=100)
    totais = load_gold(spark)
    assert isinstance(totais, dict)
    assert set(totais.keys()) == {"dim_emitente", "dim_data", "dim_localidade", "fato_vendas"}


# ---------------------------------------------------------------------------
# Mapeamentos
# ---------------------------------------------------------------------------

def test_regioes_cobre_todos_estados_brasileiros():
    estados_br = {
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
        "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
        "RS","RO","RR","SC","SP","SE","TO"
    }
    assert estados_br == set(REGIOES.keys())


def test_estados_cobre_todos_estados_brasileiros():
    assert len(ESTADOS) == 27
    assert ESTADOS["SP"] == "São Paulo"
    assert ESTADOS["RJ"] == "Rio de Janeiro"
