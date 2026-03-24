"""
Camada Gold — Modelagem Star Schema (Kimball)

Lê da camada Silver (Parquet) e gera:

  Dimensões:
    dim_emitente    — dados cadastrais dos emitentes (cnpj, nome, uf, municipio)
    dim_data        — calendário derivado da data de emissão
    dim_localidade  — UF com região e nome completo do estado

  Fato:
    fato_vendas     — métricas financeiras com FKs para as dimensões

Diagrama:
    dim_data ─────┐
    dim_emitente ─┼── fato_vendas
    dim_localidade┘
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, quarter,
    concat_ws, lpad, when,
    dense_rank, monotonically_increasing_id,
    round as spark_round
)
from pyspark.sql.window import Window

SILVER_PATH = "hdfs://namenode:8020/data/silver/nfe"
GOLD_PATH   = "hdfs://namenode:8020/data/gold"

# Mapeamento UF → Região
REGIOES = {
    "AM": "Norte",    "RR": "Norte",    "AP": "Norte",
    "PA": "Norte",    "TO": "Norte",    "RO": "Norte",    "AC": "Norte",
    "MA": "Nordeste", "PI": "Nordeste", "CE": "Nordeste", "RN": "Nordeste",
    "PE": "Nordeste", "PB": "Nordeste", "SE": "Nordeste", "AL": "Nordeste", "BA": "Nordeste",
    "MT": "Centro-Oeste", "MS": "Centro-Oeste", "GO": "Centro-Oeste", "DF": "Centro-Oeste",
    "SP": "Sudeste",  "RJ": "Sudeste",  "MG": "Sudeste",  "ES": "Sudeste",
    "PR": "Sul",      "SC": "Sul",      "RS": "Sul",
}

# Mapeamento UF → Nome completo do estado
ESTADOS = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
    "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
    "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins",
}


def _map_expr(col_name, mapping, default="Não identificado"):
    """Gera expressão Spark para mapeamento de dicionário."""
    expr = None
    for key, value in mapping.items():
        cond = when(col(col_name) == key, value)
        expr = cond if expr is None else expr.when(col(col_name) == key, value)
    return expr.otherwise(default)


def _dim_emitente(df):
    """Dimensão com dados cadastrais únicos por emitente."""
    window = Window.orderBy("cnpj_emitente")
    return (
        df.select("cnpj_emitente", "nome_emitente", "uf_emitente", "municipio_emitente")
          .dropDuplicates(["cnpj_emitente"])
          .withColumn("id_emitente", dense_rank().over(window))
          .select(
              col("id_emitente"),
              col("cnpj_emitente").alias("cnpj"),
              col("nome_emitente").alias("nome"),
              col("uf_emitente").alias("uf"),
              col("municipio_emitente").alias("municipio"),
          )
    )


def _dim_data(df):
    """Dimensão calendário derivada das datas de emissão."""
    window = Window.orderBy("ts")
    return (
        df.select(col("data_emissao").cast("timestamp").alias("ts"))
          .dropDuplicates(["ts"])
          .withColumn("data",       col("ts").cast("date").cast("string"))
          .withColumn("ano",        year("ts"))
          .withColumn("mes",        month("ts"))
          .withColumn("dia",        dayofmonth("ts"))
          .withColumn("trimestre",  quarter("ts"))
          .withColumn("id_data",    dense_rank().over(window))
          .select("id_data", "data", "dia", "mes", "ano", "trimestre")
    )


def _dim_localidade(df):
    """Dimensão geográfica com UF, região e nome completo do estado."""
    window = Window.orderBy("uf_emitente")
    return (
        df.select("uf_emitente")
          .dropDuplicates(["uf_emitente"])
          .withColumn("regiao",          _map_expr("uf_emitente", REGIOES))
          .withColumn("estado_completo", _map_expr("uf_emitente", ESTADOS))
          .withColumn("id_localidade",   dense_rank().over(window))
          .select(
              col("id_localidade"),
              col("uf_emitente").alias("uf"),
              col("regiao"),
              col("estado_completo"),
          )
    )


def _fato_vendas(df, dim_emitente, dim_data, dim_localidade):
    """Tabela fato com métricas financeiras e chaves para as dimensões."""
    dim_e = dim_emitente.select("id_emitente", col("cnpj").alias("cnpj_emitente"))
    dim_l = dim_localidade.select("id_localidade", col("uf").alias("uf_emitente"))
    dim_d = dim_data.select("id_data", col("data").alias("data_ref"))

    return (
        df.withColumn("ts",       col("data_emissao").cast("timestamp"))
          .withColumn("data_ref", col("ts").cast("date").cast("string"))
          .join(dim_e, on="cnpj_emitente", how="left")
          .join(dim_l, on="uf_emitente",   how="left")
          .join(dim_d, on="data_ref",       how="left")
          .withColumn("id_venda", monotonically_increasing_id())
          .select(
              col("id_venda"),
              col("id_nfe"),
              col("id_emitente"),
              col("id_data"),
              col("id_localidade"),
              spark_round(col("valor_total_nf"), 2).alias("valor_total"),
              spark_round(col("valor_produtos"),  2).alias("valor_produtos"),
              spark_round(col("valor_desconto"),  2).alias("valor_desconto"),
          )
    )


def load_gold(spark: SparkSession) -> dict:
    """
    Lê a Silver, constrói o star schema e salva as tabelas na Gold.

    Returns:
        Dict com total de registros de cada tabela gerada.
    """
    df = spark.read.parquet(SILVER_PATH)
    print(f"[GOLD] Registros lidos da Silver: {df.count()}")

    dim_e = _dim_emitente(df)
    dim_d = _dim_data(df)
    dim_l = _dim_localidade(df)
    fato  = _fato_vendas(df, dim_e, dim_d, dim_l)

    tabelas = {
        "dim_emitente":   dim_e,
        "dim_data":       dim_d,
        "dim_localidade": dim_l,
        "fato_vendas":    fato,
    }

    totais = {}
    for nome, df_gold in tabelas.items():
        path = f"{GOLD_PATH}/{nome}"
        df_gold.write.mode("overwrite").parquet(path)
        total = df_gold.count()
        totais[nome] = total
        print(f"[GOLD] {nome}: {total} registros gravados em {path}")

    return totais
