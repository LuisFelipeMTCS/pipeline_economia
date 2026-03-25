"""
Camada Silver — Limpeza, validação e estruturação dos dados Bronze

Responsável por:
1. Ler os dados brutos da camada Bronze (JSON no HDFS)
2. Extrair os campos do payload JSON para colunas tipadas
3. Validar e limpar os dados:
   - Remover registros com campos obrigatórios nulos
   - Remover duplicatas por id_nfe (chave única da NF-e)
   - Garantir tipos corretos (Double para valores monetários)
   - Filtrar valores monetários inválidos
4. Salvar em formato Parquet no HDFS com overwrite
   Caminho: /data/silver/nfe/

Garantias da Silver:
- Exatamente 1 registro por id_nfe
- Nenhum valor obrigatório nulo
- Tipos de dados consistentes
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, trim
from pyspark.sql.types import DoubleType

log = logging.getLogger(__name__)

BRONZE_PATH = "hdfs://namenode:8020/data/bronze/nfe"
SILVER_PATH = "hdfs://namenode:8020/data/silver/nfe"

CAMPOS_OBRIGATORIOS = ["id_nfe", "cnpj_emitente", "valor_total_nf", "data_emissao"]


def _extrair_campos(df_bronze):
    """Extrai campos do payload JSON para colunas tipadas."""
    return df_bronze.select(
        get_json_object(col("payload"), "$.id_nfe").alias("id_nfe"),
        get_json_object(col("payload"), "$.numero_nfe").alias("numero_nfe"),
        get_json_object(col("payload"), "$.serie").alias("serie"),
        get_json_object(col("payload"), "$.data_emissao").alias("data_emissao"),
        get_json_object(col("payload"), "$.natureza_operacao").alias("natureza_operacao"),
        get_json_object(col("payload"), "$.cnpj_emitente").alias("cnpj_emitente"),
        get_json_object(col("payload"), "$.nome_emitente").alias("nome_emitente"),
        get_json_object(col("payload"), "$.uf_emitente").alias("uf_emitente"),
        get_json_object(col("payload"), "$.municipio_emitente").alias("municipio_emitente"),
        get_json_object(col("payload"), "$.nome_destinatario").alias("nome_destinatario"),
        get_json_object(col("payload"), "$.valor_produtos")
            .cast(DoubleType()).alias("valor_produtos"),
        get_json_object(col("payload"), "$.valor_desconto")
            .cast(DoubleType()).alias("valor_desconto"),
        get_json_object(col("payload"), "$.valor_total_nf")
            .cast(DoubleType()).alias("valor_total_nf"),
        get_json_object(col("payload"), "$.arquivo_origem").alias("arquivo_origem"),
    )


def _validar_obrigatorios(df):
    """Remove registros com campos obrigatórios nulos."""
    condicao = None
    for campo in CAMPOS_OBRIGATORIOS:
        filtro = col(campo).isNotNull()
        condicao = filtro if condicao is None else condicao & filtro
    return df.filter(condicao)


def _deduplicar(df):
    """
    Remove duplicatas por id_nfe.
    Garante exatamente 1 registro por NF-e independente de quantas
    vezes o pipeline foi executado (idempotência contra at-least-once do Kafka).
    """
    return df.dropDuplicates(["id_nfe"])


def _validar_valores(df):
    """Remove registros com valores monetários inválidos ou nulos pós-cast."""
    return df.filter(
        "valor_total_nf > 0 AND valor_produtos > 0 AND valor_desconto >= 0"
    )


def _normalizar_strings(df):
    """Remove espaços extras de campos de texto."""
    campos_texto = ["nome_emitente", "municipio_emitente", "natureza_operacao", "nome_destinatario"]
    for campo in campos_texto:
        df = df.withColumn(campo, trim(col(campo)))
    return df


def load_silver(spark: SparkSession) -> int:
    """
    Lê a camada Bronze, aplica validações e salva como Parquet na Silver.

    Returns:
        Total de registros únicos e válidos gravados na camada Silver
    """
    df_bronze = None
    df = None

    try:
        df_bronze = spark.read.json(BRONZE_PATH)
        df_bronze.cache()
        total_bronze = df_bronze.count()
        log.info("[SILVER] Registros lidos do Bronze: %d", total_bronze)

        df = _extrair_campos(df_bronze)
        df = _validar_obrigatorios(df)
        df = _deduplicar(df)
        df = _validar_valores(df)
        df = _normalizar_strings(df)

        df.cache()
        total = df.count()

        # Alerta se houve perda significativa de registros por cast inválido
        if total < total_bronze:
            log.warning(
                "[SILVER] %d registros descartados (nulos, duplicatas ou cast inválido)",
                total_bronze - total,
            )

        df.write.mode("overwrite").parquet(SILVER_PATH)
        log.info("[SILVER] %d registros gravados em %s", total, SILVER_PATH)
        return total

    except Exception as e:
        log.error("[SILVER] Falha no processamento: %s", str(e))
        raise

    finally:
        if df_bronze is not None:
            df_bronze.unpersist()
        if df is not None:
            df.unpersist()
