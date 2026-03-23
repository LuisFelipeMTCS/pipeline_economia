"""
Validação de qualidade da camada Silver.

Assertions executadas:
1. Total de registros = 100
2. Zero duplicatas por id_nfe
3. Zero valores monetários inválidos
4. Zero nulos nos campos obrigatórios

Se qualquer assertion falhar, o script encerra com código 1,
fazendo a task do Airflow falhar e interrompendo o pipeline.
"""

from pyspark.sql import SparkSession
import sys

SILVER_PATH = "hdfs://namenode:8020/data/silver/nfe"
TOTAL_ESPERADO = 100


def validar(condicao: bool, mensagem: str):
    if not condicao:
        print(f"[FALHA] {mensagem}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK] {mensagem}")


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateSilver")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)
    total = df.count()

    print(f"\n=== Validação da camada Silver ===")
    print(f"Total de registros encontrados: {total}\n")

    validar(
        total == TOTAL_ESPERADO,
        f"Total de registros = {TOTAL_ESPERADO} (encontrado: {total})"
    )

    total_distinct = df.dropDuplicates(["id_nfe"]).count()
    validar(
        total == total_distinct,
        f"Zero duplicatas por id_nfe (distinct={total_distinct}, total={total})"
    )

    invalidos_valor = df.filter(
        "valor_total_nf <= 0 OR valor_produtos <= 0 OR valor_desconto < 0"
    ).count()
    validar(
        invalidos_valor == 0,
        f"Zero registros com valores monetários inválidos (encontrado: {invalidos_valor})"
    )

    nulos_obrigatorios = df.filter(
        "id_nfe IS NULL OR cnpj_emitente IS NULL OR valor_total_nf IS NULL OR data_emissao IS NULL"
    ).count()
    validar(
        nulos_obrigatorios == 0,
        f"Zero nulos em campos obrigatórios (encontrado: {nulos_obrigatorios})"
    )

    print("\n=== Todas as validações passaram. Silver aprovada. ===\n")
    spark.stop()


if __name__ == "__main__":
    main()
