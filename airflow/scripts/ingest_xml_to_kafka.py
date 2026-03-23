"""
Script PySpark — Ingestão de NF-e XML para o Kafka

Responsável por:
1. Ler todos os arquivos XML da pasta de entrada
2. Parsear cada NF-e utilizando a camada engines/ingestion
3. Criar um DataFrame Spark com os dados estruturados
4. Publicar cada NF-e no tópico Kafka 'nfe-raw'

Executado pelo Airflow via BashOperator (DAG: ingest_xml_streaming)
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Permite importar a camada engines/ a partir do container Airflow
sys.path.insert(0, "/opt/airflow")

from engines.ingestion.xml_reader import parse_nfe
from engines.ingestion.kafka_producer import create_producer, publish_nfe, flush_and_close

XMLS_DIR = "/opt/airflow/xmls"
KAFKA_TOPIC = "nfe-raw"
KAFKA_BROKER = "kafka:29092,kafka-2:29093"


def main():
    spark = (
        SparkSession.builder
        .appName("IngestXMLToKafka")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Coleta todos os arquivos XML disponíveis
    xml_files = sorted(Path(XMLS_DIR).glob("*.xml"))
    print(f"[INFO] {len(xml_files)} arquivos XML encontrados em {XMLS_DIR}")

    # Parseia cada XML utilizando a camada engines
    parsed = []
    erros = []
    for xml_path in xml_files:
        try:
            nfe = parse_nfe(str(xml_path))
            parsed.append(nfe)
        except Exception as e:
            erros.append((xml_path.name, str(e)))
            print(f"[WARN] Erro ao parsear {xml_path.name}: {e}")

    print(f"[INFO] {len(parsed)} NF-es parseadas | {len(erros)} erros")

    # Schema explícito para evitar erro de inferência com campos None
    schema = StructType([
        StructField("id_nfe", StringType(), True),
        StructField("numero_nfe", StringType(), True),
        StructField("serie", StringType(), True),
        StructField("data_emissao", StringType(), True),
        StructField("natureza_operacao", StringType(), True),
        StructField("cnpj_emitente", StringType(), True),
        StructField("nome_emitente", StringType(), True),
        StructField("uf_emitente", StringType(), True),
        StructField("municipio_emitente", StringType(), True),
        StructField("cpf_destinatario", StringType(), True),
        StructField("cnpj_destinatario", StringType(), True),
        StructField("nome_destinatario", StringType(), True),
        StructField("valor_produtos", StringType(), True),
        StructField("valor_desconto", StringType(), True),
        StructField("valor_total_nf", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
    ])

    rows = [{k: v for k, v in nfe.items() if k != "itens"} for nfe in parsed]
    df = spark.createDataFrame(rows, schema=schema)
    df.printSchema()
    print(f"[INFO] Total de registros no DataFrame: {df.count()}")
    df.show(5, truncate=False)

    # Publica cada NF-e no tópico Kafka
    producer = create_producer(KAFKA_BROKER)
    for nfe in parsed:
        publish_nfe(producer, KAFKA_TOPIC, nfe)
    flush_and_close(producer)

    print(f"[INFO] {len(parsed)} mensagens publicadas no tópico '{KAFKA_TOPIC}'")
    spark.stop()


if __name__ == "__main__":
    main()
