"""
Módulo responsável pela publicação de mensagens no Apache Kafka.

Encapsula a criação do producer e o envio de dados de NF-e
para o tópico de ingestão, serializando os dados em JSON.
"""

import json
from kafka import KafkaProducer


def create_producer(bootstrap_servers: str = "kafka:29092") -> KafkaProducer:
    """
    Cria e retorna uma instância do KafkaProducer.

    Args:
        bootstrap_servers: endereço do broker Kafka (padrão: listener interno do container)

    Returns:
        KafkaProducer configurado com serialização JSON em UTF-8
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_nfe(producer: KafkaProducer, topic: str, nfe_data: dict) -> None:
    """
    Publica uma NF-e no tópico Kafka especificado.

    A chave da mensagem é o ID único da NF-e, garantindo que
    notas duplicadas sejam direcionadas à mesma partição.

    Args:
        producer: instância ativa do KafkaProducer
        topic: nome do tópico de destino
        nfe_data: dicionário com os dados da NF-e
    """
    key = nfe_data.get("id_nfe", "").encode("utf-8")
    producer.send(topic, key=key, value=nfe_data)


def flush_and_close(producer: KafkaProducer) -> None:
    """
    Garante o envio de todas as mensagens pendentes e encerra o producer.

    Deve ser chamado ao final do processo para evitar perda de mensagens
    que ainda estejam no buffer interno do producer.

    Args:
        producer: instância ativa do KafkaProducer
    """
    producer.flush()
    producer.close()
