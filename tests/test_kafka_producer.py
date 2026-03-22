"""
Testes unitários para engines/ingestion/kafka_producer.py

Utiliza mocks para simular o KafkaProducer, permitindo validar
o comportamento das funções sem necessidade do broker Kafka rodando.

Esses testes cobrem:
- Criação do producer com as configurações corretas
- Publicação de mensagens com chave e valor esperados
- Encerramento seguro do producer via flush e close
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Adiciona a raiz do projeto ao path para permitir imports absolutos
sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.ingestion.kafka_producer import create_producer, publish_nfe, flush_and_close


@patch("engines.ingestion.kafka_producer.KafkaProducer")
def test_create_producer_usa_broker_correto(mock_kafka):
    """create_producer deve conectar ao broker informado via bootstrap_servers."""
    create_producer("kafka:29092")
    mock_kafka.assert_called_once()
    args = mock_kafka.call_args
    assert args.kwargs["bootstrap_servers"] == "kafka:29092"


@patch("engines.ingestion.kafka_producer.KafkaProducer")
def test_create_producer_retorna_instancia(mock_kafka):
    """create_producer deve retornar a instância criada pelo KafkaProducer."""
    producer = create_producer()
    assert producer == mock_kafka.return_value


def test_publish_nfe_envia_mensagem():
    """publish_nfe deve chamar producer.send com o tópico e os dados da NF-e."""
    producer = MagicMock()
    nfe_data = {
        "id_nfe": "NFe123",
        "numero_nfe": "001",
        "nome_emitente": "EMPRESA TESTE LTDA",
    }

    publish_nfe(producer, "nfe-raw", nfe_data)

    producer.send.assert_called_once_with(
        "nfe-raw",
        key=b"NFe123",
        value=nfe_data,
    )


def test_publish_nfe_usa_id_como_chave():
    """A chave da mensagem deve ser o id_nfe em bytes para garantir particionamento correto."""
    producer = MagicMock()
    nfe_data = {"id_nfe": "NFe999", "numero_nfe": "999"}

    publish_nfe(producer, "nfe-raw", nfe_data)

    _, kwargs = producer.send.call_args
    assert kwargs["key"] == b"NFe999"


def test_flush_and_close_chama_flush_e_close():
    """flush_and_close deve garantir o envio do buffer e encerrar a conexão."""
    producer = MagicMock()

    flush_and_close(producer)

    producer.flush.assert_called_once()
    producer.close.assert_called_once()


def test_flush_chamado_antes_do_close():
    """O flush deve ocorrer antes do close para evitar perda de mensagens."""
    producer = MagicMock()
    chamadas = []

    producer.flush.side_effect = lambda: chamadas.append("flush")
    producer.close.side_effect = lambda: chamadas.append("close")

    flush_and_close(producer)

    assert chamadas == ["flush", "close"]
