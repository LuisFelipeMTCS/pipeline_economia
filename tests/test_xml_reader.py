"""
Testes unitários para engines/ingestion/xml_reader.py

Valida que a função parse_nfe() extrai corretamente os dados
de arquivos XML no padrão NF-e (Nota Fiscal Eletrônica).

Esses testes cobrem:
- Estrutura do retorno (tipo e campos esperados)
- Integridade dos dados (campos críticos não nulos)
- Parsing dos itens da nota
- Rastreabilidade do arquivo de origem
- Comportamento em caso de arquivo inválido
"""

import pytest
import sys
from pathlib import Path

# Adiciona a raiz do projeto ao path para permitir imports absolutos
sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.ingestion.xml_reader import parse_nfe

# Arquivo XML utilizado como base para os testes
XML_PATH = "xmls/nfe_001.xml"

# Campos que toda NF-e parseada deve conter
CAMPOS_OBRIGATORIOS = [
    "id_nfe",
    "numero_nfe",
    "serie",
    "data_emissao",
    "natureza_operacao",
    "cnpj_emitente",
    "nome_emitente",
    "uf_emitente",
    "valor_total_nf",
    "itens",
    "arquivo_origem",
]


@pytest.fixture
def nfe():
    """Fixture que parseia o XML de teste e disponibiliza o resultado para os testes."""
    return parse_nfe(XML_PATH)


def test_retorna_dict(nfe):
    """parse_nfe deve retornar um dicionário Python."""
    assert isinstance(nfe, dict)


def test_campos_obrigatorios_presentes(nfe):
    """Todos os campos obrigatórios do schema devem estar presentes no retorno."""
    for campo in CAMPOS_OBRIGATORIOS:
        assert campo in nfe, f"Campo ausente: {campo}"


def test_campos_nao_vazios(nfe):
    """Campos críticos de identificação e valor não podem ser nulos."""
    campos_esperados = ["id_nfe", "numero_nfe", "cnpj_emitente", "nome_emitente", "valor_total_nf"]
    for campo in campos_esperados:
        assert nfe[campo] is not None, f"Campo vazio: {campo}"


def test_itens_e_lista(nfe):
    """O campo 'itens' deve ser uma lista para suportar múltiplos produtos por nota."""
    assert isinstance(nfe["itens"], list)


def test_itens_nao_vazios(nfe):
    """Uma NF-e válida deve conter ao menos um item (produto)."""
    assert len(nfe["itens"]) > 0


def test_item_tem_campos_esperados(nfe):
    """Cada item da nota deve conter os campos mínimos para análise."""
    item = nfe["itens"][0]
    for campo in ["descricao", "quantidade", "valor_total_item"]:
        assert campo in item, f"Campo ausente no item: {campo}"


def test_arquivo_origem_correto(nfe):
    """O nome do arquivo de origem deve ser registrado para rastreabilidade."""
    assert nfe["arquivo_origem"] == "nfe_001.xml"


def test_arquivo_invalido_lanca_erro():
    """parse_nfe deve lançar exceção ao receber um caminho de arquivo inexistente."""
    with pytest.raises(Exception):
        parse_nfe("xmls/inexistente.xml")
