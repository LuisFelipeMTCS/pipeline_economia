import xml.etree.ElementTree as ET
from pathlib import Path

NS = "http://www.portalfiscal.inf.br/nfe"


def _tag(name: str) -> str:
    return f"{{{NS}}}{name}"


def _text(parent, tag: str) -> str | None:
    el = parent.find(_tag(tag))
    return el.text if el is not None else None


def parse_nfe(xml_path: str) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    inf_nfe = root.find(f"{_tag('NFe')}/{_tag('infNFe')}")
    if inf_nfe is None:
        raise ValueError(f"infNFe não encontrado em {xml_path}")

    ide = inf_nfe.find(_tag("ide"))
    emit = inf_nfe.find(_tag("emit"))
    dest = inf_nfe.find(_tag("dest"))
    ender_emit = emit.find(_tag("enderEmit")) if emit is not None else None
    icms_tot = inf_nfe.find(f"{_tag('total')}/{_tag('ICMSTot')}")

    itens = []
    for det in inf_nfe.findall(_tag("det")):
        prod = det.find(_tag("prod"))
        if prod is not None:
            itens.append({
                "numero_item": det.get("nItem"),
                "codigo": _text(prod, "cProd"),
                "descricao": _text(prod, "xProd"),
                "ncm": _text(prod, "NCM"),
                "cfop": _text(prod, "CFOP"),
                "unidade": _text(prod, "uCom"),
                "quantidade": _text(prod, "qCom"),
                "valor_unitario": _text(prod, "vUnCom"),
                "valor_total_item": _text(prod, "vProd"),
            })

    return {
        "id_nfe": inf_nfe.get("Id"),
        "numero_nfe": _text(ide, "nNF"),
        "serie": _text(ide, "serie"),
        "data_emissao": _text(ide, "dhEmi"),
        "natureza_operacao": _text(ide, "natOp"),
        "cnpj_emitente": _text(emit, "CNPJ") if emit is not None else None,
        "nome_emitente": _text(emit, "xNome") if emit is not None else None,
        "uf_emitente": _text(ender_emit, "UF") if ender_emit is not None else None,
        "municipio_emitente": _text(ender_emit, "xMun") if ender_emit is not None else None,
        "cpf_destinatario": _text(dest, "CPF") if dest is not None else None,
        "cnpj_destinatario": _text(dest, "CNPJ") if dest is not None else None,
        "nome_destinatario": _text(dest, "xNome") if dest is not None else None,
        "valor_produtos": _text(icms_tot, "vProd") if icms_tot is not None else None,
        "valor_desconto": _text(icms_tot, "vDesc") if icms_tot is not None else None,
        "valor_total_nf": _text(icms_tot, "vNF") if icms_tot is not None else None,
        "itens": itens,
        "arquivo_origem": Path(xml_path).name,
    }
