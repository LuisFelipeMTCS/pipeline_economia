# Testes Unitários

Testes unitários da camada `engines/`, validando o comportamento das funções
de ingestão sem necessidade de infraestrutura (Docker, Kafka, Spark).

## Como executar

```bash
# Ativar o ambiente virtual
source .venv/bin/activate

# Rodar todos os testes
pytest tests/ -v

# Rodar um arquivo específico
pytest tests/test_xml_reader.py -v
pytest tests/test_kafka_producer.py -v
```

## Dependências

```bash
pip install -r requirements.txt
```

---

## test_xml_reader.py

Testa a função `parse_nfe()` de `engines/ingestion/xml_reader.py`.

| Teste | O que valida |
|---|---|
| `test_retorna_dict` | A função retorna um dicionário Python |
| `test_campos_obrigatorios_presentes` | Todos os campos do schema estão presentes no retorno |
| `test_campos_nao_vazios` | Campos críticos (CNPJ, nome, valor) não são nulos |
| `test_itens_e_lista` | O campo `itens` é uma lista |
| `test_itens_nao_vazios` | A NF-e contém ao menos um produto |
| `test_item_tem_campos_esperados` | Cada item tem descrição, quantidade e valor |
| `test_arquivo_origem_correto` | O nome do arquivo de origem é registrado corretamente |
| `test_arquivo_invalido_lanca_erro` | Arquivo inexistente lança exceção |

---

## test_kafka_producer.py

Testa as funções de `engines/ingestion/kafka_producer.py` usando **mocks**,
sem necessidade do broker Kafka rodando.

| Teste | O que valida |
|---|---|
| `test_create_producer_usa_broker_correto` | O producer conecta ao broker informado |
| `test_create_producer_retorna_instancia` | A função retorna a instância criada |
| `test_publish_nfe_envia_mensagem` | `send()` é chamado com o tópico e os dados corretos |
| `test_publish_nfe_usa_id_como_chave` | A chave da mensagem é o `id_nfe` em bytes |
| `test_flush_and_close_chama_flush_e_close` | Ambos `flush()` e `close()` são chamados |
| `test_flush_chamado_antes_do_close` | `flush()` ocorre antes de `close()` |
