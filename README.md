# Pipeline de Dados NF-e Streaming — Arquitetura Medalhão com Star Schema

> Projeto desenvolvido como teste técnico para a vaga de **Engenheiro de Dados Sênior** na **Indra Group / Minsait**, e mantido como portfólio de referência em Engenharia de Dados com tecnologias do ecossistema Hadoop/Spark.

---

## Visão Geral

Pipeline de dados completo para ingestão, processamento e análise de **100 Notas Fiscais Eletrônicas (NF-e)** no padrão SEFAZ, implementando:

- **Arquitetura Medalhão** (Bronze → Silver → Gold)
- **Star Schema** (modelagem dimensional Kimball) na camada Gold
- **Streaming** com Apache Kafka (2 brokers)
- **Processamento distribuído** com Apache Spark (cluster com 2 workers)
- **Orquestração** com Apache Airflow (2 DAGs encadeadas)
- **Armazenamento** em HDFS com catálogo Hive
- **Visualização** via Hue (File Browser + Hive Editor)
- **Qualidade de dados** com validações explícitas e task de assertion no Airflow
- **Testes unitários** cobrindo todas as camadas (42 testes)

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Airflow                           │
│   DAG 1: ingest_xml_streaming    DAG 2: process_medallion       │
│   [xml_to_kafka]          [kafka_to_bronze]                     │
│                           [bronze_to_silver]                    │
│                           [validate_silver]  ← quality gate     │
│                           [silver_to_gold]                      │
└──────────────┬──────────────────────┬───────────────────────────┘
               │                      │
        ┌──────▼──────┐        ┌──────▼──────────────────────────┐
        │ Apache Kafka │        │              HDFS               │
        │  2 brokers   │        │  /data/bronze/  (JSON raw)      │
        │  1 tópico    │        │  /data/silver/  (Parquet clean) │
        └──────┬───────┘        │  /data/gold/    (Star Schema)   │
               │                └──────────────┬──────────────────┘
        ┌──────▼──────┐                        │
        │ Apache Spark │                ┌──────▼──────┐
        │  1 Master    │                │ Apache Hive │
        │  2 Workers   │                │  3 databases│
        └─────────────┘                │  12 tabelas │
                                       └─────────────┘
```

---

## Stack Tecnológica e Decisões de Arquitetura

### Apache Kafka — Por que 2 brokers?

Kafka é a espinha dorsal do streaming. Utilizei **2 brokers** (`kafka:29092` e `kafka-2:29093`) com **fator de replicação 2** para garantir:

- **Alta disponibilidade** — se um broker cair, o outro assume
- **Paralelismo** — produtores e consumidores podem se conectar a brokers diferentes
- **Resiliência** — nenhuma mensagem de NF-e é perdida

O tópico `nfe-raw` recebe os JSONs das notas fiscais publicados pelo PySpark via `kafka-python`.

### Apache Spark — Por que cluster mode com 2 workers?

O Spark foi configurado em **modo cluster standalone** (1 Master + 2 Workers) para simular um ambiente de produção real. Cada `spark-submit` distribui o processamento entre os workers, permitindo:

- **Paralelismo real** na leitura do Kafka (batch) e escrita no HDFS
- **Separação de responsabilidades** — driver no Airflow, executors nos workers
- **Escalabilidade** — basta adicionar mais workers ao `docker-compose.yml`

Utilizei **Spark 3.5** com o conector `spark-sql-kafka-0-10` para leitura batch do Kafka.

### Apache Airflow — Por que 2 DAGs?

Separar em 2 DAGs respeita o princípio de **responsabilidade única**:

| DAG | Responsabilidade | Schedule |
|---|---|---|
| `ingest_xml_streaming` | Leitura dos XMLs → Kafka | A cada hora |
| `process_medallion` | Kafka → Bronze → Silver → Gold | Acionada pela DAG 1 |

A DAG 1 dispara a DAG 2 via `TriggerDagRunOperator`, garantindo que o processamento só começa após a ingestão estar completa. Isso evita condições de corrida e torna o pipeline **idempotente**.

### HDFS — Por que não armazenar direto no Hive?

O HDFS é o **sistema de arquivos distribuído** que desacopla o armazenamento do processamento. Vantagens:

- Spark, Hive e qualquer outra ferramenta podem ler os mesmos dados
- Dados persistem independente do container Hive estar rodando
- Permite reprocessar uma camada sem reingestion (ex: reprocessar Silver sem reprocessar Bronze)

### Apache Hive — Por que tabelas EXTERNAL?

Todas as tabelas são **EXTERNAL** apontando para caminhos HDFS. Isso significa:

- `DROP TABLE` não apaga os dados do HDFS
- O Spark escreve diretamente no HDFS e o Hive apenas lê
- Múltiplas ferramentas (Hue, Superset) acessam os mesmos dados via SQL

---

## Arquitetura Medalhão

### Bronze — Dados Brutos

- **Formato:** JSON (1 arquivo por execução do pipeline)
- **Conteúdo:** Payload completo da NF-e sem transformações
- **Caminho HDFS:** `/data/bronze/nfe/`
- **Tabela Hive:** `bronze.nfe`

**Por que JSON na Bronze?**
A Bronze deve refletir exatamente o que veio da fonte. Se houver erro no parsing posterior, os dados originais estão preservados para reprocessamento.

### Silver — Dados Limpos

- **Formato:** Parquet (compressão Snappy)
- **Conteúdo:** Campos tipados, deduplicados e validados
- **Caminho HDFS:** `/data/silver/nfe/`
- **Tabela Hive:** `silver.nfe`

**Pipeline de qualidade aplicado:**

```
Bronze → extração de campos → validação de nulos → deduplicação → validação de valores → normalização → Silver
```

| Validação | Regra |
|---|---|
| Campos obrigatórios | `id_nfe`, `cnpj_emitente`, `valor_total_nf`, `data_emissao` não nulos |
| Deduplicação | `dropDuplicates(["id_nfe"])` — exatamente 1 NF-e por `id_nfe` |
| Valores monetários | `valor_total_nf > 0`, `valor_produtos > 0`, `valor_desconto >= 0` |
| Normalização | `trim()` em campos de texto |

**Por que Parquet na Silver?**
Parquet é colunar — consultas que leem apenas algumas colunas (ex: `valor_total_nf`) leem muito menos dados do disco. Ideal para analytics.

**Task de validação explícita no Airflow:**
A task `validate_silver` roda 4 assertions após a escrita. Se qualquer uma falhar, o pipeline para antes de popular a Gold:

```python
assert total == 100           # total esperado
assert duplicatas == 0        # zero duplicatas
assert nulos == 0             # zero campos obrigatórios nulos
assert valores_invalidos == 0 # zero valores monetários inválidos
```

### Gold — Star Schema (Kimball)

- **Formato:** Parquet por tabela
- **Caminho HDFS:** `/data/gold/{tabela}/`
- **Database Hive:** `gold`

**Por que Star Schema e não tabelas agregadas?**

Tabelas agregadas são rígidas — cada nova pergunta de negócio exige reprocessamento. O Star Schema permite **qualquer análise via JOIN** sem reprocessar dados, é extensível (nova dimensão = nova tabela) e é o padrão da indústria para Data Warehouses (metodologia Kimball).

#### Modelo Dimensional

```
                    dim_emitente
                  ┌──────────────────┐
                  │ id_emitente PK   │
                  │ cnpj             │
                  │ nome             │
                  │ uf               │
                  │ municipio        │
                  └────────┬─────────┘
                           │
dim_data                   │                dim_localidade
┌──────────────┐    ┌──────▼───────────┐   ┌──────────────────┐
│ id_data PK   │    │   fato_vendas    │   │ id_localidade PK │
│ data         ├────│ id_venda PK      ├───│ uf               │
│ dia          │    │ id_nfe           │   │ regiao           │
│ mes          │    │ id_emitente FK   │   │ estado_completo  │
│ ano          │    │ id_data FK       │   └──────────────────┘
│ trimestre    │    │ id_localidade FK │
└──────────────┘    │ valor_total      │
                    │ valor_produtos   │
                    │ valor_desconto   │
                    └──────────────────┘
```

#### Queries Analíticas

**Vendas por UF** — *Quais estados geram mais faturamento?*
```sql
SELECT l.uf, l.regiao, COUNT(f.id_nfe) AS quantidade_nfe,
       ROUND(SUM(f.valor_total), 2) AS faturamento_total,
       ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_localidade l ON f.id_localidade = l.id_localidade
GROUP BY l.uf, l.regiao ORDER BY faturamento_total DESC;
```

**Top Emitentes** — *Quais são os 10 maiores emitentes?*
```sql
SELECT e.cnpj, e.nome, l.uf, COUNT(f.id_nfe) AS quantidade_nfe,
       ROUND(SUM(f.valor_total), 2) AS faturamento_total,
       ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_emitente   e ON f.id_emitente   = e.id_emitente
JOIN gold.dim_localidade l ON f.id_localidade = l.id_localidade
GROUP BY e.cnpj, e.nome, l.uf ORDER BY faturamento_total DESC LIMIT 10;
```

**Evolução Mensal** — *Como o faturamento evoluiu mês a mês?*
```sql
SELECT d.ano, d.mes, COUNT(f.id_nfe) AS quantidade_nfe,
       ROUND(SUM(f.valor_total), 2) AS faturamento_total,
       ROUND(AVG(f.valor_total), 2) AS ticket_medio
FROM gold.fato_vendas f
JOIN gold.dim_data d ON f.id_data = d.id_data
GROUP BY d.ano, d.mes ORDER BY d.ano, d.mes;
```

---

## Estrutura do Projeto

```
pipeline_economia/
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest_xml.py          # DAG 1 — XMLs → Kafka
│   │   └── dag_process_medallion.py   # DAG 2 — Kafka → Bronze → Silver → Gold
│   └── scripts/
│       ├── ingest_xml_to_kafka.py     # Lê XMLs, parseia e publica no Kafka
│       ├── kafka_to_bronze.py         # Consome Kafka e salva JSON no HDFS
│       ├── bronze_to_silver.py        # Aplica qualidade e salva Parquet
│       ├── validate_silver.py         # 4 assertions de qualidade
│       └── silver_to_gold.py          # Constrói Star Schema no HDFS
├── engines/
│   └── processing/
│       ├── bronze.py                  # Lógica de ingestão Bronze
│       ├── silver.py                  # Pipeline de qualidade Silver
│       └── gold.py                    # Modelagem dimensional Gold
├── tests/
│   ├── test_xml_reader.py             # 8 testes — parser de XML
│   ├── test_kafka_producer.py         # Testes — producer Kafka
│   ├── test_bronze.py                 # Testes — camada Bronze
│   ├── test_silver.py                 # 7 testes — qualidade Silver
│   └── test_gold.py                   # 12 testes — Star Schema Gold
├── config/
│   ├── hadoop.env                     # Configurações Hadoop/Hive
│   └── hue/hue.ini                    # Configuração Hue
├── docs/
│   └── evidencias/                    # Prints e logs de execução
├── xmls/                              # 100 NF-es fictícias SEFAZ
└── docker-compose.yml                 # Infraestrutura completa
```

---

## Como Executar

### Pré-requisitos

- Docker e Docker Compose instalados
- Mínimo 8GB de RAM disponível para os containers

### 1. Subir a infraestrutura

```bash
docker compose up -d
```

Aguarde todos os serviços estarem saudáveis (~2 minutos). Serviços disponíveis:

| Serviço | URL |
|---|---|
| Airflow | http://localhost:8081 |
| Spark Master | http://localhost:8080 |
| Hue | http://localhost:8888 |
| Kafdrop (Kafka UI) | http://localhost:9000 |
| HDFS NameNode | http://localhost:50070 |

### 2. Inicializar tabelas no Hive

Acesse o Hue (usuário: `admin`, senha: `admin`) e rode no Hive Editor:

```sql
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE EXTERNAL TABLE bronze.nfe (payload STRING)
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/data/bronze/nfe';

CREATE EXTERNAL TABLE silver.nfe (
  id_nfe STRING, numero_nfe STRING, serie STRING, data_emissao STRING,
  natureza_operacao STRING, cnpj_emitente STRING, nome_emitente STRING,
  uf_emitente STRING, municipio_emitente STRING, nome_destinatario STRING,
  valor_produtos DOUBLE, valor_desconto DOUBLE, valor_total_nf DOUBLE, arquivo_origem STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/silver/nfe';

CREATE EXTERNAL TABLE gold.dim_emitente (
  id_emitente INT, cnpj STRING, nome STRING, uf STRING, municipio STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/gold/dim_emitente';

CREATE EXTERNAL TABLE gold.dim_data (
  id_data INT, data STRING, dia INT, mes INT, ano INT, trimestre INT
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/gold/dim_data';

CREATE EXTERNAL TABLE gold.dim_localidade (
  id_localidade INT, uf STRING, regiao STRING, estado_completo STRING
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/gold/dim_localidade';

CREATE EXTERNAL TABLE gold.fato_vendas (
  id_venda BIGINT, id_nfe STRING, id_emitente INT, id_data INT,
  id_localidade INT, valor_total DOUBLE, valor_produtos DOUBLE, valor_desconto DOUBLE
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/gold/fato_vendas';
```

### 3. Executar o pipeline

No Airflow (usuário: `admin`, senha: `admin`):

1. Acesse a DAG `ingest_xml_streaming`
2. Clique em **Trigger DAG**
3. Aguarde a conclusão — ela aciona automaticamente a DAG `process_medallion`

O fluxo completo executa em ~3 minutos:
```
xml_to_kafka → kafka_to_bronze → bronze_to_silver → validate_silver → silver_to_gold
```

### 4. Rodar os testes

```bash
docker exec pipeline_economia-airflow-1 bash -c \
  "pip install pytest -q && python3 -m pytest /opt/airflow/tests/ -v"
```

Resultado esperado: **42 testes passando**.

---

## Testes

| Módulo | Testes | O que cobre |
|---|---|---|
| `test_xml_reader.py` | 8 | Parser SEFAZ — campos, itens, erros |
| `test_kafka_producer.py` | 5 | Producer — conexão, serialização |
| `test_bronze.py` | 10 | Leitura Kafka, escrita HDFS |
| `test_silver.py` | 7 | Validações de qualidade |
| `test_gold.py` | 12 | Star Schema — dimensões, fato, mapeamentos |
| **Total** | **42** | |

Todos os testes usam **mocks** para rodar sem infraestrutura real (sem Spark, sem Kafka, sem HDFS).

---

## Tecnologias

| Tecnologia | Versão | Papel |
|---|---|---|
| Apache Spark | 3.5.0 | Processamento distribuído |
| Apache Kafka | 3.x | Streaming de mensagens |
| Apache Airflow | 2.x | Orquestração |
| Apache Hive | 2.3.2 | Catálogo e SQL analytics |
| HDFS | 3.3 | Armazenamento distribuído |
| Hue | 4.11 | Interface web HDFS + Hive |
| PostgreSQL | 15 | Metastore Hive + Airflow |
| Docker Compose | — | Infraestrutura local |
| Python | 3.8 | Scripts PySpark e testes |

---

## Autor

**Luis Felipe Maio Toledo de Carvalho e Silva**
