# Checklist do Projeto

## Dia 1: Infraestrutura
- [X] Organizar estrutura do projeto 
- [X] Subir Docker Compose com Spark, Kafka, Hive, Airflow
- [X] Testar comunicação entre os serviços
- [X] Garantir persistência com volumes
- [X] Commitar com mensagens padronizadas

## Dia 2: Ingestão e Orquestração
- [X] Implementar script PySpark para ler XMLs e converter em JSON
- [X] Padroninzando Enginnes 
- [X] Implementar teste Unitario
- [X] Criar DAG 1 no Airflow para orquestrar o processo
- [X] Testar envio dos dados ao Kafka
- [X] Commitar com mensagens padronizadas

## Dia 3: Consumo e Persistência
- [X] Adicionar mais um broker no kafka para haver paralelismo
- [X] Adicionando HDFS para armazenamento 
- [X] Implementar script PySpark para consumir do Kafka e salvar no Hive
- [X] Criar DAG 2 no Airflow com dependência da DAG 1
- [X] Validar persistência dos dados no Hive
- [X] Commitar com mensagens padronizadas

## Dia 4: Análises e Evidências
- [X] Implementar camada silver e gold 
- [X] Escrever consultas analíticas no Hive (contagens, somas, agrupamentos)
- [X] Salvar resultados das consultas no Hive
- [X] Capturar evidências (prints, logs, outputs)
- [X] Commitar com mensagens padronizadas

## Dia 5: Revisão e Documentação
- [X] Revisar todo o projeto
- [X] Ajustar detalhes finais
- [X] Preparar documentação no repositório
- [X] Garantir que o fork no GitHub está completo e organizado
- [X] Commitar com mensagens padronizadas

## Dia 6: Finalização
- [ ] Última revisão
- [ ] Enviar link do repositório + currículo


## Extras implementados além do escopo original

- [X] Subir Superset
- [ ] Criar analise do Star Schema
