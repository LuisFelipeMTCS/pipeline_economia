# Checklist do Projeto

## Dia 1: Infraestrutura
- [X] Organizar estrutura do projeto 
- [X] Subir Docker Compose com Spark, Kafka, Hive, Airflow
- [ ] Testar comunicação entre os serviços
- [ ] Garantir persistência com volumes
- [ ] Commitar com mensagens padronizadas

## Dia 2: Ingestão e Orquestração
- [ ] Implementar script PySpark para ler XMLs e converter em JSON
- [ ] Criar DAG 1 no Airflow para orquestrar o processo
- [ ] Testar envio dos dados ao Kafka
- [ ] Commitar com mensagens padronizadas

## Dia 3: Consumo e Persistência
- [ ] Implementar script PySpark para consumir do Kafka e salvar no Hive
- [ ] Criar DAG 2 no Airflow com dependência da DAG 1
- [ ] Validar persistência dos dados no Hive
- [ ] Commitar com mensagens padronizadas

## Dia 4: Análises e Evidências
- [ ] Escrever consultas analíticas no Hive (contagens, somas, agrupamentos)
- [ ] Salvar resultados das consultas no Hive
- [ ] Capturar evidências (prints, logs, outputs)
- [ ] Commitar com mensagens padronizadas

## Dia 5: Revisão e Documentação
- [ ] Revisar todo o projeto
- [ ] Ajustar detalhes finais
- [ ] Preparar documentação no repositório
- [ ] Garantir que o fork no GitHub está completo e organizado
- [ ] Commitar com mensagens padronizadas

## Dia 6: Finalização
- [ ] Última revisão
- [ ] Enviar link do repositório + currículo
