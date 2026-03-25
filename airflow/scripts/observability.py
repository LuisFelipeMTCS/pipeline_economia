"""
Callbacks de observabilidade compartilhados entre os DAGs do pipeline.

Registra falhas e sucessos de todas as tasks com contexto completo de execução.
"""

import logging

log = logging.getLogger("airflow.task")


def on_task_failure(context):
    log.error(
        "[OBSERVABILIDADE] FALHA | dag=%s | task=%s | run=%s | erro=%s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["run_id"],
        context.get("exception"),
    )


def on_task_success(context):
    log.info(
        "[OBSERVABILIDADE] SUCESSO | dag=%s | task=%s | run=%s | duração=%.1fs",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["run_id"],
        context["task_instance"].duration or 0,
    )
