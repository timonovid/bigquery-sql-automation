from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Tuple

from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1

from .config import JobSpec

logger = logging.getLogger(__name__)


@dataclass
class ParsedTableId:
    project_id: str
    dataset_id: str
    table_id: str


def parse_table_id(destination_table: str, default_project: str) -> ParsedTableId:
    """
    Принимает 'dataset.table' или 'project.dataset.table',
    возвращает ParsedTableId с подставленным default_project при необходимости.
    """
    parts = destination_table.split(".")
    if len(parts) == 2:
        dataset_id, table_id = parts
        project_id = default_project
    elif len(parts) == 3:
        project_id, dataset_id, table_id = parts
    else:
        raise ValueError(
            f"destination_table must be 'dataset.table' or 'project.dataset.table', got '{destination_table}'"
        )
    return ParsedTableId(project_id=project_id, dataset_id=dataset_id, table_id=table_id)


def dry_run_query(
    client: bigquery.Client,
    sql: str,
    max_bytes_billed: int,
    job_spec: JobSpec,
) -> Tuple[int, float]:
    """
    Делает dry-run BigQuery запроса и возвращает (estimated_bytes, total_slot_ms).
    Бросает исключение при ошибках.
    """
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        maximum_bytes_billed=max_bytes_billed,
        labels=job_spec.labels,
    )

    logger.info("Starting dry-run for job '%s'", job_spec.name)
    job = client.query(sql, job_config=job_config)
    # в dry-run job не исполняется, но метаданные доступны
    stats = job._job_statistics()  # не самый официальный API, но работает; можно взять job._properties
    estimated_bytes = int(stats.get("totalBytesProcessed", 0))
    slot_ms = float(stats.get("totalSlotMs", 0))

    logger.info(
        "Dry-run for job '%s' estimated %d bytes, %f slot-ms",
        job_spec.name,
        estimated_bytes,
        slot_ms,
    )

    if estimated_bytes > max_bytes_billed:
        raise RuntimeError(
            f"Dry-run estimated {estimated_bytes} bytes, which exceeds max_bytes_billed={max_bytes_billed}"
        )

    return estimated_bytes, slot_ms


def deploy_scheduled_query(
    job_spec: JobSpec,
    sql: str,
    default_project: str,
    location: str = "US",
) -> None:
    """
    Создаёт или обновляет BigQuery Scheduled Query через DataTransferService.
    Предполагается, что аутентификация уже настроена через ADC.
    """

    parsed = parse_table_id(job_spec.destination_table, default_project)

    transfer_client = bigquery_datatransfer_v1.DataTransferServiceClient()

    parent = transfer_client.common_project_path(parsed.project_id)

    # Параметры для data source 'scheduled_query'
    params = {
        "query": sql,
        "destination_table_name_template": parsed.table_id,
        "write_disposition": job_spec.write_disposition,
        "partitioning_field": "",  # можно задать при необходимости
    }

    transfer_config = bigquery_datatransfer_v1.TransferConfig(
        destination_dataset_id=parsed.dataset_id,
        display_name=job_spec.name,
        data_source_id="scheduled_query",
        params=params,
        schedule=job_spec.schedule,
        disabled=False,
    )

    # Попробуем найти существующую конфигурацию с тем же display_name и dataset
    logger.info(
        "Deploying scheduled query '%s' to dataset '%s.%s' (project=%s)",
        job_spec.name,
        parsed.project_id,
        parsed.dataset_id,
        parsed.project_id,
    )

    existing = None
    for cfg in transfer_client.list_transfer_configs(parent=parent):
        if (
            cfg.display_name == job_spec.name
            and cfg.destination_dataset_id == parsed.dataset_id
            and cfg.data_source_id == "scheduled_query"
        ):
            existing = cfg
            break

    if existing is None:
        # создаём новую конфигурацию
        created = transfer_client.create_transfer_config(
            parent=parent,
            transfer_config=transfer_config,
        )
        logger.info("Created new scheduled query config: %s", created.name)
    else:
        # обновляем существующую
        transfer_config.name = existing.name
        # указываем, какие поля обновляем
        update_mask = {"paths": ["params", "schedule", "display_name"]}
        updated = transfer_client.update_transfer_config(
            transfer_config=transfer_config,
            update_mask=update_mask,
        )
        logger.info("Updated existing scheduled query config: %s", updated.name)
