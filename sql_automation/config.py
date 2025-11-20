from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from crontab import CronSlices


class Limits(BaseModel):
    max_bytes_billed: int = Field(
        ...,
        ge=0,
        description="Maximum bytes billed limit for the query (BigQuery maxBytesBilled).",
    )


class JobSpec(BaseModel):
    name: str = Field(..., description="Logical job name, also used as display name.")
    schedule: str = Field(
        ...,
        description="Cron expression or BigQuery schedule string (e.g. 'every 24 hours').",
    )
    sql_template: str = Field(
        ..., description="Relative path to SQL template (Jinja2)."
    )
    destination_table: str = Field(
        ...,
        description="Fully qualified table id like 'project.dataset.table' or 'dataset.table'.",
    )
    write_disposition: str = Field(
        "WRITE_TRUNCATE",
        description="WRITE_TRUNCATE, WRITE_APPEND or WRITE_EMPTY.",
    )
    labels: Dict[str, str] = Field(
        default_factory=dict,
        description="Labels to attach to the scheduled query / job.",
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Template parameters for SQL rendering.",
    )
    limits: Limits

    # optional fields for stricter policies
    environment: Optional[str] = Field(
        default=None,
        description="Environment name like dev/stage/prod, often duplicated in labels.",
    )

    @field_validator("write_disposition")
    @classmethod
    def validate_write_disposition(cls, v: str) -> str:
        allowed = {"WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"}
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"write_disposition must be one of {allowed}, got {v}")
        return v_upper

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: str) -> str:
        """
        Basic validation: either cron or a string like 'every ...'.
        """
        v_stripped = v.strip()
        if v_stripped.lower().startswith("every "):
            return v_stripped
        # allow cron strings validated via CronSlices
        if not CronSlices.is_valid(v_stripped):
            raise ValueError(
                f"schedule must be valid cron or 'every ...' string, got '{v_stripped}'"
            )
        return v_stripped

    @field_validator("destination_table")
    @classmethod
    def validate_destination_table(cls, v: str) -> str:
        raw = v.strip()
        # allow 'project.dataset.table' or 'dataset.table'
        parts = raw.split(".")
        if len(parts) not in (2, 3):
            raise ValueError(
                "destination_table must be 'dataset.table' or 'project.dataset.table', "
                f"got '{raw}'"
            )
        return raw

    @model_validator(mode="after")
    def validate_labels_and_env(self) -> "JobSpec":
        required_labels = {"owner", "domain", "environment"}
        missing = [lbl for lbl in required_labels if lbl not in self.labels]
        if missing:
            raise ValueError(f"missing required labels: {missing}")

        env = self.environment or self.labels.get("environment")
        if env not in {"dev", "stage", "prod"}:
            raise ValueError(
                f"environment must be one of dev/stage/prod (from field or labels), got '{env}'"
            )
        # keep environment synchronized with labels
        self.environment = env
        self.labels["environment"] = env
        return self


def load_job_spec(path: Path) -> JobSpec:
    if not path.exists():
        raise FileNotFoundError(f"job spec file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"job spec must be YAML mapping, got {type(data)}")
    return JobSpec(**data)
