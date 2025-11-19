from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from google.cloud import bigquery

from .config import load_job_spec
from .renderer import SqlRenderer
from .bigquery_ops import dry_run_query, deploy_scheduled_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sql_automation.cli")


def cmd_validate(args: argparse.Namespace) -> int:
    path = Path(args.spec)
    try:
        spec = load_job_spec(path)
    except Exception as e:
        logger.error("Spec validation failed: %s", e)
        return 1
    logger.info("Spec is valid. Parsed model:\n%s", spec.model_dump_json(indent=2))
    return 0


def cmd_render(args: argparse.Namespace) -> int:
    spec_path = Path(args.spec)
    templates_root = Path(args.templates_root)
    try:
        spec = load_job_spec(spec_path)
        renderer = SqlRenderer(templates_root)
        sql = renderer.render(spec.sql_template, spec.parameters)
    except Exception as e:
        logger.error("Render failed: %s", e)
        return 1

    if args.output:
        out_path = Path(args.output)
        out_path.write_text(sql + "\n", encoding="utf-8")
        logger.info("Rendered SQL written to %s", out_path)
    else:
        print(sql)
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    spec_path = Path(args.spec)
    templates_root = Path(args.templates_root)
    try:
        spec = load_job_spec(spec_path)
        renderer = SqlRenderer(templates_root)
        sql = renderer.render(spec.sql_template, spec.parameters)
    except Exception as e:
        logger.error("Pre-dry-run failed (spec/template): %s", e)
        return 1

    project_id = args.project or None
    client = bigquery.Client(project=project_id)

    try:
        estimated_bytes, slot_ms = dry_run_query(
            client=client,
            sql=sql,
            max_bytes_billed=spec.limits.max_bytes_billed,
            job_spec=spec,
        )
    except Exception as e:
        logger.error("Dry-run failed: %s", e)
        return 1

    logger.info(
        "Dry-run success. Estimated bytes: %d, slot-ms: %f",
        estimated_bytes,
        slot_ms,
    )
    return 0


def cmd_deploy(args: argparse.Namespace) -> int:
    spec_path = Path(args.spec)
    templates_root = Path(args.templates_root)
    try:
        spec = load_job_spec(spec_path)
        renderer = SqlRenderer(templates_root)
        sql = renderer.render(spec.sql_template, spec.parameters)
    except Exception as e:
        logger.error("Pre-deploy failed (spec/template): %s", e)
        return 1

    project_id = args.project
    if not project_id:
        logger.error("--project is required for deploy")
        return 1

    # dry-run перед деплоем (можно сделать опциональным, но по статье лучше обязательный)
    client = bigquery.Client(project=project_id)
    try:
        dry_run_query(
            client=client,
            sql=sql,
            max_bytes_billed=spec.limits.max_bytes_billed,
            job_spec=spec,
        )
    except Exception as e:
        logger.error("Dry-run before deploy failed: %s", e)
        return 1

    try:
        deploy_scheduled_query(
            job_spec=spec,
            sql=sql,
            default_project=project_id,
            location=args.location,
        )
    except Exception as e:
        logger.error("Deploy failed: %s", e)
        return 1

    logger.info("Deploy finished successfully")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sql-automation",
        description="Spec-driven SQL automation for BigQuery (validate, render, dry-run, deploy).",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # validate
    p_val = sub.add_parser("validate", help="Validate job spec YAML.")
    p_val.add_argument("--spec", required=True, help="Path to job-spec.yaml")
    p_val.set_defaults(func=cmd_validate)

    # render
    p_r = sub.add_parser("render", help="Render SQL from spec + template.")
    p_r.add_argument("--spec", required=True, help="Path to job-spec.yaml")
    p_r.add_argument(
        "--templates-root",
        required=True,
        help="Path to root directory with SQL templates.",
    )
    p_r.add_argument(
        "--output",
        help="Optional path to write rendered SQL. If omitted, prints to stdout.",
    )
    p_r.set_defaults(func=cmd_render)

    # dry-run
    p_dr = sub.add_parser("dry-run", help="Dry-run BigQuery query from spec + template.")
    p_dr.add_argument("--spec", required=True, help="Path to job-spec.yaml")
    p_dr.add_argument(
        "--templates-root",
        required=True,
        help="Path to root directory with SQL templates.",
    )
    p_dr.add_argument(
        "--project",
        required=False,
        help="GCP project id for BigQuery. If omitted, uses default credentials project.",
    )
    p_dr.set_defaults(func=cmd_dry_run)

    # deploy
    p_dep = sub.add_parser(
        "deploy",
        help="Validate, dry-run and deploy BigQuery scheduled query via API.",
    )
    p_dep.add_argument("--spec", required=True, help="Path to job-spec.yaml")
    p_dep.add_argument(
        "--templates-root",
        required=True,
        help="Path to root directory with SQL templates.",
    )
    p_dep.add_argument(
        "--project",
        required=True,
        help="GCP project id where scheduled query will be created.",
    )
    p_dep.add_argument(
        "--location",
        default="US",
        help="BigQuery/Data Transfer location (default: US).",
    )
    p_dep.set_defaults(func=cmd_deploy)

    return p


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
