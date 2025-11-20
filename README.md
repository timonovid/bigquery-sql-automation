# bigquery-sql-automation

BigQuery SQL Automation provides a lightweight toolkit for managing templated SQL workflows in Google BigQuery. It lets you define a job specification in YAML, render parameterized SQL from Jinja templates, validate configurations locally, dry-run queries against BigQuery, and deploy scheduled queries via the BigQuery Data Transfer API.

## Quick start

1. **Install dependencies** from the project root:
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare a job spec** similar to `examples/job-spec.yaml` and place your Jinja templates under a directory like `examples/sql/`.

3. **Validate the spec** locally to ensure the configuration is complete:
   ```bash
   python -m sql_automation.cli validate --spec examples/job-spec.yaml
   ```

4. **Render SQL** to see the fully parameterized query text:
   ```bash
   python -m sql_automation.cli render \
     --spec examples/job-spec.yaml \
     --templates-root examples/sql
   ```

5. **Dry-run in BigQuery** to confirm the query executes without side effects (the `--project` flag is optional if your Application Default Credentials already target a project):
   ```bash
   python -m sql_automation.cli dry-run \
     --spec examples/job-spec.yaml \
     --templates-root examples/sql \
     --project your-gcp-project-id
   ```

6. **Deploy a scheduled query** via the BigQuery Data Transfer API:
   ```bash
   python -m sql_automation.cli deploy \
     --spec examples/job-spec.yaml \
     --templates-root examples/sql \
     --project your-gcp-project-id \
     --location US
   ```

## Command overview

- `validate`: Ensures the job specification YAML is well-formed and references existing template files.
- `render`: Renders Jinja SQL templates with variables defined in the job spec, printing the final query text.
- `dry-run`: Executes the rendered query with BigQuery's dry-run mode to verify syntax and resource estimates.
- `deploy`: Creates or updates a scheduled query in BigQuery using the Data Transfer API, applying schedule and destination settings from the spec.

## Tips

- Inspect `examples/job-spec.yaml` for a starting point when creating your own job configurations.
- Keep credentials available via `gcloud auth application-default login` or environment-based service account keys to allow dry-run and deploy commands to authenticate.
- Use separate template files per query to simplify maintenance and reusability across jobs.
