# bigquery-sql-automation

## from the project root
pip install -r requirements.txt

## validate the spec
python -m sql_automation.cli validate --spec examples/job-spec.yaml

##render SQL
python -m sql_automation.cli render \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql

## dry-run in BigQuery (the project flag is optional if ADC already targets a project)
python -m sql_automation.cli dry-run \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql \
  --project your-gcp-project-id

## deploy Scheduled Query via the Data Transfer API
python -m sql_automation.cli deploy \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql \
  --project your-gcp-project-id \
  --location US
