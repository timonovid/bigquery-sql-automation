# bigquery-sql-automation

# в корне проекта
pip install -r requirements.txt

# валидация spec
python -m sql_automation.cli validate --spec examples/job-spec.yaml

# рендер SQL
python -m sql_automation.cli render \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql

# dry-run в BigQuery (project можно не задавать, если ADC уже привязаны к проекту)
python -m sql_automation.cli dry-run \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql \
  --project your-gcp-project-id

# деплой Scheduled Query через Data Transfer API
python -m sql_automation.cli deploy \
  --spec examples/job-spec.yaml \
  --templates-root examples/sql \
  --project your-gcp-project-id \
  --location US
