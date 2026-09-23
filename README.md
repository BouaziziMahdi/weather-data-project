# Weather data pipeline

Open-Meteo → Kafka → Spark Bronze/Silver/Gold → PostgreSQL, orchestrated by Airflow.

## CI before a deployment server is available

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch:

1. Unit tests for API requests, validation, and Kafka publication. Network calls
   are mocked; failures must propagate so Airflow can retry.
2. A 95% branch/line coverage gate for `src/weather` and `src/messaging`.
   JUnit and coverage XML reports are uploaded as `test-reports`.
3. Compose validation with a disposable `.env` generated on the CI runner.
4. Build the actual Airflow image, then test DAG imports, dependencies, ingestion
   behavior, and referenced Spark scripts with the real Airflow providers.
5. Run real local Spark tests for Silver filtering, deduplication, hourly Gold
   metrics, and Tunis time buckets. The jobs use the same transformation functions.

No GitHub secrets, production server, registry push, or deployment is required.
The workflow validates the build but does not publish an application image.
The Airflow image still depends on the repository's mounted `dags/` and `src/`.

This is not yet an end-to-end test of Kafka transport, Delta checkpoint recovery
and MERGE idempotency, or JDBC/PostgreSQL UPSERT. The coverage percentage applies
only to ingestion modules, not the full pipeline. Add isolated integration tests
for those boundaries before enabling deployment. A future deployment job should
depend on successful CI and use the chosen server/registry credentials.

Push these files into your GitHub repository, then inspect the **Weather CI** run
in **Actions**. After a successful run, require the `tests` and `docker` checks in
your protected branch rules. GitHub execution can only be confirmed after a push.
See [GitHub's Python CI documentation](https://docs.github.com/en/actions/tutorials/build-and-test-code/python).

## Run the same checks locally

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pytest --cov=src.weather --cov=src.messaging --cov-branch --cov-fail-under=95 --cov-report=term-missing
docker compose --env-file .env config --quiet
docker build -f dockerFile.airflow -t weather-airflow:ci .
docker run --rm --env-file .env \
  -e PYTHONPATH=/opt/airflow -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -v "$PWD/dags:/opt/airflow/dags:ro" \
  -v "$PWD/src:/opt/airflow/src:ro" \
  -v "$PWD/ci:/opt/airflow/ci:ro" \
  --entrypoint python weather-airflow:ci /opt/airflow/ci/check_dag.py
docker run --rm -e PYTHONPATH=/opt/airflow -e SPARK_LOCAL_IP=127.0.0.1 \
  -v "$PWD/src:/opt/airflow/src:ro" \
  -v "$PWD/ci:/opt/airflow/ci:ro" \
  --entrypoint python weather-airflow:ci /opt/airflow/ci/check_spark.py
```

All Python dependencies for local work, tests, and Airflow are in `requirements.txt`.
Use Python 3.12, matching CI, for the local environment.
Configuration lives in `.env`, which is excluded from Git and Docker build contexts.
On a fresh checkout, run `python ci/create_env.py` to generate `.env`, then adjust
its values for local use, including SMTP credentials. The script refuses to
overwrite an existing `.env`. CI generates its own file on the runner.
Logs, local data, and test reports are also excluded from Git.
