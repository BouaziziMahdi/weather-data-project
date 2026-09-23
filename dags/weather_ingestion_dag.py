from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

from src.weather.client import fetch_weather
from src.weather.validator import validation_weather_data
from src.messaging.kafka_producer import publish_weather

failure_email = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email="test05@gmail.com",
    to="mahdibouazizi10@gmail.com",
    subject="[AIRFLOW FAILED] {{ dag.dag_id }} - {{ ti.task_id }}",
    html_content="""
        <h2>Weather pipeline failed</h2>

        <p><b>DAG:</b> {{ dag.dag_id }}</p>
        <p><b>Task:</b> {{ ti.task_id }}</p>
        <p><b>Run ID:</b> {{ run_id }}</p>
        <p><b>Try:</b> {{ ti.try_number }}</p>
        <p><b>Exception:</b> {{ exception }}</p>
    """,
)


with DAG(
    dag_id="weather_ingestion",
    start_date=datetime(2026, 9, 17),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": [failure_email],
    },
    tags=["weather"],
):
    @task
    def fetch_validate_and_publish():
        data = fetch_weather()

        if not validation_weather_data(data):
            raise ValueError(
                "Weather data validation failed"
            )

        print("Weather data valid")

        publish_weather(data)

        print("Weather data published to Kafka")

    def spark_stage(task_id, script, packages, env_vars=None):
        return SparkSubmitOperator(
            task_id=task_id,
            application=f"/opt/airflow/src/spark/{script}.py",
            conn_id="spark_default",
            deploy_mode="client",
            name=task_id,
            packages=packages,
            conf={
                "spark.jars.ivy": "/tmp/weather-spark-ivy",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.driver.host": "airflow-worker",
                "spark.driver.bindAddress": "0.0.0.0",
                "spark.cores.max": "2",
            },
            executor_cores=1,
            executor_memory="1g",
            env_vars=env_vars,
            execution_timeout=timedelta(minutes=30),
        )

    delta_package = "io.delta:delta-spark_2.13:4.0.0"
    bronze = spark_stage(
        "bronze", "weather_bronze",
        f"{delta_package},org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
        {"BRONZE_RUN_MODE": "available-now"},
    )
    silver = spark_stage(
        "silver", "weather_silver", delta_package,
        {"SILVER_RUN_MODE": "available-now"},
    )
    gold = spark_stage("gold", "weather_gold", delta_package)
    serve = spark_stage(
        "gold_to_postgres", "gold_to_postgres",
        f"{delta_package},org.postgresql:postgresql:42.7.7",
    )
    fetch_validate_and_publish() >> bronze >> silver >> gold >> serve
