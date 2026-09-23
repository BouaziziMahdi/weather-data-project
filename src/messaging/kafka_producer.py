import json
import os

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:19092",
)

KAFKA_TOPIC = "weather"


def publish_weather(data: dict) -> None:
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    message = json.dumps(data)

    producer.produce(
        topic=KAFKA_TOPIC,
        value=message.encode("utf-8"),
    )

    remaining = producer.flush(10)

    if remaining != 0:
        raise RuntimeError(
            f"{remaining} message(s) Kafka non envoyés"
        )
