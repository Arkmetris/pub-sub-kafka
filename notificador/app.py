import json
import logging
import os

import yagmail
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO)

SMTP_USER = os.getenv("SMTP_USER", "matheusmoras77@gmail.com")
RECEIVER = os.getenv("NOTIFICATION_RECEIVER", SMTP_USER)
OAUTH2_FILE = "/notificador/oauth2_creds.json"


def send_notification(filename, operation):
    body = f"O arquivo {filename} foi {operation}."
    yag = yagmail.SMTP(SMTP_USER, oauth2_file=OAUTH2_FILE)
    yag.send(
        to=RECEIVER,
        subject="Operação de imagem concluída",
        contents=body,
    )


consumer = Consumer(
    {
        "bootstrap.servers": "kafka1:19091,kafka2:19092,kafka3:19093",
        "group.id": "notificador-group",
        "client.id": "notificador",
        "enable.auto.commit": True,
        "session.timeout.ms": 6000,
        "default.topic.config": {"auto.offset.reset": "smallest"},
    }
)

consumer.subscribe(["notifications"])

try:
    while True:
        message = consumer.poll(0.1)
        if message is None:
            continue
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                logging.error("Kafka error: %s", message.error().str())
            continue

        data = json.loads(message.value())
        filename = data["filename"]
        operation = data["operation"]
        logging.info("Sending notification for %s: %s", filename, operation)
        send_notification(filename, operation)
        logging.info("Notification sent for %s", filename)
finally:
    consumer.close()
