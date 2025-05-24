import json
import random
import time

import pika
from decouple import config

# ─── Configuration ────────────────────────────────────────────────────────────
RABBITMQ_HOST = config('RABBITMQ_HOST', default='localhost')
RABBITMQ_QUEUE = config('RABBITMQ_QUEUE', default='log_queue')
# ───────────────────────────────────────────────────────────────────────────────

# Mock log data
SOURCES = ["app1", "app2", "app3"]
EVENTS = ["login_failed", "high-cpu", "new_user"]


def main():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    for _ in range(10):
        log = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": random.choice(SOURCES),
            "event": random.choice(EVENTS),
            "count": random.randint(1, 5)
        }
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(log)
        )
        print(f"Sent: {log}")
        time.sleep(0.5)  # small pause for readability

    connection.close()


if __name__ == "__main__":
    main()
