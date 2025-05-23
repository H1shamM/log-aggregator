import os

import pika, json, random, time

# mock log data
sources =["app1","app2","app3"]
events = ["login_failed", "high-cpu","new_user"]

#conncet to RabbitMQ
rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")

connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
channel = connection.channel()
channel.queue_declare(queue="log_queue", durable=True)

for i in range(10):
    log = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": random.choice(sources),
        "event": random.choice(events),
        "count": random.randint(1,5)
    }
    channel.basic_publish(exchange="",routing_key="log_queue", body=json.dumps(log))
    print(f"Sent: {log}")
connection.close()
