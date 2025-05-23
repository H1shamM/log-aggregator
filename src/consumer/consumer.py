import logging
import os
import time
from collections import deque
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

import boto3
import pika, json

S3_BUCKET = "log-aggregator-2024"
BATCH_SIZE = 50
MAX_WORKERS = 5
RETRY_LIMIT = 3
DLQ_NAME = "log_dlq"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_s3():
    try:
        return boto3.client('s3')
    except Exception as e:
        logger.critical(f'failed to init S3:{e}')
        raise


def initialize_rabbitmq():
    try:
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")

        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        channel = connection.channel()

        channel.queue_declare("log_queue", durable=True)

        channel.exchange_declare(exchange="dlx", exchange_type='direct')
        channel.queue_declare(queue=DLQ_NAME, durable=True)
        channel.queue_bind(exchange="dlx", queue=DLQ_NAME, routing_key=DLQ_NAME)

        return connection, channel
    except Exception as e:
        logger.critical(f"RabbitMQ connection failed: {e}")
        raise


def process_batch(s3_client, batch):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(
            lambda log: process_single_log(s3_client, log),
            batch
        ))

    success_rate = sum(results) / len(results)
    if success_rate < 0.9:
        alert_admin(
            f"Low upload success ({success_rate * 100:.1f}%). "
            f"Failed {len(results) - sum(results)}/{len(results)}")
    return success_rate


def process_single_log(s3_client, log):
    timestamp = datetime.now().isoformat()
    key = f"logs/{timestamp[:10]}/{log['source']}/{timestamp[11:19]}.json"

    for attempt in range(RETRY_LIMIT):
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(log),
                Metadata={
                    'attempt': str(attempt + 1),
                    'source': log.get('source', 'unknown')
                }
            )
            return True
        except Exception as e:
            if attempt == RETRY_LIMIT - 1:
                logger.error(f"Final fail after {RETRY_LIMIT} attempts: {e}")
                handle_failed_log(log,e)
                return False
            wait = min(2 ** attempt, 10)
            time.sleep(wait)


def handle_failed_log(log, error):
    """Fallback storage for failed logs"""
    try:
        with open('failed_logs.ndjson', 'a') as f:
            log['_error'] = str(error)
            f.write(json.dumps(log) + '\n')
    except Exception as e:
        logger.critical(f"Failed to save log locally: {e}")


def alert_admin(message):
    """Simple email/slack alert placeholder"""
    print(f"ADMIN ALERT: {message}")


def message_callback(s3_client, batch_buffer):
    def callback(ch, method, properties, body):
        try:
            log = json.loads(body)
            batch_buffer.append(log)
            if len(batch_buffer) >= BATCH_SIZE:
                current_batch = [batch_buffer.popleft() for _ in range(BATCH_SIZE)]
                process_batch(s3_client, current_batch)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {body}")
            ch.basic_publish(
                exchange="dlx",
                routing_key=DLQ_NAME,
                body=body
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    return callback

def main():

    try:
        logger.info("Initializing services...")
        s3_client = initialize_s3()
        rmq_connection ,rmp_channel = initialize_rabbitmq()
        batch_buffer = deque(maxlen=BATCH_SIZE * 2)

        logger.info("Starting consumer...")

        rmp_channel.basic_consume(
            queue="log_queue",
            on_message_callback=message_callback(s3_client, batch_buffer),
            consumer_tag="log_aggregator"
        )
        rmp_channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.critical(f'Fatal Error:{e}')
    finally:
        if 'rmq_connection' in locals():
            rmq_connection.close()
        logger.info("Service stopped")

if __name__ == "__main__":
    main()