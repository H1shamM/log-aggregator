import json
import logging
import os
import time
from collections import deque
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

import boto3
import pika
from decouple import config

# ─── Configuration ────────────────────────────────────────────────────────────
RABBITMQ_HOST = config('RABBITMQ_HOST', default='localhost')
RABBITMQ_QUEUE = config('RABBITMQ_QUEUE', default='log_queue')
RABBITMQ_DLQ = config('RABBITMQ_DLQ', default='log_dlq')
S3_BUCKET = config('S3_BUCKET')
AWS_REGION = config('AWS_REGION', default='us-east-1')
BATCH_SIZE = config('BATCH_SIZE', cast=int, default=50)
MAX_WORKERS = config('MAX_WORKERS', cast=int, default=5)
RETRY_LIMIT = config('RETRY_LIMIT', cast=int, default=3)
FALLBACK_PATH = config('FALLBACK_PATH', default='/tmp/failed_logs.ndjson')
# ───────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_s3():
    boto3_kwargs = {'region_name': AWS_REGION}
    try:
        return boto3.client('s3', **boto3_kwargs)
    except Exception as e:
        logger.critical(f'failed to init S3: {e}')
        raise


def initialize_rabbitmq():
    try:
        conn_params = pika.ConnectionParameters(RABBITMQ_HOST)
        connection = pika.BlockingConnection(conn_params)
        channel = connection.channel()

        channel.queue_declare(RABBITMQ_QUEUE, durable=True)
        channel.exchange_declare(exchange="dlx", exchange_type='direct')
        channel.queue_declare(queue=RABBITMQ_DLQ, durable=True)
        channel.queue_bind(exchange="dlx", queue=RABBITMQ_DLQ, routing_key=RABBITMQ_DLQ)

        return connection, channel
    except Exception as e:
        logger.critical(f"RabbitMQ connection failed: {e}")
        raise


def process_batch(s3_client, batch):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(
            executor.map(
                lambda log: process_single_log(s3_client, log), batch
            )
        )

    success_rate = sum(results) / len(results)
    if success_rate < 0.9:
        alert_admin(
            f"Low upload success ({success_rate * 100:.1f}%). "
            f"Failed {len(results) - sum(results)}/{len(results)}"
        )
    return success_rate


def process_single_log(s3_client, log):
    timestamp = datetime.utcnow().isoformat()
    key = (f"logs/{timestamp[:10]}/"
           f"{log.get('source', 'unknown')}/{timestamp[11:19]}.json")

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(log),
                Metadata={
                    'attempt': str(attempt),
                    'source': log.get('source', 'unknown')
                }
            )
            return True
        except Exception as e:
            if attempt == RETRY_LIMIT:
                logger.error(f"Final fail after {RETRY_LIMIT} attempts: {e}")
                handle_failed_log(log, e)
                return False
            time.sleep(min(2 ** (attempt - 1), 10))
    return False


def handle_failed_log(log, error):
    """Fallback storage for failed logs"""
    try:
        log_copy = dict(log)
        log_copy['_error'] = str(error)

        fallback_path = os.getenv('FALLBACK_PATH', FALLBACK_PATH)
        dirpath = os.path.dirname(fallback_path)
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)

        with open(fallback_path, 'a') as f:
            f.write(json.dumps(log_copy) + '\n')
    except Exception as e:
        logger.critical(f"Failed to save log locally: {e}")


def alert_admin(message):
    """Simple email/slack alert placeholder"""
    logger.warning(f"ADMIN ALERT: {message}")


def message_callback(s3_client, batch_buffer):
    def callback(ch, method, properties, body):
        try:
            log = json.loads(body)
            batch_buffer.append(log)
            if len(batch_buffer) >= BATCH_SIZE:
                batch = [batch_buffer.popleft() for _ in range(BATCH_SIZE)]
                process_batch(s3_client, batch)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {body}")
            ch.basic_publish(exchange="dlx", routing_key=RABBITMQ_DLQ, body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    return callback


def main():
    try:
        logger.info("Initializing services...")
        s3_client, = (initialize_s3(),)
        rmq_conn, rmq_channel = initialize_rabbitmq()
        batch_buffer = deque(maxlen=BATCH_SIZE * 2)

        logger.info("Starting consumer...")
        rmq_channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=message_callback(s3_client, batch_buffer),
            consumer_tag="log_aggregator"
        )
        rmq_channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.critical(f'Fatal Error: {e}')
    finally:
        if 'rmq_conn' in locals():
            rmq_conn.close()
        logger.info("Service stopped")


if __name__ == "__main__":
    main()
