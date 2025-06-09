import time
import pytest
from collections import deque

from src.consumer.consumer import (
    initialize_rabbitmq,
    message_callback,
    RABBITMQ_QUEUE, RABBITMQ_DLQ,
)


@pytest.mark.integration
def test_publish_consume_fallback(tmp_path, monkeypatch):
    """
   End-to-end integration:
      • Publish invalid JSON to the primary queue.
      • Consume via message_callback.
      • Verify it shows up in the DLQ queue.
    """
    conn, chan = initialize_rabbitmq()
    chan.queue_purge(queue=RABBITMQ_QUEUE)
    chan.queue_purge(queue=RABBITMQ_DLQ)

    bad_body = b'{"not": "valid"'
    chan.basic_publish(exchange="", routing_key=RABBITMQ_QUEUE, body=bad_body)

    method, props, payload = chan.basic_get(queue=RABBITMQ_QUEUE, auto_ack=False)
    assert payload == bad_body, "Message payload should match what was published"

    buffer = deque()
    cb = message_callback(None, buffer)
    cb(chan, method, props, payload)

    time.sleep(0.1)

    dlq_method, dlq_props, dlq_payload = chan.basic_get(
        queue=RABBITMQ_DLQ, auto_ack=True
    )
    assert dlq_payload == bad_body, "Invalid JSON should be routed to the DLQ"

    conn.close()
