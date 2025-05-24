import json
import os
from collections import deque
from unittest.mock import MagicMock

import pytest

from src.consumer import consumer


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    # point FALLBACK_PATH to a temp file
    fake_fallback = tmp_path / "failed.ndjson"
    monkeypatch.setenv("FALLBACK_PATH", str(fake_fallback))
    # minimal other envs
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    yield


class DummyChannel:
    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body):
        self.published.append((exchange, routing_key, body))

    def basic_ack(self, delivery_tag):
        pass

    def basic_nack(self, delivery_tag, requeue):
        pass


def test_process_single_log_success(tmp_path, monkeypatch):
    # mock boto3 client
    mock_s3 = MagicMock()
    log = {"source": "app1", "event": "test"}
    # should return True without writing fallback
    assert consumer.process_single_log(mock_s3, log) is True


def test_process_single_log_failure_writes_fallback(tmp_path, monkeypatch):
    # boto3.put_object always raises
    class BadClient:
        def put_object(self, **kwargs):
            raise RuntimeError("boom")

    bad = BadClient()
    log = {"source": "app2", "event": "fail"}
    # call and assert False
    result = consumer.process_single_log(bad, log)
    assert result is False

    # fallback file should have one line
    path = os.getenv("FALLBACK_PATH")
    content = open(path).read().strip().splitlines()
    data = json.loads(content[-1])
    assert data["source"] == "app2"
    assert "_error" in data


def test_json_decode_routes_to_dlq(monkeypatch):
    # simulate callback with bad JSON
    channel = DummyChannel()
    s3 = MagicMock()
    buf = deque(maxlen=10)
    cb = consumer.message_callback(s3, buf)

    # call with invalid JSON
    cb(channel, MagicMock(delivery_tag=1), None, b"not-a-json")
    assert channel.published, "Should publish to DLQ"
    exch, rk, body = channel.published[0]
    assert rk == os.getenv("RABBITMQ_DLQ", "log_dlq")
