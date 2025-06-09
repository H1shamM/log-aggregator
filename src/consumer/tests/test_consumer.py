import json
import os
from collections import deque
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.client import BaseClient

from src.consumer import consumer


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    # point FALLBACK_PATH to a temp file
    fake_fallback = tmp_path / "failed.ndjson"
    monkeypatch.setenv("FALLBACK_PATH", str(fake_fallback))
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    yield


class DummyChannel:
    def __init__(self):
        self.published = []

    def basic_publish(self, exchange, routing_key, body):
        self.published.append({
            'exchange': exchange,
            'routing_key': routing_key,
            'body': body
        })

    def basic_ack(self, delivery_tag):
        pass

    def basic_nack(self, delivery_tag, requeue):
        pass


def test_initialize_s3_success(monkeypatch):
    client = consumer.initialize_s3()
    assert isinstance(client, BaseClient)


def test_initialize_s3_failure(monkeypatch):
    monkeypatch.setattr(
        boto3,
        "client",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        consumer.initialize_s3()


def test_process_single_log_success(tmp_path, monkeypatch):
    mock_s3 = MagicMock()
    log = {"source": "app1", "event": "test"}
    assert consumer.process_single_log(mock_s3, log) is True


def test_process_single_log_no_credentials(tmp_path, monkeypatch):
    from botocore.exceptions import NoCredentialsError

    class NoCredsClient:
        def put_object(self):
            raise NoCredentialsError()

    result = consumer.process_single_log(NoCredsClient(), {"foo": "bar"})
    assert result is False

    content = open(os.getenv("FALLBACK_PATH")).read().splitlines()
    data = json.loads(content[-1])
    assert data["foo"] == "bar"
    assert "_error" in data


def test_process_single_log_failure_writes_fallback(tmp_path, monkeypatch):
    class BadClient:
        def put_object(self, **kwargs):
            raise RuntimeError("boom")

    bad = BadClient()
    log = {"source": "app2", "event": "fail"}
    result = consumer.process_single_log(bad, log)
    assert result is False

    path = os.getenv("FALLBACK_PATH")
    content = open(path).read().strip().splitlines()
    data = json.loads(content[-1])
    assert data["source"] == "app2"
    assert "_error" in data


def test_message_callback_success_ack(monkeypatch):
    monkeypatch.setattr(consumer, "process_single_log", lambda s3, log: True)
    channel = MagicMock()
    method = MagicMock(delivery_tag=42)
    buf = deque()
    cb = consumer.message_callback(MagicMock(), buf)

    cb(channel, method, None, b'{"x": 1}')

    channel.basic_ack.assert_called_once_with(delivery_tag=42)
    channel.basic_publish.assert_not_called()


def test_main_declares_and_consumes(monkeypatch):
    import src.consumer.consumer as mod

    fake_s3 = object()
    monkeypatch.setattr(mod, "initialize_s3", lambda: fake_s3)

    calls = {}

    class FakeChan:
        def basic_consume(self, *args, **kwargs):
            calls['consume'] = {'args': args, **kwargs}

        def start_consuming(self):
            calls['start'] = True

    class FakeConn:
        def __init__(self, chan):
            self._chan = chan
            calls['closed'] = False

        def channel(self):
            return self._chan

        def close(self):
            # record that close() was called
            calls['closed'] = True

    fake_chan = FakeChan()
    fake_conn = FakeConn(fake_chan)

    monkeypatch.setattr(mod, "initialize_rabbitmq", lambda: (fake_conn, fake_chan))

    mod.main()

    assert 'consume' in calls, "main() must call rmq_channel.basic_consume"
    args = calls['consume']

    assert args.get('queue') == mod.RABBITMQ_QUEUE
    assert args.get('on_message_callback') is not None
    assert args.get('consumer_tag') == "log_aggregator"
    assert calls.get('start') is True, "main() must call start_consuming()"


def test_json_decode_routes_to_dlq(monkeypatch):
    channel = DummyChannel()
    s3 = MagicMock()
    buf = deque(maxlen=10)
    cb = consumer.message_callback(s3, buf)

    cb(channel, MagicMock(delivery_tag=1), None, b"not-a-json")
    assert channel.published, "Should publish to DLQ"
    exch, rk, body = channel.published[0].values()
    assert rk == os.getenv("RABBITMQ_DLQ", "log_dlq")
