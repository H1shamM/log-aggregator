import json
from unittest.mock import patch, MagicMock

from src.producer import producer


@patch("src.producer.producer.pika.BlockingConnection")
def test_producer_sends_10_messages(mock_conn):
    # setup channel stub
    mock_channel = MagicMock()
    mock_conn.return_value.channel.return_value = mock_channel

    # shorten loop for test
    with patch("builtins.print") as fake_print:
        producer.main()
        # Expect 10 publish calls
        assert mock_channel.basic_publish.call_count == 10

        # check print was called
        assert fake_print.call_count == 10

        # inspect one call for correct queue and JSON body
        call_args, call_kwargs = mock_channel.basic_publish.call_args_list[0]
        assert call_kwargs["routing_key"] == producer.RABBITMQ_QUEUE
        body = json.loads(call_kwargs["body"])
        assert "timestamp" in body and "source" in body
