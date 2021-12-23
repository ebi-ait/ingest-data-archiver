#!/usr/bin/env python
import logging
import os
import sys
from threading import Thread

from config import AmqpConnConfig, QueueConfig
from listener import Listener

DEFAULT_RABBIT_URL = os.path.expandvars(
    os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))

EXCHANGE = 'ingest.data.archiver.exchange'
EXCHANGE_TYPE = 'topic'

SUBSCRIBE_QUEUE = 'ingest.data.archiver.request.queue'
SUBSCRIBE_ROUTING_KEY = 'ingest.data.archiver.request'

PUBLISH_ROUTING_KEY = 'ingest.data.archiver.complete'

RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}


def setup_data_archiver() -> Thread:
    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_access_key_secret = os.environ.get('AWS_ACCESS_KEY_SECRET', '')

    #ingest_client = IngestApi(ingest_api_url)


    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    sub_queue_config = QueueConfig(SUBSCRIBE_QUEUE, SUBSCRIBE_ROUTING_KEY, EXCHANGE, EXCHANGE_TYPE, False, None)
    pub_queue_config = QueueConfig(None, PUBLISH_ROUTING_KEY, EXCHANGE, EXCHANGE_TYPE, True, RETRY_POLICY)

    listener = Listener(amqp_conn_config, sub_queue_config, pub_queue_config)

    listener_process = Thread(target=lambda: listener.run())
    listener_process.start()

    return listener_process


if __name__ == '__main__':
    logging.getLogger('ingest-data-archiver').setLevel(logging.INFO)

    format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:' \
             '%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING,
                        format=format)

    setup_data_archiver()
