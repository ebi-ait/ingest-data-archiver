#!/usr/bin/env python
import logging
import sys
from threading import Thread

from data.archiver.config import RABBIT_HOST, RABBIT_PORT, EXCHANGE, EXCHANGE_TYPE, SUBSCRIBE_QUEUE, SUBSCRIBE_ROUTING_KEY, PUBLISH_ROUTING_KEY, RETRY_POLICY
from data.archiver.listener import Listener
from data.archiver.dataclass import AmqpConnConfig, QueueConfig


def setup() -> Thread:
    amqp_conn_config = AmqpConnConfig(RABBIT_HOST, RABBIT_PORT)

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

    setup()
    logging.getLogger('ingest-data-archiver').info("Ingest data archiver listening...")
