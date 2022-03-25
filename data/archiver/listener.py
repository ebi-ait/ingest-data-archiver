import logging
import json
from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Consumer, Message, Queue, Exchange
from typing import Type, List
from concurrent.futures import ThreadPoolExecutor

from data.archiver.archiver import Archiver
from data.archiver.aws_s3_client import AwsS3
from data.archiver.dataclass import AmqpConnConfig, DataArchiverResult, FileResult, QueueConfig, DataArchiverRequest
from data.archiver.ingest_api import Ingest


class _Listener(ConsumerProducerMixin):

    def __init__(self,
                 connection: Connection,
                 sub_queue_config: QueueConfig,
                 pub_queue_config: QueueConfig,
                 executor: ThreadPoolExecutor):
        self.connection = connection
        self.sub_queue_config = sub_queue_config
        self.pub_queue_config = pub_queue_config
        self.executor = executor

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def get_consumers(self, _consumer: Type[Consumer], channel) -> List[Consumer]:
        consumer = _consumer([_Listener.queue_from_config(self.sub_queue_config)],
                                        callbacks=[self.data_archiver_message_handler],
                                        prefetch_count=1)
        return [consumer]
    
    def data_archiver_message_handler(self, body: dict, msg: Message):
        return self.executor.submit(lambda: self._data_archiver_message_handler(body, msg))

    def _data_archiver_message_handler(self, body: dict, msg: Message):
        ingest_cli = Ingest()
        try:
            req = DataArchiverRequest.from_dict(body)
            self.logger.info(f'Received data archiving request for submission uuid {req.sub_uuid}')

            result = Archiver(ingest_cli, AwsS3()).start(req)            
            self.logger.info(f'Archived data for submission uuid {req.sub_uuid}')
            
        except Exception as e:
            error_msg = f'Data archiving request {body} failed: {str(e)}'
            self.logger.info(error_msg)
            result = DataArchiverResult(req.sub_uuid, success=False, error=error_msg)

        ingest_cli.patch_files(result.files)

        msg.ack()


    @staticmethod
    def queue_from_config(queue_config: QueueConfig) -> Queue:
        exchange = Exchange(queue_config.exchange, queue_config.exchange_type)
        return Queue(queue_config.name, exchange, queue_config.routing_key)


class Listener:

    def __init__(self,
                 amqp_conn_config: AmqpConnConfig,
                 sub_queue_config: QueueConfig,
                 pub_queue_config: QueueConfig):
        self.amqp_conn_config = amqp_conn_config
        self.sub_queue_config = sub_queue_config
        self.pub_queue_config = pub_queue_config

    def run(self):
        with Connection(self.amqp_conn_config.broker_url()) as conn:
            _listener = _Listener(conn, self.sub_queue_config, self.pub_queue_config, ThreadPoolExecutor())
            _listener.run()
