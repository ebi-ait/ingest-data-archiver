import logging
import json
from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Consumer, Message, Queue, Exchange
from typing import Type, List
from concurrent.futures import ThreadPoolExecutor

from data.archiver.archiver import Archiver
from data.archiver.dataclass import AmqpConnConfig, QueueConfig, DataArchiverRequest


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
    
    def data_archiver_message_handler(self, body: str, msg: Message):
        return self.executor.submit(lambda: self._data_archiver_message_handler(body, msg))

    def _data_archiver_message_handler(self, body: str, msg: Message):
        try:
            dict = json.loads(body)
            req = DataArchiverRequest.from_dict(dict)
            self.logger.info(f'Received data archiving request for submission uuid {req.sub_uuid}')

            result = Archiver().start(req)
            self.producer.publish(result.to_dict(),
                exchange=self.pub_queue_config.exchange,
                routing_key=self.pub_queue_config.routing_key,
                retry=self.pub_queue_config.retry,
                retry_policy=self.pub_queue_config.retry_policy)
            
            self.logger.info(f'Archived data for submission uuid {req.sub_uuid}')
            

        except ValueError as e:
            self.logger.info(f'Invalid JSON request: {body}')
            #self.logger.exception(e)
        except Exception as e:
            self.logger.info(f'Data archiving request failed: {body}')
            #self.logger.exception(e)

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
