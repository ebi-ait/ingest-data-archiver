import os
from dataclasses import dataclass


AWS_S3_REGION = os.getenv('INGEST_S3_REGION')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_ACCESS_KEY_SECRET')

ENA_FTP_HOST = os.getenv('ENA_FTP_HOST')
ENA_FTP_DIR = os.getenv('ENA_FTP_DIR')
ENA_WEBIN_USER = os.getenv('ENA_WEBIN_USERNAME')
ENA_WEBIN_PWD = os.getenv('ENA_WEBIN_PASSWORD')

INGEST_API = os.getenv('INGEST_API')
if not INGEST_API.endswith("/"):
    INGEST_API += '/'

# messaging
RABBIT_HOST = os.getenv('RABBIT_HOST')
RABBIT_PORT = os.getenv('RABBIT_PORT')

SINGLE_THREADED = os.getenv('SINGLE_THREADED')

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
