import os
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
import logging
from urllib.parse import urlparse
from tqdm import tqdm
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from data.archiver.dataclass import DataArchiverResult
from data.archiver.config import AWS_S3_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY


class S3Url:

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)
    
    @property
    def bucket(self):
        return self._parsed.netloc
    
    @property
    def key(self):
        return self._parsed.path.lstrip('/')

    @property
    def uuid(self):
        return self.key[:36]


class S3FileDownload:

    def __init__(self, cloud_url, success=False, error=None):
        self.cloud_url = cloud_url
        self.success = success
        self.error = error


class AwsS3:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.s3_cli = boto3.Session(region_name=AWS_S3_REGION,
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY).resource('s3', config=Config(s3={'use_accelerate_endpoint': True})).meta.client

    def new_session(self):
        return 

    def file_exists(self, s3url):
        response = self.s3_cli.list_objects_v2(Bucket=s3url.bucket, Prefix=s3url.key)

        for obj in response.get('Contents', []):
            if obj['Key'] == s3url.key:
                return True, obj['Size']
        return False, 0

    def get_files(self, res: DataArchiverResult):

        total_size = 0
        for file in res.files:

            exists, size = self.file_exists(S3Url(file.cloud_url))
            if exists:
                total_size += size
                file.size = size
            else:
                file.error = 'File not found.'
                file.success = False
        
        def download(file):
            if not file.success:
                return
            try:
                s3url = S3Url(file.cloud_url)
                bucket = s3url.bucket
                key = s3url.key

                os.makedirs(os.path.dirname(key), exist_ok=True)

                self.s3_cli.download_file(bucket, key, key, Callback=pbar.update, Config=get_transfer_config(file.size))

            except Exception as ex:
                file.error = str(ex)
                file.success = False
                pass

        self.logger.info('Downloading...')

        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=num_files(res.files))
        pool = ThreadPool() # cpu_count() DEFAULT_THREAD_COUNT=25
        pool.map_async(download, res.files)
        pool.close()
        pool.join()
        pbar.close()


def num_files(ls):
    l = len(ls)
    return f'{l} file{"s" if l > 1 else ""}'


# this is based on the dcplib s3_multipart module
KB = 1024
MB = KB * KB
MIN_CHUNK_SIZE = 64 * MB
MULTIPART_THRESHOLD = MIN_CHUNK_SIZE + 1
MAX_MULTIPART_COUNT = 10000 # s3 imposed


def get_transfer_config(filesize):
    return TransferConfig(multipart_threshold=MULTIPART_THRESHOLD, 
                          multipart_chunksize=get_chunk_size(filesize))


def get_chunk_size(filesize):
    if filesize <= MAX_MULTIPART_COUNT * MIN_CHUNK_SIZE:
        return MIN_CHUNK_SIZE
    else:
        div = filesize // MAX_MULTIPART_COUNT
        if div * MAX_MULTIPART_COUNT < filesize:
            div += 1
        return ((div + MB - 1) // MB) * MB
