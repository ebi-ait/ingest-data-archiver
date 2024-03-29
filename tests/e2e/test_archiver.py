import os
import sys
import s3fs
import uuid
import unittest
from unittest.mock import Mock, patch
import tempfile
import logging
import random
from ftplib import FTP
from data.archiver.archiver import Archiver
from data.archiver.aws_s3_client import AwsS3
from data.archiver.dataclass import DataArchiverRequest
from data.archiver.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, ENA_FTP_DIR, ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD
from data.archiver.ftp_uploader import FtpUploader
from data.archiver.ingest_api import Ingest

TEST_BUCKET = "org-hca-data-archive-upload-dev"

# TODO move this to the integration tests suite as not all dep components are mocked.
class TestArchiver(unittest.TestCase):

    def setUp(self):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

        self.sub_uuid = str(uuid.uuid4())
        self.logger.info(f'Test Archiver sub_uuid {self.sub_uuid }')
        
        self.logger.info(f'Initiating S3 and FTP clients for test')
        self.s3 = s3fs.S3FileSystem(anon=False, key=AWS_ACCESS_KEY, secret=AWS_SECRET_KEY)
        self.ftp = FTP(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD)

        self.logger.info(f'Generate temp file ')
        file_size = random.randint(1024, 1024*1024) # in bytes (between 1Kb and 1Mb)
        self.tmp_file = tempfile.NamedTemporaryFile()
        self.tmp_file.write(os.urandom(file_size))
        self.file_uuid = str(uuid.uuid4())
        self.file_name = os.path.basename(self.tmp_file.name)
        self.logger.info(f'Temp file: {self.tmp_file.name} {file_size} bytes')

        self.s3_file = f'{TEST_BUCKET}/{self.sub_uuid}/{self.file_name}'

        self.logger.info(f'Saving temp file to S3 {TEST_BUCKET}/{self.sub_uuid}/{self.file_name}')
        self.s3.put(self.tmp_file.name, self.s3_file)

        self.logger.info(f'Changing to FTP destination dir')
        FtpUploader.chdir(self.ftp, ENA_FTP_DIR)
        FtpUploader.chdir(self.ftp, self.sub_uuid)
        super().setUp()

    def test_archive(self):
        self.logger.info(f'Mocking ingest service as Ingest will not contain test submission')
        with patch('data.archiver.archiver.Ingest') as mock:
            mock_ingest = mock.return_value
            mock_ingest.get_sequence_files.return_value = [{"uuid": self.file_uuid, "file_name": self.file_name, "cloud_url": f's3://{self.s3_file}'}]

            self.logger.info(f'Starting data archiver ')
            request = {"sub_uuid": f"{self.sub_uuid}", "files": [ f"{self.file_uuid}" ]}
            req = DataArchiverRequest(**request)
            archiver = Archiver(mock_ingest, AwsS3())
            archiver.start(req)
            archiver.close()

        self.logger.info(f'Finish. Checking expected files in FTP location.')
        assert FtpUploader.file_exists(self.ftp, f'{self.file_name}.gz') == True
        assert FtpUploader.file_exists(self.ftp, f'{self.file_name}.gz.md5') == True

    def tearDown(self):
        self.logger.info('Local clean up')
        self.tmp_file.close()
        self.logger.info('S3 clean up')
        self.s3.rm(self.s3_file)
        self.logger.info('FTP clean up')
        self.ftp.delete(f'{self.file_name}.gz')
        self.ftp.delete(f'{self.file_name}.gz.md5')
        self.ftp.cwd("..")
        self.ftp.rmd(self.sub_uuid)
        self.ftp.close()


if __name__ == '__main__':
    unittest.main()