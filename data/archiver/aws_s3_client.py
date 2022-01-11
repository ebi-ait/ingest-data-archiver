import os
import boto3
import logging
from data.archiver.dataclass import DataArchiverRequest, DataArchiverResult
from data.archiver.file_transfer import FileTransfer, TransferProgress, transfer
from data.archiver.config import AWS_S3_REGION, AWS_S3_BUCKET, AWS_ACCESS_KEY, AWS_SECRET_KEY


class AwsS3:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def get_files(self, req: DataArchiverRequest, res: DataArchiverResult):
        bucket = self.new_session().resource('s3').Bucket(AWS_S3_BUCKET)

        fs = []
        for obj in bucket.objects.filter(Prefix=req.sub_uuid):
            # skip the top-level directory
            if obj.key == req.sub_uuid:
                continue
            f_name = obj.key.split('/')[1]
            if f_name in req.files:
                fs.append(FileTransfer(path=os.getcwd(), key=obj.key, size=obj.size))


        def download(idx):
            try:
                file = fs[idx].key
                os.makedirs(os.path.dirname(file), exist_ok=True)

                s3 = self.new_session().resource('s3') # session not thread-safe, so requires new session
                s3.Bucket(AWS_S3_BUCKET).download_file(file, file, Callback=TransferProgress(fs[idx]))

                # if file size is 0, callback will likely never be called
                # and complete will not change to True
                # hack
                if fs[idx].size == 0:
                    fs[idx].status = 'Empty file.'
                    fs[idx].complete = True
                    fs[idx].successful = True

            except Exception as thread_ex:
                
                if 'Forbidden' in str(thread_ex) or 'AccessDenied' in str(thread_ex):
                    fs[idx].status = 'Access denied.'
                else:
                    fs[idx].status = 'Download failed.'
                fs[idx].complete = True
                fs[idx].successful = False

        self.logger.info('Downloading...')
        transfer(download, fs)

        files = [f.key for f in fs if f.successful]
        self.logger.info(f'{len(files)}/{len(fs)} files downloaded.')

        for f in fs:
            f_name = f.key.split('/')[1]
            res.update_file_result(f_name, None, f.successful, None if f.successful else f.status)

    def new_session(self):
        return boto3.Session(region_name=AWS_S3_REGION,
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY)