import os
import boto3
from file_transfer import FileTransfer, TransferProgress, transfer

S3_REGION = os.getenv('INGEST_S3_REGION')
BUCKET_NAME = os.getenv('INGEST_S3_BUCKET')


class AwsS3:

    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def new_session(self):
        return boto3.Session(region_name=S3_REGION,
                             aws_access_key_id=self.access_key,
                             aws_secret_access_key=self.secret_key)

    def get_files(self, submission_uuid):

        bucket = self.new_session().resource('s3').Bucket(BUCKET_NAME)

        fs = []
        for obj in bucket.objects.filter(Prefix=submission_uuid):
            # skip the top-level directory
            if obj.key == submission_uuid:
                continue
            fs.append(FileTransfer(path=os.getcwd(), key=obj.key, size=obj.size))

        def download(idx):
            try:
                file = fs[idx].key
                os.makedirs(os.path.dirname(file), exist_ok=True)

                s3 = self.new_session().resource('s3') # session not thread-safe, so requires new session
                s3.Bucket(BUCKET_NAME).download_file(file, file, Callback=TransferProgress(fs[idx]))

                # if file size is 0, callback will likely never be called
                # and complete will not change to True
                # hack
                if fs[idx].size == 0:
                    fs[idx].status = 'Empty file.'
                    fs[idx].complete = True
                    fs[idx].successful = True

            except Exception as thread_ex:
                print(thread_ex) 
                print()
                if 'Forbidden' in str(thread_ex) or 'AccessDenied' in str(thread_ex):
                    fs[idx].status = 'Access denied.'
                else:
                    fs[idx].status = 'Download failed.'
                fs[idx].complete = True
                fs[idx].successful = False

        print('Downloading...')

        transfer(download, fs)

        files = [f.key for f in fs if f.successful]
        print(f'{len(files)}/{len(fs)} files downloaded.')

        return files
