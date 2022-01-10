import os
from aws_s3_client import AwsS3
from ftp_uploader import FtpUploader
from utils import compress, md5
from config import AWS_ACCESS_KEY, AWS_SECRET_KEY
from listener import DataArchiverRequest
from ingest_api import Ingest

class Archiver:
    def __init__():
        pass

    def start(self, req: DataArchiverRequest):

        if not req.files:
            # get all sequence files for submission
            req.files = Ingest().get_sequence_files(req.sub_uuid)
        
        # TODO logic here to decide between local copy or stream archive/upload to ena
        self.archive_files_via_localcopy(req.sub_uuid)

    def archive_files_via_localcopy(self, uuid):

        # step 1 get a list of files to be uploaded
        files = AwsS3().get_files(uuid)
        
        # step 2 compress files using gz
        print('Compressing...')
        def maybe_compress(f):
            if (f.endswith('.gz')):
                print(f'Skipping {f}')
                return f
            else:
                print(f'Compressing {f}')
                compress(f)
                return f'{f}.gz'

        compressed_files = list(map(maybe_compress, files))
        
        # step 3 calculate checksums of compressed files (and create .md5 files)

        def calc_checksum(f):
            print(f'Generating checksum {f}')
            md5_file = f'{f}.md5'
            with open(md5_file,'a') as f1:
                f1.write(md5(f))
            return md5_file

        checksums = list(map(calc_checksum, compressed_files))

        # step 4 upload files to Webin upload area
        FtpUploader(uuid).upload(compressed_files + checksums)
