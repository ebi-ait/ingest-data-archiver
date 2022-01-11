import os
from data.archiver.aws_s3_client import AwsS3
from data.archiver.ftp_uploader import FtpUploader
from data.archiver.utils import compress, md5
from data.archiver.dataclass import DataArchiverRequest, DataArchiverResult
from data.archiver.ingest_api import Ingest

class Archiver:

    def __init__(self):
        self.ingest_api = Ingest()
        self.aws_client = AwsS3()
        self.ftp = None

    def start(self, req: DataArchiverRequest):

        if not req.files:
            # get all sequence files for submission
            req.files = self.ingest_api.get_sequence_files(req.sub_uuid)
        
        print(req)
        # TODO logic here to decide between local copy or stream archive/upload to ena
        self.archive_files_via_localcopy(req.sub_uuid)

    def archive_files_via_localcopy(self, uuid):
        res = DataArchiverResult()

        # step 1 get a list of files to be uploaded
        files = self.aws_client.get_files(uuid)
        
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
        self.ftp = FtpUploader(uuid)
        self.ftp.upload(compressed_files + checksums)

    def close(self):
        self.ingest_api.close()
        #self.aws_client.close()
        if self.ftp:
            self.ftp.close()
