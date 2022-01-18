import os
import logging
from data.archiver.aws_s3_client import AwsS3
from data.archiver.ftp_uploader import FtpUploader
from data.archiver.utils import compress, md5
from data.archiver.dataclass import DataArchiverRequest, DataArchiverResult, FileResult
from data.archiver.ingest_api import Ingest


class Archiver:

    def __init__(self):
        self.ingest_api = Ingest()
        self.aws_client = AwsS3()
        self.ftp = None
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def start(self, req: DataArchiverRequest):
        self.logger.info(req)
        self.logger.info(f'Getting sequence files for submission {req.sub_uuid} from INGEST_API')
        sequence_files = self.ingest_api.get_sequence_files(req.sub_uuid)
        self.logger.info(sequence_files)

        if not sequence_files:
            res = DataArchiverResult(req.sub_uuid, success=False, error='No sequence files in submission')
            self.logger.info(res)
            return res

        if not req.files:
            # archive all files
            res_files = list(map(lambda f: FileResult(f["file_name"], f["cloud_url"]), sequence_files))
        else:
            staging_area = self.ingest_api.get_staging_area()
            res_files = list(map(lambda f: FileResult(f, staging_area + f), req.files))
        
        res = DataArchiverResult(req.sub_uuid, files=res_files)
        self.logger.info(res)

        try:
            # TODO logic here to decide between local copy or stream archive/upload to ena
            if req.stream:
                return
            else:
                result = self.archive_files_via_localcopy(res)
                self.logger.info(result)
                return result

        except Exception as ex:
            print(ex)

    def archive_files_via_localcopy(self, res: DataArchiverResult):

        self.logger.info(f'# step 1 download sequence files from S3')
        self.aws_client.get_files(res)

        self.logger.info(f'# step 2 compress files using gz')
        def try_compress(f):
            if (f.file_name.endswith('.gz')):
                self.logger.info(f'Skipping {f.file_name}')
            else:
                self.logger.info(f'Compressing {f.file_name}')
                try:
                    compress(f)
                    f.file_name = f'{f.file_name}.gz'
                except:
                    f.success = False
                    f.error = 'Compression failed'

        for f in res.files:
            if f.success:
                try_compress(f)

        
        self.logger.info(f'# step 3 calculate checksums of compressed files (and create .md5 files)')
        checksum_files = []
        def calc_checksum(f):
            self.logger.info(f'Generating checksum {f.file_name}')
            md5_file = f'{res.sub_uuid}/{f.file_name}.md5'
            with open(md5_file,'a') as f1:
                md5_str = md5(f'{res.sub_uuid}/{f.file_name}')
                f1.write(md5_str)
                f.md5 = md5_str
            checksum_files.append(md5_file) 

        for f in res.files:
            if f.success:
                calc_checksum(f)

        self.logger.info(f'# step 4 upload files to ENA upload area')
        
        self.ftp = FtpUploader(res)
        self.ftp.upload()

        for f in res.files:
            res.success = res.success and f.success

        return res


    def close(self):
        self.ingest_api.close()
        #self.aws_client.close()
        if self.ftp:
            self.ftp.close()
