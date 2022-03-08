import logging
from data.archiver.aws_s3_client import AwsS3
from data.archiver.ftp_uploader import FtpUploader
from data.archiver.ingest_api import Ingest
from data.archiver.stream import S3FTPStreamer
from data.archiver.utils import compress, md5
from data.archiver.dataclass import DataArchiverRequest, DataArchiverResult, FileResult


class Archiver:

    def __init__(self, ingest_cli: Ingest, aws_cli: AwsS3):
        self.ingest_cli = ingest_cli
        self.aws_cli = aws_cli
        self.ftp = None
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def start(self, req: DataArchiverRequest):
        self.logger.info(req)
        self.logger.info(f'Getting sequence files for submission {req.sub_uuid} from Ingest.')
        sequence_files = self.ingest_cli.get_sequence_files(req.sub_uuid)
        self.logger.info(sequence_files)

        if not sequence_files:
            res = DataArchiverResult(req.sub_uuid, success=False, error='No sequence files in submission.')
            self.logger.info(res)
            return res

        if not req.files:
            # archive all files
            res_files = list(map(FileResult.from_file, sequence_files))
        else:
            def file_result(uuid):
                for f in sequence_files:
                    if f["uuid"] == uuid:
                        return FileResult(f["uuid"], f["file_name"], f["cloud_url"])
                return FileResult.not_found_error(uuid)

            res_files = list(map(file_result, req.files))
        
        res = DataArchiverResult(req.sub_uuid, files=res_files)
        self.logger.info(res)

        try:
            # TODO logic here to decide between local copy or stream archive/upload to ena
            if req.stream:
                result = self.archive_files_via_streaming(res)
            else:
                result = self.archive_files_via_localcopy(res)
            self.logger.info(result)
            return result

        except Exception as ex:
            self.logger.error(str(ex))

    def archive_files_via_localcopy(self, res: DataArchiverResult):

        self.logger.info(f'# step 1 download sequence files from S3')
        self.aws_cli.get_files(res)

        self.logger.info(f'# step 2 compress files using gz')
        def try_compress(file):
            if (file.file_name.endswith('.gz')):
                self.logger.info(f'Skipping {file.file_name}')
            else:
                self.logger.info(f'Compressing {file.file_name}')
                try:
                    compress(file)
                    file.file_name = f'{file.file_name}.gz'
                    file.compressed = True
                except:
                    file.success = False
                    file.error = 'Compression failed'

        for file in res.files:
            if file.success:
                try_compress(file)

        
        self.logger.info(f'# step 3 calculate checksums of compressed files (and create .md5 files)')
        checksum_files = []
        def calc_checksum(file):
            self.logger.info(f'Generating checksum {file.file_name}')
            md5_file = f'{res.sub_uuid}/{file.file_name}.md5'
            with open(md5_file,'a') as f1:
                md5_str = md5(f'{res.sub_uuid}/{file.file_name}')
                f1.write(md5_str)
                file.md5 = md5_str
            checksum_files.append(md5_file) 

        for file in res.files:
            if file.success:
                calc_checksum(file)

        self.logger.info(f'# step 4 upload files to ENA upload area')
        
        self.ftp = FtpUploader(res)
        self.ftp.upload()

        res.update_status()

        return res

    def archive_files_via_streaming(self, res: DataArchiverResult):

        self.logger.info(f'# stream sequence files from S3 to FTP, gzipping and calculating checksums on-the-fly')
        
        S3FTPStreamer().start(res)
        res.update_status()

        return res

    def close(self):
        self.ingest_cli.close()
        if self.ftp:
            self.ftp.close()
