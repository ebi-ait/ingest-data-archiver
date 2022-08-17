import gzip
import hashlib
import logging
import shutil
import tempfile
from ftplib import FTP
from io import BytesIO
from multiprocessing.pool import ThreadPool

import s3fs
from tqdm import tqdm

from data.archiver.aws_s3_client import S3Url
from data.archiver.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, ENA_FTP_HOST, ENA_WEBIN_USER, \
    ENA_WEBIN_PWD, SINGLE_THREADED
from data.archiver.dataclass import DataArchiverResult, FileResult
from data.archiver.ftp_uploader import FtpUploader

MAX_IN_MEM_FILE_COMPRESSION = 1024 * 1024 * 500  # 500M
BLOCKSIZE = 8192


class S3FTPStreamer:

    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=False, key=AWS_ACCESS_KEY, secret=AWS_SECRET_KEY)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @staticmethod
    def new_ftpcli():
        return FTP(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD, timeout=60 * 60 * 2)  # 2h

    def md5(self, file):
        """
        calculate md5 by streaming file without saving locally.
        """
        hash_md5 = hashlib.md5()
        with self.s3.open(file.cloud_url, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def stream(self, ftp, fin, fout, cb):
        """
        stream s3 file to ftp 
        """
        with self.s3.open(fin, 'rb') as f:
            ftp.storbinary(f'STOR {fout}', f, callback=cb)

    def s3_ftp_stream(self, file, callback):

        s3url = S3Url(file.cloud_url)
        env = s3url.bucket.split('-')[-1]
        compressed = self.is_compressed(file.cloud_url)

        file.ena_upload_path = f"{env}/{s3url.uuid}/"

        with S3FTPStreamer.new_ftpcli() as ftp:
            FtpUploader.chdir(ftp, env)
            FtpUploader.chdir(ftp, s3url.uuid)

            if FtpUploader.file_exists(ftp, file.file_name) and FtpUploader.file_size(ftp,
                                                                                      file.file_name) == file.size:
                self.logger.info(
                    f'Skipping {file.file_name} ({file.size} bytes). File exists in ENA FTP.')
                file.error = 'File already exists in ENA upload area.'
                file.success = False
            else:

                if compressed:
                    self.logger.info(f'Streaming {file.file_name} ({file.size} bytes) to FTP.')
                    self.stream_with_md5(ftp, file, lambda str: callback(len(str)))
                else:
                    self.logger.info(
                        f'Compressing {file.file_name} ({file.size} bytes) / streaming {file.file_name}.gz to FTP.')
                    self.stream_with_compression_and_md5(ftp, file, lambda str: callback(len(str)))
                self.logger.info(f'Finish streaming {file.file_name}.')

    def stream_with_md5(self, ftp, file, cb):
        hash_md5 = hashlib.md5()
        ftp.voidcmd('TYPE I')
        with self.s3.open(file.cloud_url, 'rb') as fp, ftp.transfercmd(f'STOR {file.file_name}',
                                                                       None) as conn:
            while 1:
                buf = fp.read(BLOCKSIZE)
                if not buf:
                    break
                hash_md5.update(buf)
                conn.sendall(buf)
                cb(buf)
        ftp.voidresp()
        file.md5 = hash_md5.hexdigest()
        file.ena_upload_path += file.file_name
        ftp.storbinary(f'STOR {file.file_name}.md5', BytesIO(bytes(file.md5, 'utf-8')))

    def stream_with_compression_and_md5(self, ftp, file, cb):
        fout = f'{file.file_name}.gz'
        hash_md5 = hashlib.md5()
        ftp.voidcmd('TYPE I')
        with self.s3.open(file.cloud_url, 'rb') as fp, ftp.transfercmd(f'STOR {fout}',
                                                                       None) as conn:
            while 1:
                buf = fp.read(BLOCKSIZE)
                if not buf:
                    break
                cbuf = gzip.compress(buf)
                hash_md5.update(cbuf)
                conn.sendall(cbuf)
                cb(buf)
        ftp.voidresp()
        file.md5 = hash_md5.hexdigest()
        file.compressed = True
        file.ena_upload_path += fout
        ftp.storbinary(f'STOR {fout}.md5', BytesIO(bytes(file.md5, 'utf-8')))

    def stream_with_compression_and_md5_using_tmpfile(self, ftp, fin, fout, cb):
        with self.s3.open(fin, 'rb') as f:
            compressed_fp = tempfile.SpooledTemporaryFile()  # BytesIO() #tempfile.NamedTemporaryFile()
            hash_md5 = hashlib.md5()
            with gzip.GzipFile(fileobj=compressed_fp, mode='wb') as gz:
                shutil.copyfileobj(f, gz)
            compressed_fp.seek(0)
            ftp.storbinary(f'STOR {fout}.gz', compressed_fp, callback=cb)
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
            md5 = hash_md5.hexdigest()
            ftp.storbinary(f'STOR {fout}.gz.md5', BytesIO(bytes(md5, 'utf-8')))
            compressed_fp.close()

    def is_compressed(self, s3url):
        return s3url.endswith('.gz') and self.s3.read_block(s3url, 0, 2) == b'\x1f\x8b'

    def start(self, res: DataArchiverResult):
        total_size = 0
        prefix_paths = {}
        for file in res.files:
            if not file.success:
                continue

            if self.s3.exists(file.cloud_url):
                url = S3Url(file.cloud_url)
                environment = url.bucket.split('-')[-1]
                prefix_paths.setdefault(environment, {})[url.uuid] = f'{environment}/{url.uuid}'

                size = self.s3.size(file.cloud_url)
                total_size += size
                file.size = size
            else:
                file.error = 'File not found in S3.'
                file.success = False

        total_files = len(res.files)
        num_files = sum(map(lambda f: f.success, res.files))
        if total_files != num_files:
            self.logger.info(f'{total_files - num_files} files not found.')

        # Create each environment/uuid directory once for all files
        # rather than have all files try to create it
        for environment, uuids in prefix_paths.items():
            with S3FTPStreamer.new_ftpcli() as ftp:
                FtpUploader.chdir(ftp, environment)
                for uuid in uuids.keys():
                    FtpUploader.mk_dir(ftp, uuid)

        if SINGLE_THREADED:
            self.single_threaded_copy(res.files, num_files, total_size)
        else:
            self.multi_threaded_copy(res.files, num_files, total_size)

    def multi_threaded_copy(self, files, num_files, total_size):
        self.logger.info('Streaming Multi Threaded...')
        pbar = self.get_progressbar(num_files, total_size)
        pool = ThreadPool()  # cpu_count() DEFAULT_THREAD_COUNT=25

        def cp(file_to_copy: FileResult):
            if not file_to_copy.success:
                return
            try:
                self.s3_ftp_stream(file_to_copy, pbar.update)
            except Exception as ex:
                logging.error(f'Failed to copy file via stream: {file_to_copy} error: {ex}')
                file_to_copy.error = str(ex)
                file_to_copy.success = False

        pool.map_async(cp, files)
        pool.close()
        pool.join()
        pbar.close()

    def single_threaded_copy(self, files, num_files, total_size):
        self.logger.info('Streaming Single Threaded...')
        with self.get_progressbar(num_files, total_size) as progress_bar:
            for file in files:
                self.copy_file(file, progress_bar)

    def copy_file(self, file: FileResult, progress_bar: tqdm):
        if not file.success:
            return
        try:
            self.s3_ftp_stream(file, progress_bar.update)
        except Exception as ex:
            logging.error(f'Failed to copy file via stream: {file} error: {ex}')
            file.error = str(ex)
            file.success = False

    @staticmethod
    def get_progressbar(num_files, total_size):
        return tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            desc=f'{num_files} files',
            mininterval=2
        )

    def close(self):
        s3fs.S3FileSystem.close_session(None, self.s3)
