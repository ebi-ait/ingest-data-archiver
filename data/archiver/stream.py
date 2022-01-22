import s3fs
import hashlib
import gzip
import shutil
import logging
from ftplib import FTP
from io import BytesIO
import tempfile
from tqdm import tqdm
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from data.archiver.aws_s3_client import S3Url
from data.archiver.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD
from data.archiver.dataclass import DataArchiverRequest
from data.archiver.ftp_uploader import FtpUploader


MAX_IN_MEM_FILE_COMPRESSION = 1024*1024*500 #500M
BLOCKSIZE = 8192

class S3FTPStreamer:

    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=False, key=AWS_ACCESS_KEY, secret=AWS_SECRET_KEY)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @staticmethod
    def new_ftpcli():
        return FTP(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD, timeout=60*60*2) # 2h

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

    def s3_ftp_stream(self, file, pbar):

        s3url = S3Url(file.cloud_url)
        env = s3url.bucket.split('-')[-1]
        compressed = self.is_compressed(file.cloud_url)

        with S3FTPStreamer.new_ftpcli() as ftp: 
            FtpUploader.chdir(ftp, env)
            FtpUploader.chdir(ftp, s3url.uuid)

            if FtpUploader.file_exists(ftp, file.file_name) and FtpUploader.file_size(ftp, file.file_name) == file.size:
                self.logger.info(f'Skipping {file.file_name} ({file.size} bytes). File exists in ENA FTP.')
                file.error = 'File exists in ENA FTP.'
                file.success = False
            else:
                
                if compressed:
                    self.logger.info(f'Streaming {file.file_name} ({file.size} bytes) to FTP.')
                    self.stream_with_md5(ftp, file.cloud_url, file.file_name, lambda str: pbar.update(len(str))) 
                else:
                    self.logger.info(f'Compressing {file.file_name} ({file.size} bytes) / streaming {file.file_name}.gz to FTP.')
                    self.stream_with_compression_and_md5(ftp, file.cloud_url, file.file_name, lambda str: pbar.update(len(str))) 
                self.logger.info(f'Finish streaming {file.file_name}.')

    def stream_with_md5(self, ftp, fin, fout, cb):
        hash_md5 = hashlib.md5()
        ftp.voidcmd('TYPE I')
        with self.s3.open(fin, 'rb') as fp, ftp.transfercmd(f'STOR {fout}', None) as conn:
            while 1:
                buf = fp.read(BLOCKSIZE)
                if not buf:
                    break
                hash_md5.update(buf)
                conn.sendall(buf)
                cb(buf)
        ftp.voidresp()
        ftp.storbinary(f'STOR {fout}.md5', BytesIO(bytes(hash_md5.hexdigest(), 'utf-8')))

    def stream_with_compression_and_md5(self, ftp, fin, fout, cb):
        fout = f'{fout}.gz'
        hash_md5 = hashlib.md5()
        ftp.voidcmd('TYPE I')
        with self.s3.open(fin, 'rb') as fp, ftp.transfercmd(f'STOR {fout}', None) as conn:
            while 1:
                buf = fp.read(BLOCKSIZE)
                if not buf:
                    break
                cbuf = gzip.compress(buf)
                hash_md5.update(cbuf)
                conn.sendall(cbuf)
                cb(buf)
        ftp.voidresp()
        ftp.storbinary(f'STOR {fout}.md5', BytesIO(bytes(hash_md5.hexdigest(), 'utf-8')))

    def stream_with_compression_and_md5_using_tmpfile(self, ftp, fin, fout, cb):
        with self.s3.open(fin, 'rb') as f:
            compressed_fp = tempfile.SpooledTemporaryFile() #BytesIO() #tempfile.NamedTemporaryFile()
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

    def start(self, res: DataArchiverRequest):
        total_size = 0
        for file in res.files:

            if self.s3.exists(file.cloud_url):
                size = self.s3.size(file.cloud_url)
                total_size += size
                file.size = size
            else:
                file.error = 'File not found.'
                file.success = False
        
        total_files = len(res.files)
        num_files = sum(map(lambda f : f.success, res.files))
        
        if total_files != num_files:
            self.logger.info(f'{total_files - num_files} files not found.')
        
        self.logger.info('Streaming...')
        pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f'{num_files} files')
        pool = ThreadPool() # cpu_count() DEFAULT_THREAD_COUNT=25

        def cp(file):

            if not file.success:
                return

            try:
                self.s3_ftp_stream(file, pbar) 
            except Exception as ex:
                file.error = str(ex)
                file.success = False
                pass

        pool.map_async(cp, res.files)
        pool.close()
        pool.join()
        pbar.close()

    def close():
        pass