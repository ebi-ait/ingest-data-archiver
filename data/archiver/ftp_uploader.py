import os
import logging
from ftplib import FTP, FTP_TLS
from data.archiver.config import ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD, ENA_FTP_DIR
from data.archiver.dataclass import DataArchiverRequest, DataArchiverResult


class FtpUploader:
    def __init__(self, req:DataArchiverRequest, res:DataArchiverResult, secure=False):
        self.req = req
        self.res = res
        self.secure = secure
        if secure:
            self.ftp = FTP_TLS(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD)
            self.ftp.prot_p()
        else:
            self.ftp = FTP(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD)
        FtpUploader.chdir(self.ftp, ENA_FTP_DIR)
        FtpUploader.chdir(self.ftp, req.sub_uuid)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def ftp_stor(self, file):
        with open(file, "rb") as f:
            self.ftp.storbinary(f'STOR {os.path.basename(file)}', f, 1024)

    def upload(self):
        for f in self.res.files:
            if f.success:
                try:
                    self.logger.info(f'Uploading {f.file_name}')
                    self.ftp_stor(f'{self.req.sub_uuid}/{f.file_name}')
                    self.logger.info(f'Uploading {f.file_name}.md5')
                    self.ftp_stor(f'{self.req.sub_uuid}/{f.file_name}.md5')
                except:
                    f.success = False
                    f.error = 'FTP upload error'

    def close(self):
        self.ftp.close()


    @staticmethod
    def chdir(ftp, dir): 
        if not FtpUploader.dir_exists(ftp, dir):
            ftp.mkd(dir)
        ftp.cwd(dir)

    @staticmethod
    def dir_exists(ftp, dir):
        fs = []
        ftp.retrlines('LIST', fs.append)
        for f in fs:
            if f.split()[-1] == dir and f.upper().startswith('D'):
                return True
        return False

    @staticmethod
    def file_exists(ftp, file):
        fs = []
        ftp.retrlines('LIST', fs.append)
        for f in fs:
            if not f.upper().startswith('D') and f.split()[-1] == file:
                return True
        return False

    @staticmethod
    def file_size(ftp, file):
        try:
            return ftp.size(file)
        except Exception as ex:
            #self.logger.error(f'Exception in ftp.file_size: {ex}')
            return None