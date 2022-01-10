import os
from ftplib import FTP, FTP_TLS
from config import ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD, ENA_FTP_DIR


class FtpUploader:
    def __init__(self, uuid, secure=False):
        self.uuid = uuid
        self.secure = secure
        if secure:
            self.ftp = FTP_TLS(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD)
            self.ftp.prot_p()
        else:
            self.ftp = FTP(ENA_FTP_HOST, ENA_WEBIN_USER, ENA_WEBIN_PWD)
        FtpUploader.chdir(self.ftp, ENA_FTP_DIR)
        FtpUploader.chdir(self.ftp, uuid)

    def ftp_stor(self, file):
        with open(file, "rb") as f:
            self.ftp.storbinary(f'STOR {os.path.basename(file)}', f, 1024)

    def upload(self, fs):
        for f in fs:
            print(f'Uploading {f}')
            self.ftp_stor(f)

    def close_conn(self):
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
            #print(f'Exception in ftp.file_size: {ex}')
            return None