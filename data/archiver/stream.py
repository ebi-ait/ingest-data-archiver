from io import BytesIO, StringIO
import os
import s3fs
import hashlib
import gzip
import zlib
import shutil
from ftplib import FTP, FTP_TLS
from io import BytesIO
from memory_profiler import profile
import tempfile
from ftp_uploader import FtpUploader
import time


AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_ACCESS_KEY_SECRET')

FTP_HOST = os.getenv('ENA_FTP_HOST')
FTP_USER = os.getenv('ENA_WEBIN_USERNAME')
FTP_PWD = os.getenv('ENA_WEBIN_PASSWORD')

s3 = s3fs.S3FileSystem(anon=False, key=AWS_KEY, secret=AWS_SECRET)
ftp = FTP(FTP_HOST, FTP_USER, FTP_PWD, timeout=60*60*2) # 2h


@profile
def calc_md5(s3file):
    """
    Calculate md5 by streaming s3 file without saving it locally.
    """
    hash_md5 = hashlib.md5()
    with s3.open(s3file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@profile
def transfer_file(fin, fout):
    """
    Transfer s3 file to ftp via stream.
    """
    with s3.open(fin, 'rb') as f:
        ftp.storbinary(f'STOR {fout}', f) 

@profile
def transfer_file_md5(fin, fout):
    hash_md5 = hashlib.md5()
    with s3.open(fin, 'rb') as f:
        ftp.storbinary(f'STOR {fout}', f) 
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    md5 = hash_md5.hexdigest()
    ftp.storbinary(f'STOR {fout}.md5', BytesIO(bytes(md5, 'utf-8')))


@profile
def transfer_file_compress(fin, fout):
    with s3.open(fin, 'rb') as f:
        compressed_fp = tempfile.SpooledTemporaryFile() #BytesIO() #tempfile.NamedTemporaryFile()
        hash_md5 = hashlib.md5()
        with gzip.GzipFile(fileobj=compressed_fp, mode='wb') as gz:
            shutil.copyfileobj(f, gz)
        compressed_fp.seek(0)
        ftp.storbinary(f'STOR {fout}.gz', compressed_fp) 
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
        md5 = hash_md5.hexdigest()
        ftp.storbinary(f'STOR {fout}.gz.md5', BytesIO(bytes(md5, 'utf-8')))
        compressed_fp.close()


#https://stackoverflow.com/questions/30113119/compress-a-file-in-memory-compute-checksum-and-write-it-as-gzip-in-python
#https://gist.github.com/tobywf/079b36898d39eeb1824977c6c2f6d51e

@profile
def run(file):
    hash_md5 = hashlib.md5()
    hash_md5_1 = hashlib.md5()
    with s3.open(file, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

        print(hash_md5.hexdigest())
        """
        with gzip.open(f'test.gz', 'rwb') as f_out:
            shutil.copyfileobj(f, f_out)
            print('Compressed file md5')
            for chunk in iter(lambda: f_out.read(4096), b""):
                hash_md5.update(chunk)

            print(hash_md5.hexdigest())

        print('Original file md5')

        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5_1.update(chunk)

        print(hash_md5_1.hexdigest())
        """


def is_compressed(s3file):
    return s3file.endswith('.gz') and s3.read_block(s3file, 0, 2) == b'\x1f\x8b'

MAX_IN_MEM_FILE_COMPRESSION = 1024*1024*500 #500M

def transfer(s3file):
    bucket, uuid, file_name = s3file.split('/')
    env = bucket.split('-')[-1]

    s3_file_size = s3.size(s3file)
    s3_file_compressed = is_compressed(s3file)

    print(f'Transfer {s3file} -> FTP')
    print(f'File size (bytes): {s3_file_size}' )
    print(f'Compressed: {s3_file_compressed}')

    #ftp_dir = f'{env}/{uuid}'
    ftp_file_exists = False
    if FtpUploader.dir_exists(ftp, env):
       ftp.cwd(env)
       if FtpUploader.dir_exists(ftp, uuid):
           ftp.cwd(uuid)
           if FtpUploader.file_exists(file_name):
               ftp_file_exists = True
               ftp_file_size = FtpUploader.file_size(file_name)

    if ftp_file_exists:
        print(f'File exists in FTP.')
        print(f'File size (bytes): {ftp_file_size}')

    else:
        if s3_file_compressed:
            # stream file to ftp without compression
            transfer_file(s3file)
        else:
            # s3 file not compressed
            if s3_file_size < MAX_IN_MEM_FILE_COMPRESSION:

                pass #transfer_file_gzip(s3file,out):


if __name__ == '__main__':
    #run('org-hca-data-archive-upload-dev/0169d6f3-f65b-4d8a-b04c-857eec3b805e/SRR3562314_1.fastq.gz')
    #run('org-hca-data-archive-upload-staging/37f4c191-fbf3-4d58-915a-59d79d07185d/4861STDY7771115.mtx')
    #transfer_file_gzip('org-hca-data-archive-upload-staging/37f4c191-fbf3-4d58-915a-59d79d07185d/MRC_Endo8715416.mtx', 'out1')
    #transfer_file_gzip2('org-hca-data-archive-upload-staging/37f4c191-fbf3-4d58-915a-59d79d07185d/MRC_Endo8715416.mtx', 'out2')

    #print(s3.size('org-hca-data-archive-upload-staging/37f4c191-fbf3-4d58-915a-59d79d07185d/4861STDY7771115.mtx'))
    #print(ftp.size('4861STDY7771115.mtx'))
    #transfer('org-hca-data-archive-upload-dev/0169d6f3-f65b-4d8a-b04c-857eec3b805e/SRR3562314_1.fastq.gz')
    
    fin = 'org-hca-data-archive-upload-staging/37f4c191-fbf3-4d58-915a-59d79d07185d/'
    """
    start = time.time()
    transfer_file(fin, 'out1')
    end = time.time()
    print(end - start)    

    start = time.time()
    transfer_file_md5(fin, 'out2')
    end = time.time()
    print(end - start)
    """
    #start = time.time()
    #transfer_file_compress(fin, 'out3')
    #end = time.time()
    #print(end - start)
    print(calc_md5(fin))


## TODO:
# run stream impl on a large submission and check runtime
# on the EBI cluster / k8s cluster
# check a queue / api to trigger run / return result

