import hashlib
import uuid
import gzip
import shutil
import re

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def compress(fname: str):
    with open(fname, 'rb') as f_in:
        with gzip.open(f'{fname}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

def valid_webin_user(user):
    if re.match(r"Webin-[0-9]+", user):
        return True
    return False