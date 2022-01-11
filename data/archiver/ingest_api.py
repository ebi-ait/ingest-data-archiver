import json
import requests
import functools
import logging
from data.archiver.config import INGEST_API

def handle_exception(f):
    @functools.wraps(f)
    def func(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            #print(f'Ingest API exception in {f.__name__}')
            return []
    return func

class Ingest:

    def __init__(self):
        self.session = requests.Session()

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    @handle_exception
    def get_submission(self, uuid):
        submission_url = f'{INGEST_API}submissionEnvelopes/search/findByUuidUuid?uuid={uuid}'
        self.logger.info(f'Submission url {submission_url}')
        response = self.session.get(submission_url)
        submission_json = json.loads(response.text)
        return submission_json

    @handle_exception
    def get_files(self, uuid):
        submission = self.get_submission(uuid)
        files_url = submission['_links']['files']['href']
        self.logger.info(f'Files url {files_url}')
        response = self.session.get(files_url)
        files_json = json.loads(response.text)
        return files_json

    @handle_exception
    def get_sequence_files(self, uuid):
        files = self.get_files(uuid)
        s3_files = []

        for file in files['_embedded']['files']:
            if (file['content']['describedBy']).endswith('sequence_file'):
                file_name = file['content']['file_core']['file_name']
                s3_files.append(file_name)
    
        return s3_files

    def close(self):
        self.session.close()
    