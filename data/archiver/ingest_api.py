import json
import requests
import functools
from config import INGEST_API

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

    @handle_exception
    def get_submission(self, uuid):
        submission_url = f'{INGEST_API}submissionEnvelopes/search/findByUuidUuid?uuid={uuid}'
        response = requests.get(submission_url)
        submission_json = json.loads(response.text)
        return submission_json

    @handle_exception
    def get_files(self, uuid):
        submission = self.get_submission(uuid)
        files_url = submission['_links']['files']['href']
        response = requests.get(files_url)
        files_json = json.loads(response.text)
        return files_json

    @handle_exception
    def get_sequence_files(self, uuid):
        files = self.get_files(uuid)
        s3_files = []

        for file in files['_embeddfed']['files']:
            if (file['content']['describedBy']).endswith('sequence_file'):
                cloud_url = file['cloudUrl']
                s3_files.append(cloud_url)
    
        return s3_files
