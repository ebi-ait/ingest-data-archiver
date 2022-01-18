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
        self.submission = None
        self.files = None

    @handle_exception
    def get_submission(self, uuid):
        submission_url = f'{INGEST_API}submissionEnvelopes/search/findByUuidUuid?uuid={uuid}'
        self.logger.info(f'Submission url {submission_url}')
        response = self.session.get(submission_url)
        self.submission = json.loads(response.text)
        return self.submission

    @handle_exception
    def get_files(self, uuid):
        if not self.submission:
            self.get_submission(uuid)
        files_url = self.submission['_links']['files']['href']
        self.logger.info(f'Files url {files_url}')
        response = self.session.get(files_url)
        self.files = json.loads(response.text)
        return self.files

    @handle_exception
    def get_sequence_files(self, uuid):
        if not self.files:
            self.get_files(uuid)
        self.s3_files = []

        for file in self.files['_embedded']['files']:
            if (file['content']['describedBy']).endswith('sequence_file'):
                file_name = file['content']['file_core']['file_name']
                cloud_url = file['cloudUrl']
                self.s3_files.append({"file_name": file_name, "cloud_url": cloud_url})
    
        return self.s3_files

    def get_staging_area(self):
        if self.submission and self.submission['stagingDetails']:
            return self.submission['stagingDetails']['stagingAreaLocation']['value']
        return None

    def close(self):
        self.session.close()


#if __name__ == "__main__": # python -m data.archiver.ingest_api
def test_ingest_api():
    ingest = Ingest()
    ingest.get_submission('14df1f92-155c-4da2-97fc-85601dee64da')
    if ingest.submission:
        print(ingest.submission)
        print(ingest.get_staging_area())