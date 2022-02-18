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
            logging.error(f'Ingest API exception in {f.__name__}')
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
        self.files = self.get_all(files_url, "files", [])
        return self.files

    @handle_exception
    def get_sequence_files(self, uuid):
        if not self.files:
            self.get_files(uuid)
        self.s3_files = []

        for file in self.files:
            if (file['content']['describedBy']).endswith('sequence_file'):
                uuid = file['uuid']['uuid']
                file_name = file['content']['file_core']['file_name']
                cloud_url = file['cloudUrl']
                self.s3_files.append({"uuid": uuid, "file_name": file_name, "cloud_url": cloud_url})
    
        return self.s3_files

    def get_staging_area(self):
        if self.submission and self.submission['stagingDetails']:
            return self.submission['stagingDetails']['stagingAreaLocation']['value']
        return None

    def get_all(self, url, entity_type, entities=[]):
        response = self.session.get(url)
        response.raise_for_status()
        if "_embedded" in response.json():
            entities += response.json()["_embedded"][entity_type]

            if "next" in response.json()["_links"]:
                url = response.json()["_links"]["next"]["href"]
                self.get_all(url, entity_type, entities)
        return entities

    def close(self):
        self.session.close()