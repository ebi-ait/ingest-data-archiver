from email.policy import HTTP
from http import HTTPStatus
import json
from shutil import ExecError
from urllib.error import HTTPError
import requests
import functools
import logging
from datetime import datetime
from data.archiver.config import INGEST_API
from data.archiver.dataclass import FileResult

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

    #@handle_exception
    def patch_files(self, files: [FileResult]):
        for file in files:
            if not file.success:
                continue
            response = self.session.get(f'{INGEST_API}files/search/findByUuid?uuid={file.uuid}')
            if response.status_code == HTTPStatus.OK:
                file_url = response.json()["_links"]["self"]["href"]
                archive_result = {
                    "fileArchiveResult": {
                        "lastArchived":  datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "compressed": file.compressed,
                        "md5": file.md5,
                        "enaUploadPath": file.ena_upload_path,
                        "error": file.error
                    }
                }
                patch_response = self.session.patch(file_url, json.dumps(archive_result), headers={ 'Content-type':'application/json' })
                if patch_response.status_code == HTTPStatus.ACCEPTED:
                    self.logger.info(f"Patched {file_url} {archive_result}")
                else:
                    self.logger.info(f"Could not patch {file_url}: {patch_response.status_code} ")
            else:
                self.logger.info(f"Could not get {file_url}: {response.status_code} ")


    def close(self):
        self.session.close()